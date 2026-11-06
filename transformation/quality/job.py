"""Điều phối và thực thi cổng kiểm định chất lượng dữ liệu (Data Quality Job).

Mô-đun này là "Nhạc trưởng" điều phối toàn bộ các bài kiểm định chất lượng dữ liệu cho kiến trúc Lakehouse:
1. Đảm bảo cấu trúc hợp đồng dữ liệu (Table Contracts): Đối chiếu schema thực tế với định nghĩa chuẩn.
2. Kiểm tra đa tầng dữ liệu:
   - Tầng Bronze: Kiểm tra hợp đồng schema, cột bắt buộc, tính không rỗng.
   - Tầng Silver: Kiểm tra khóa chính, tính duy nhất, thứ tự thời gian các mốc đơn hàng, giá trị không âm, khóa ngoại mồ côi (phân loại nhóm cách ly trong quarantine.orders).
   - Tầng Gold: Kiểm tra tính toàn vẹn của mô hình Star Schema, bảng cầu nối many-to-many, khóa 0 (Unknown Member), và đối soát bảo toàn doanh thu (Conservation of Revenue).
3. Lưu trữ kết quả bất biến vào bảng Iceberg: 'lakehouse.quality.quality_results' (phân vùng theo days(checked_at)).
4. Ghi nhận nhật ký kiểm toán (Audit Logging) vào 'lakehouse.control.pipeline_runs'.
5. Hỗ trợ cơ chế Zero-Config Defaults: Có thể chạy ngay mà không bắt buộc truyền cờ dòng lệnh.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import os
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformation.common.audit import (
    attempt_run_id,
    ensure_audit_tables,
    finish_pipeline_run,
    pipeline_run_outcome,
    start_pipeline_run,
    truncate_error_summary,
)
from transformation.common.config import Settings
from transformation.common.iceberg import _batch_ownership_lock, create_or_replace
from transformation.common.logging import configure_logging
from transformation.common.registry import BRONZE_METADATA_COLUMNS, TABLE_SPECS
from transformation.common.spark_session import build_spark_session
from transformation.gold.job import GOLD_TARGET_SPECS, SILVER_KEYS, SILVER_REQUIREMENTS
from transformation.quality.checks import (
    QualityResult,
    aggregate_status,
    check_classified_foreign_key,
    check_date_ordering,
    check_foreign_key,
    check_non_negative,
    check_nullability,
    check_reconciliation,
    check_row_count,
    check_uniqueness,
    check_unknown_members,
    unavailable_result,
)


SUPPORTED_LAYERS = ("bronze", "silver", "gold")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_AUDIT_FINALIZATION_ATTEMPTS = 3
_QUALITY_INIT_LOCK_ID = "quality-table-initialization"

# Cấu trúc schema của bảng lưu trữ kết quả kiểm định chất lượng 'lakehouse.quality.quality_results'
QUALITY_RESULT_SCHEMA = StructType(
    [
        StructField("layer", StringType(), False),           # Tầng dữ liệu: bronze, silver, gold
        StructField("table_name", StringType(), False),      # Tên bảng được kiểm tra
        StructField("rule_name", StringType(), False),       # Tên quy tắc chất lượng
        StructField("severity", StringType(), False),        # Mức độ nghiêm trọng: INFO, WARN, ERROR
        StructField("observed_value", StringType(), False),  # Giá trị quan sát thực tế
        StructField("threshold", StringType(), False),       # Ngưỡng kỳ vọng
        StructField("status", StringType(), False),          # Trạng thái kết quả: PASS, WARN, ERROR
        StructField("run_id", StringType(), False),          # Mã phiên chạy pipeline
        StructField("checked_at", TimestampType(), False),   # Thời điểm kiểm tra (dùng làm khóa phân vùng)
    ]
)

LayerChecker = Callable[[SparkSession, Settings, str], Sequence[QualityResult]]
QualityGrain = tuple[str, str, str, str]
QualityPayload = tuple[str, str, str, str, str, str, str, str]


@dataclass(frozen=True)
class TableContract:
    """Định nghĩa hợp đồng dữ liệu cho một bảng: Schema các trường, cột bắt buộc, và cột khóa."""

    fields: tuple[StructField, ...]
    required: tuple[str, ...]
    key: tuple[str, ...]

    @property
    def columns(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (field.name, field.dataType.simpleString()) for field in self.fields
        )


def _contract_type(spark_type: str):
    """Chuyển đổi chuỗi tên kiểu dữ liệu Spark sang đối tượng DataType tương ứng."""
    primitives = {
        "boolean": BooleanType(),
        "date": DateType(),
        "int": IntegerType(),
        "bigint": LongType(),
        "string": StringType(),
        "timestamp": TimestampType(),
    }
    if spark_type in primitives:
        return primitives[spark_type]
    decimal = re.fullmatch(r"decimal\(([1-9][0-9]*),([0-9]+)\)", spark_type)
    if decimal:
        return DecimalType(int(decimal.group(1)), int(decimal.group(2)))
    raise ValueError(f"Kiểu Spark trong contract không được hỗ trợ: {spark_type}")


def _persisted_fields(
    columns: Sequence[tuple[str, str]],
) -> tuple[StructField, ...]:
    """Tạo danh sách các StructField từ danh sách bộ (tên_cột, kiểu_spark)."""
    return tuple(
        StructField(name, _contract_type(spark_type), True)
        for name, spark_type in columns
    )


# Danh sách các cột nghiệp vụ bắt buộc không được phép null ở tầng Silver
SILVER_REQUIRED = {
    "customers": (
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ),
    "orders": (
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_estimated_delivery_date",
        "is_late_delivery",
    ),
    "order_items": tuple(column for column, _ in SILVER_REQUIREMENTS["order_items"]),
    "order_payments": tuple(column for column, _ in SILVER_REQUIREMENTS["order_payments"]),
    "order_reviews": (
        "review_id",
        "order_id",
        "review_score",
        "review_creation_date",
        "review_answer_timestamp",
        "has_title",
        "has_message",
    ),
    "products": ("product_id", "product_category_name_english"),
    "sellers": tuple(column for column, _ in SILVER_REQUIREMENTS["sellers"]),
    "geography": tuple(column for column, _ in SILVER_REQUIREMENTS["geography"]),
}

# Các cột metadata lineage bắt buộc từ tầng Bronze truyền sang Silver
SILVER_LINEAGE_CONTRACT = tuple(
    (column.name, column.spark_type) for column in BRONZE_METADATA_COLUMNS
)
SILVER_LINEAGE_REQUIRED = tuple(column for column, _ in SILVER_LINEAGE_CONTRACT)

# Tập hợp các Hợp đồng dữ liệu chuẩn hóa cho 8 bảng Silver
SILVER_CONTRACTS = {
    name: TableContract(
        _persisted_fields(tuple(columns) + SILVER_LINEAGE_CONTRACT),
        SILVER_REQUIRED[name] + SILVER_LINEAGE_REQUIRED,
        tuple(SILVER_KEYS[name]),
    )
    for name, columns in SILVER_REQUIREMENTS.items()
}

# Các khóa ngoại liên kết giữa bảng Fact/Bridge với bảng dim_date
GOLD_DATE_FOREIGN_KEYS = (
    ("fact_orders", "purchase_date_key"),
    ("fact_orders", "approval_date_key"),
    ("fact_orders", "carrier_date_key"),
    ("fact_orders", "delivered_date_key"),
    ("fact_orders", "estimated_delivery_date_key"),
    ("fact_order_items", "order_date_key"),
    ("fact_order_items", "shipping_limit_date_key"),
    ("fact_payments", "order_date_key"),
    ("fact_reviews", "review_date_key"),
    ("fact_reviews", "answer_date_key"),
)


def _qualified(catalog: str, namespace: str, table: str) -> str:
    """Tạo tên bảng đầy đủ chuẩn định dạng catalog.namespace.table."""
    if any(not _IDENTIFIER.fullmatch(part) for part in (catalog, namespace, table)):
        raise ValueError(f"Định danh bảng không hợp lệ: {catalog}.{namespace}.{table}")
    return f"{catalog}.{namespace}.{table}"


def _quoted(name: str) -> str:
    """Bao bọc các phần của định danh bảng trong dấu backtick để an toàn cú pháp SQL."""
    parts = name.split(".")
    if any(not _IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError(f"Định danh không hợp lệ: {name}")
    return ".".join(f"`{part}`" for part in parts)


def _safe_detail(exc: BaseException) -> str:
    """Lấy tên loại ngoại lệ an toàn để hiển thị ngắn gọn trong kết quả audit."""
    return type(exc).__name__


def _result_grain(result: QualityResult) -> QualityGrain:
    """Độ hạt định danh duy nhất của một kết quả kiểm định: (run_id, layer, table_name, rule_name)."""
    return (
        result.run_id,
        result.layer,
        result.table_name,
        result.rule_name,
    )


def _canonicalize_quality_results(
    results: Sequence[QualityResult],
) -> tuple[QualityResult, ...]:
    """Khử trùng lặp và chuẩn hóa kết quả kiểm định, đảm bảo tính tất định trước khi ghi vào Iceberg."""
    grouped: dict[QualityGrain, list[QualityResult]] = {}
    for result in results:
        grouped.setdefault(_result_grain(result), []).append(result)
    canonical: list[QualityResult] = []
    for grain, candidates in grouped.items():
        if len(candidates) == 1:
            canonical.append(candidates[0])
            continue
        considered = candidates
        if grain[-1] == "table_available":
            native = [
                item
                for item in candidates
                if not item.observed_value.startswith("gx:")
            ]
            if native:
                considered = native
        payloads = {
            (
                item.severity,
                item.observed_value,
                item.threshold,
                item.status,
            )
            for item in considered
        }
        if len(payloads) != 1:
            raise ValueError(f"Độ hạt kết quả chất lượng xung đột: {grain}")
        canonical.append(min(considered, key=lambda item: item.checked_at))
    return tuple(canonical)


def _contract_result(
    *,
    layer: str,
    table_name: str,
    rule_name: str,
    observed: str,
    threshold: str,
    passed: bool,
    run_id: str,
    severity: str = "ERROR",
) -> QualityResult:
    """Tạo đối tượng QualityResult cho các bài kiểm tra hợp đồng cấu trúc."""
    return QualityResult(
        layer=layer,
        table_name=table_name,
        rule_name=rule_name,
        severity=severity,
        observed_value=observed,
        threshold=threshold,
        status="PASS" if passed else severity,
        run_id=run_id,
        checked_at=datetime.now(timezone.utc),
    )


def _schema_result(
    frame: DataFrame,
    *,
    layer: str,
    table_name: str,
    contract: TableContract,
    run_id: str,
) -> QualityResult:
    """Đối chiếu schema thực tế của bảng với hợp đồng TableContract:
    - Kiểm tra cột bị thiếu (missing)
    - Kiểm tra cột thừa không mong đợi (unexpected)
    - Kiểm tra lệch kiểu dữ liệu (type mismatches)
    - Kiểm tra lệch thuộc tính nullable
    """
    expected = {field.name: field for field in contract.fields}
    actual = {field.name: field for field in frame.schema.fields}
    missing = sorted(set(expected) - set(actual))
    unexpected = sorted(set(actual) - set(expected))
    mismatches = sorted(
        f"{name}:{actual[name].dataType.simpleString()}!="
        f"{expected[name].dataType.simpleString()}"
        for name in set(expected) & set(actual)
        if actual[name].dataType != expected[name].dataType
    )
    nullable_mismatches = sorted(
        f"{name}:{str(actual[name].nullable).lower()}!="
        f"{str(expected[name].nullable).lower()}"
        for name in set(expected) & set(actual)
        if actual[name].nullable != expected[name].nullable
    )
    details = (
        f"missing={missing}; unexpected={unexpected}; type_mismatches={mismatches}; "
        f"nullable_mismatches={nullable_mismatches}"
    )
    return _contract_result(
        layer=layer,
        table_name=table_name,
        rule_name="schema_matches_contract",
        observed=details,
        threshold="Tên cột, kiểu Spark và thuộc tính nullable phải khớp chính xác",
        passed=(
            not missing
            and not unexpected
            and not mismatches
            and not nullable_mismatches
        ),
        run_id=run_id,
    )


def _required_result(
    frame: DataFrame,
    *,
    layer: str,
    table_name: str,
    required: Sequence[str],
    run_id: str,
) -> QualityResult:
    """Kiểm tra không được null trên danh sách các cột bắt buộc."""
    return check_nullability(
        frame,
        columns=required,
        layer=layer,
        table_name=table_name,
        severity="ERROR",
        run_id=run_id,
    )


def declared_layer_columns() -> dict[str, dict[str, set[str]]]:
    """Cung cấp danh mục cột chính thức của các bảng trên cả 3 tầng Bronze, Silver, Gold."""
    return {
        "bronze": {
            name: {column.name for column in spec.columns}
            for name, spec in TABLE_SPECS.items()
        },
        "silver": {
            name: {
                *(column for column, _spark_type in requirements),
                *SILVER_LINEAGE_REQUIRED,
            }
            for name, requirements in SILVER_REQUIREMENTS.items()
        },
        "gold": {
            name: set(spec.schema.fieldNames())
            for name, spec in GOLD_TARGET_SPECS.items()
        },
    }


def _load_layer(
    spark: SparkSession,
    *,
    catalog: str,
    namespace: str,
    tables: Sequence[str],
    layer: str,
    run_id: str,
) -> tuple[dict[str, DataFrame], list[QualityResult]]:
    """Tải tất cả các bảng trong một schema thành các DataFrame, ghi nhận lỗi nếu bảng không đọc được."""
    frames: dict[str, DataFrame] = {}
    results: list[QualityResult] = []
    for table_name in tables:
        try:
            frames[table_name] = spark.table(
                _qualified(catalog, namespace, table_name)
            )
        except Exception as exc:
            results.append(
                unavailable_result(
                    layer=layer,
                    table_name=table_name,
                    detail=_safe_detail(exc),
                    run_id=run_id,
                )
            )
    return frames, results


def _bronze_checker(
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    *,
    namespace: str = "bronze",
) -> tuple[QualityResult, ...]:
    """Kiểm tra chất lượng Tầng Bronze: Đối chiếu schema và tính không null của các cột bắt buộc trên 9 bảng."""
    frames, results = _load_layer(
        spark,
        catalog=settings.iceberg_catalog_name,
        namespace=namespace,
        tables=tuple(TABLE_SPECS),
        layer="bronze",
        run_id=run_id,
    )
    for name, frame in frames.items():
        spec = TABLE_SPECS[name]
        required = tuple(column.name for column in spec.columns if not column.nullable)
        contract = TableContract(
            _persisted_fields(
                tuple((column.name, column.spark_type) for column in spec.columns)
            ),
            required,
            tuple(spec.key),
        )
        schema = _schema_result(
            frame,
            layer="bronze",
            table_name=name,
            contract=contract,
            run_id=run_id,
        )
        results.append(schema)
        if schema.status == "PASS":
            results.append(
                _required_result(
                    frame,
                    layer="bronze",
                    table_name=name,
                    required=required,
                    run_id=run_id,
                )
            )
    return tuple(results)


def _silver_checker(
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    *,
    namespace: str = "silver",
    quarantine_namespace: str = "quarantine",
) -> tuple[QualityResult, ...]:
    """Kiểm tra chất lượng Tầng Silver:
    - Schema hợp đồng & cột bắt buộc
    - Thứ tự thời gian vòng đời đơn hàng (order_purchase_timestamp <= approved <= delivered)
    - Giá bán & phí ship không âm
    - Khóa ngoại nghiêm ngặt giữa order_items -> products, sellers, và orders -> customers
    - Phân loại bản ghi mồ côi (Classified Foreign Key) theo quarantine.orders
    """
    frames, results = _load_layer(
        spark,
        catalog=settings.iceberg_catalog_name,
        namespace=namespace,
        tables=tuple(SILVER_REQUIREMENTS),
        layer="silver",
        run_id=run_id,
    )
    valid_frames: dict[str, DataFrame] = {}
    for name, frame in frames.items():
        contract = SILVER_CONTRACTS[name]
        schema = _schema_result(
            frame,
            layer="silver",
            table_name=name,
            contract=contract,
            run_id=run_id,
        )
        results.append(schema)
        if schema.status == "PASS":
            valid_frames[name] = frame
            results.append(
                _required_result(
                    frame,
                    layer="silver",
                    table_name=name,
                    required=contract.required,
                    run_id=run_id,
                )
            )

    # Kiểm tra thứ tự các mốc thời gian của đơn hàng
    orders = valid_frames.get("orders")
    if orders is not None:
        for later in (
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ):
            results.append(
                check_date_ordering(
                    orders,
                    earlier_column="order_purchase_timestamp",
                    later_column=later,
                    layer="silver",
                    table_name="orders",
                    severity="ERROR",
                    run_id=run_id,
                )
            )

    # Kiểm tra thứ tự mốc đánh giá
    reviews = valid_frames.get("order_reviews")
    if reviews is not None:
        results.append(
            check_date_ordering(
                reviews,
                earlier_column="review_creation_date",
                later_column="review_answer_timestamp",
                layer="silver",
                table_name="order_reviews",
                severity="ERROR",
                run_id=run_id,
            )
        )

    # Kiểm tra giá trị không âm cho tiền tệ
    for table_name, columns in {
        "order_items": ("price", "freight_value"),
        "order_payments": ("payment_installments", "payment_value"),
    }.items():
        metric_frame = valid_frames.get(table_name)
        if metric_frame is not None:
            for column in columns:
                results.append(
                    check_non_negative(
                        metric_frame,
                        column=column,
                        layer="silver",
                        table_name=table_name,
                        severity="ERROR",
                        run_id=run_id,
                    )
                )

    # Khóa ngoại quan hệ nghiêm ngặt
    strict_foreign_keys = (
        ("orders", "customers", ("customer_id",), ("customer_id",)),
        ("order_items", "products", ("product_id",), ("product_id",)),
        ("order_items", "sellers", ("seller_id",), ("seller_id",)),
    )
    for child_name, parent_name, child_key, parent_key in strict_foreign_keys:
        if child_name in valid_frames and parent_name in valid_frames:
            results.append(
                check_foreign_key(
                    valid_frames[child_name],
                    valid_frames[parent_name],
                    child_columns=child_key,
                    parent_columns=parent_key,
                    layer="silver",
                    table_name=child_name,
                    severity="ERROR",
                    run_id=run_id,
                )
            )

    # Phân loại bản ghi mồ côi con theo quarantine.orders
    order_children = tuple(
        name
        for name in ("order_items", "order_payments", "order_reviews")
        if name in valid_frames and "orders" in valid_frames
    )
    if order_children:
        quarantine_name = _qualified(
            settings.iceberg_catalog_name,
            quarantine_namespace,
            "orders",
        )
        try:
            catalog = getattr(spark, "catalog", None)
            if catalog is not None and not catalog.tableExists(quarantine_name):
                quarantine_orders = valid_frames["orders"].select("order_id").limit(0)
            else:
                quarantine_orders = spark.table(quarantine_name)
            if "order_id" not in quarantine_orders.columns:
                raise ValueError("quarantine.orders không có order_id")
        except Exception as exc:
            results.append(
                _contract_result(
                    layer="silver",
                    table_name="orders",
                    rule_name="quarantine_orders_available",
                    observed=_safe_detail(exc),
                    threshold="quarantine.orders phải đọc được và có column order_id",
                    passed=False,
                    run_id=run_id,
                )
            )
        else:
            for child_name in order_children:
                results.extend(
                    check_classified_foreign_key(
                        valid_frames[child_name],
                        valid_frames["orders"],
                        quarantine_orders,
                        child_column="order_id",
                        parent_column="order_id",
                        quarantine_column="order_id",
                        layer="silver",
                        table_name=child_name,
                        run_id=run_id,
                    )
                )
    return tuple(results)


def _gold_checker(
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    *,
    namespace: str = "gold",
) -> tuple[QualityResult, ...]:
    """Kiểm tra chất lượng Tầng Gold:
    - Schema hợp đồng & cột bắt buộc
    - Khóa không xác định (Unknown Member - Key 0) trên các Dimension (phải đúng = 1)
    - Khóa ngoại giữa Fact và Dimension (fact_orders -> dim_customer, fact_order_items -> dim_product, v.v.)
    - Khóa ngoại ngày tháng (dim_date)
    - Đối soát bảo toàn doanh thu (Conservation of Revenue):
      fact_order_items.price == fact_orders.item_revenue == marts.item_revenue (chênh lệch <= 0.01 BRL)
    """
    frames, results = _load_layer(
        spark,
        catalog=settings.iceberg_catalog_name,
        namespace=namespace,
        tables=tuple(GOLD_TARGET_SPECS),
        layer="gold",
        run_id=run_id,
    )
    valid_frames: dict[str, DataFrame] = {}
    for name, frame in frames.items():
        spec = GOLD_TARGET_SPECS[name]
        contract = TableContract(
            tuple(
                StructField(field.name, field.dataType, True)
                for field in spec.schema.fields
            ),
            tuple(field.name for field in spec.schema.fields if not field.nullable),
            spec.key,
        )
        schema = _schema_result(
            frame,
            layer="gold",
            table_name=name,
            contract=contract,
            run_id=run_id,
        )
        results.append(schema)
        if schema.status == "PASS":
            valid_frames[name] = frame
            results.append(
                _required_result(
                    frame,
                    layer="gold",
                    table_name=name,
                    required=contract.required,
                    run_id=run_id,
                )
            )
            for column in tuple(item for item in frame.columns if item.endswith("_key")):
                is_dimension_member = name.startswith("dim_") and column in spec.key
                results.append(
                    check_unknown_members(
                        frame,
                        column=column,
                        layer="gold",
                        table_name=name,
                        run_id=run_id,
                        expected_count=1 if is_dimension_member else 0,
                        severity="ERROR" if is_dimension_member else "WARN",
                    )
                )

    foreign_keys = (
        ("fact_orders", "dim_customer", ("customer_key",), ("customer_key",)),
        ("fact_order_items", "dim_customer", ("customer_key",), ("customer_key",)),
        ("fact_order_items", "dim_product", ("product_key",), ("product_key",)),
        ("fact_order_items", "dim_seller", ("seller_key",), ("seller_key",)),
        ("fact_payments", "dim_customer", ("customer_key",), ("customer_key",)),
        ("fact_reviews", "dim_customer", ("customer_key",), ("customer_key",)),
        ("bridge_review_products", "dim_product", ("product_key",), ("product_key",)),
        ("bridge_review_sellers", "dim_seller", ("seller_key",), ("seller_key",)),
        ("mart_seller_performance", "dim_seller", ("seller_key",), ("seller_key",)),
        *tuple(
            (table_name, "dim_date", (column,), ("date_key",))
            for table_name, column in GOLD_DATE_FOREIGN_KEYS
        ),
    )
    for child_name, parent_name, child_key, parent_key in foreign_keys:
        if child_name in valid_frames and parent_name in valid_frames:
            results.append(
                check_foreign_key(
                    valid_frames[child_name],
                    valid_frames[parent_name],
                    child_columns=child_key,
                    parent_columns=parent_key,
                    layer="gold",
                    table_name=child_name,
                    severity="ERROR",
                    run_id=run_id,
                )
            )

    items = valid_frames.get("fact_order_items")
    orders = valid_frames.get("fact_orders")
    payments = valid_frames.get("fact_payments")
    if items is not None and orders is not None:
        for item_column, order_column, metric in (
            ("price", "item_revenue", "revenue"),
            ("freight_value", "freight_total", "freight"),
        ):
            results.append(
                check_reconciliation(
                    items,
                    orders,
                    left_column=item_column,
                    right_column=order_column,
                    rule_name=f"fact_{metric}_reconciliation",
                    layer="gold",
                    table_name="fact_orders",
                    severity="ERROR",
                    run_id=run_id,
                )
            )
    if payments is not None and orders is not None:
        results.append(
            check_reconciliation(
                payments,
                orders,
                left_column="payment_value",
                right_column="payment_total",
                rule_name="fact_payment_reconciliation",
                layer="gold",
                table_name="fact_orders",
                severity="ERROR",
                run_id=run_id,
            )
        )
    if items is not None:
        for mart_name in (
            "mart_daily_revenue",
            "mart_seller_performance",
            "mart_product_performance",
        ):
            mart = valid_frames.get(mart_name)
            if mart is None:
                continue
            for item_column, mart_column, metric in (
                ("price", "item_revenue", "revenue"),
                ("freight_value", "freight_total", "freight"),
            ):
                results.append(
                    check_reconciliation(
                        items,
                        mart,
                        left_column=item_column,
                        right_column=mart_column,
                        rule_name=f"{mart_name}_{metric}_reconciliation",
                        layer="gold",
                        table_name=mart_name,
                        severity="ERROR",
                        run_id=run_id,
                    )
                )
    return tuple(results)


def _result_frame(spark: SparkSession, results: Sequence[QualityResult]) -> DataFrame:
    """Chuyển đổi danh sách đối tượng QualityResult thành DataFrame theo schema QUALITY_RESULT_SCHEMA."""
    rows = [
        (
            item.layer,
            item.table_name,
            item.rule_name,
            item.severity,
            item.observed_value,
            item.threshold,
            item.status,
            item.run_id,
            item.checked_at,
        )
        for item in results
    ]
    return spark.createDataFrame(rows, QUALITY_RESULT_SCHEMA)


def _quality_run_grains(
    spark: SparkSession,
    target: str,
    run_id: str,
) -> tuple[QualityGrain, ...]:
    if not spark.catalog.tableExists(target):
        return ()
    table = spark.table(target)
    rows = (
        table.where(table.run_id == run_id)
        .select("run_id", "layer", "table_name", "rule_name")
        .collect()
    )
    return tuple(
        (row.run_id, row.layer, row.table_name, row.rule_name) for row in rows
    )


def _quality_payload(result: QualityResult) -> QualityPayload:
    return (
        result.run_id,
        result.layer,
        result.table_name,
        result.rule_name,
        result.severity,
        result.observed_value,
        result.threshold,
        result.status,
    )


def _quality_run_payloads(
    spark: SparkSession,
    target: str,
    run_id: str,
) -> tuple[QualityPayload, ...]:
    table = spark.table(target)
    rows = (
        table.where(table.run_id == run_id)
        .select(
            "run_id",
            "layer",
            "table_name",
            "rule_name",
            "severity",
            "observed_value",
            "threshold",
            "status",
        )
        .collect()
    )
    return tuple(
        (
            row.run_id,
            row.layer,
            row.table_name,
            row.rule_name,
            row.severity,
            row.observed_value,
            row.threshold,
            row.status,
        )
        for row in rows
    )


def _reconcile_quality_output_count(
    spark: SparkSession,
    target: str,
    run_id: str,
    expected_grains: set[QualityGrain],
) -> int:
    actual = _quality_run_grains(spark, target, run_id)
    actual_set = set(actual)
    if len(actual) != len(actual_set):
        raise RuntimeError("Run chất lượng đã lưu chứa grain trùng lặp")
    unexpected = actual_set - expected_grains
    if unexpected:
        raise RuntimeError("Run chất lượng đã lưu chứa grain ngoài dự kiến")
    return len(actual)


def _ensure_quality_table_contract(spark: SparkSession, target: str) -> None:
    """Đảm bảo bảng Iceberg quality_results có phân vùng days(checked_at) và các thuộc tính quản trị."""
    table = _quoted(target)
    description = "\n".join(
        str(row) for row in spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
    )
    if "days(checked_at)" not in description:
        spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD days(`checked_at`)")
    spark.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ("
        "'grain'='one quality result per run, layer, table, and rule', "
        "'key'='run_id,layer,table_name,rule_name')"
    )


def persist_quality_results(
    spark: SparkSession,
    settings: Settings,
    results: Sequence[QualityResult],
    *,
    quality_namespace: str = "quality",
) -> int:
    """Lưu trữ bất biến (Append-only) kết quả kiểm định vào bảng Iceberg 'lakehouse.quality.quality_results'."""
    if not results:
        raise ValueError("Cần ít nhất một kết quả chất lượng")
    results = _canonicalize_quality_results(results)
    run_ids = {item.run_id for item in results}
    if len(run_ids) != 1:
        raise ValueError("Mỗi lần lưu chỉ được chứa chính xác một run_id")
    run_id = next(iter(run_ids))
    target = _qualified(
        settings.iceberg_catalog_name, quality_namespace, "quality_results"
    )
    expected_grains = {_result_grain(item) for item in results}
    expected_payloads = {_quality_payload(item) for item in results}
    frame = _result_frame(spark, results)
    try:
        created = False
        with _batch_ownership_lock(spark, target, _QUALITY_INIT_LOCK_ID):
            exists = spark.catalog.tableExists(target)
            if not exists:
                create_or_replace(
                    spark,
                    target,
                    frame,
                    ("days(checked_at)",),
                    input_size_bytes=max(1, len(results) * 512),
                )
                _ensure_quality_table_contract(spark, target)
                created = True
        if not created:
            with _batch_ownership_lock(spark, target, f"quality-run:{run_id}"):
                existing = spark.table(target)
                if existing.where(existing.run_id == run_id).limit(1).count():
                    if not hasattr(existing, "select"):
                        raise ValueError(f"run_id chất lượng đã tồn tại: {run_id}")
                    actual_payloads = _quality_run_payloads(spark, target, run_id)
                    if len(actual_payloads) != len(set(actual_payloads)):
                        raise RuntimeError("Run chất lượng đã lưu chứa nội dung trùng lặp")
                    if set(actual_payloads) == expected_payloads:
                        return len(results)
                    raise ValueError(f"run_id chất lượng có nội dung xung đột: {run_id}")
                _ensure_quality_table_contract(spark, target)
                frame.writeTo(_quoted(target)).append()
    except BaseException as exc:
        try:
            committed_count = _reconcile_quality_output_count(
                spark,
                target,
                run_id,
                expected_grains,
            )
        except BaseException as reconciliation_error:
            exc.add_note(
                "Đối soát việc lưu kết quả chất lượng thất bại "
                f"({type(reconciliation_error).__name__})"
            )
        else:
            setattr(exc, "quality_output_count", committed_count)
        raise
    return len(results)


def _retry_pipeline_terminalization(
    *,
    spark: SparkSession,
    run_id: str,
    input_count: int,
    output_count: int,
    rejected_count: int,
    status: str,
    error_summary: str | None,
    control_namespace: str,
) -> str | None:
    """Thực hiện thử lại việc ghi nhận trạng thái cuối cùng của audit pipeline."""
    last_error: BaseException | None = None
    last_status: str | None = None
    expected_summary = truncate_error_summary(error_summary)
    expected = {
        "status": status,
        "input_count": input_count,
        "output_count": output_count,
        "rejected_count": rejected_count,
        "error_summary": expected_summary,
    }
    for _ in range(_AUDIT_FINALIZATION_ATTEMPTS):
        try:
            finish_pipeline_run(
                spark,
                run_id=run_id,
                input_count=input_count,
                output_count=output_count,
                rejected_count=rejected_count,
                status=status,
                completed_at=datetime.now(timezone.utc),
                error_summary=error_summary,
                namespace=control_namespace,
            )
        except BaseException as exc:
            last_error = exc
        try:
            outcome = pipeline_run_outcome(
                spark, run_id=run_id, namespace=control_namespace
            )
        except BaseException as exc:
            last_error = exc
            continue
        if outcome is None:
            last_status = None
            continue
        last_status = outcome.status
        mismatches = [
            f"{field}: mong đợi={value!r}, quan sát={getattr(outcome, field)!r}"
            for field, value in expected.items()
            if getattr(outcome, field) != value
        ]
        if not mismatches:
            return None
        if last_status in {"SUCCESS", "FAILED"}:
            return "Kết quả đọc lại trạng thái cuối audit pipeline không khớp: " + "; ".join(
                mismatches
            )
    suffix = "" if last_error is None else f" ({type(last_error).__name__})"
    return (
        f"Đã hết {_AUDIT_FINALIZATION_ATTEMPTS} lần thử kết thúc audit pipeline; "
        f"trạng thái lưu cuối cùng={last_status!r}{suffix}"
    )


def _alert_quality_gate_failure(
    *,
    run_id: str,
    batch_id: str,
    results: Sequence[QualityResult],
) -> None:
    """Gửi email cảnh báo khi cổng kiểm định chất lượng trả về mức ERROR.

    Đầu vào : mã run, mã batch và toàn bộ kết quả kiểm định của lần chạy.
    Xử lý   : lọc ra các bài kiểm tra có ``status == 'ERROR'``, tóm tắt tối đa 20 bài đầu
              (tránh gửi email dài hàng nghìn dòng) rồi chuyển cho module gửi SMTP.
    Đầu ra  : không trả về giá trị; mọi lỗi của kênh cảnh báo đều bị nuốt có chủ đích.

    Vì sao giới hạn 20 dòng? Khi một bảng nguồn hỏng nặng, hàng trăm bài kiểm tra có thể
    cùng đỏ một lúc. Email cảnh báo chỉ cần đủ thông tin để Quản trị viên biết "hỏng ở đâu,
    bắt đầu từ bài nào"; chi tiết đầy đủ đã nằm sẵn trong bảng ``lakehouse.quality.quality_results``.
    """
    try:
        from infrastructure.alerts.email_alert import send_alert_email

        failed = [item for item in results if item.status == "ERROR"]
        preview = "\n".join(
            (
                f"  - [{item.layer}.{item.table_name}] {item.rule_name}: "
                f"quan sát={item.observed_value} / kỳ vọng={item.threshold}"
            )[:300]
            for item in failed[:20]
        )
        remaining = len(failed) - 20
        if remaining > 0:
            preview += f"\n  ... và {remaining} bài kiểm định lỗi khác."
        send_alert_email(
            subject=f"Cổng chất lượng CHẶN pipeline — {len(failed)} kiểm định mức ERROR",
            body_text=(
                "Great Expectations phát hiện vi phạm chất lượng dữ liệu mức ERROR.\n"
                "Pipeline đã bị chặn lại để dữ liệu lỗi không chảy xuống tầng dưới.\n\n"
                f"  Run ID        : {run_id}\n"
                f"  Batch ID      : {batch_id}\n"
                f"  Tổng kiểm định: {len(results)}\n"
                f"  Số bài ERROR  : {len(failed)}\n\n"
                f"Danh sách kiểm định vi phạm:\n{preview}\n\n"
                "Truy vấn đầy đủ: SELECT * FROM lakehouse.quality.quality_results "
                f"WHERE run_id = '{run_id}' AND status = 'ERROR';\n"
            ),
            severity="ERROR",
        )
    except Exception:  # noqa: BLE001 - kênh cảnh báo không được phép che mất lỗi chất lượng gốc
        pass


def run_quality_job(
    *,
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    attempt_id: str | None = None,
    layers: Sequence[str] = SUPPORTED_LAYERS,
    bronze_namespace: str = "bronze",
    silver_namespace: str = "silver",
    gold_namespace: str = "gold",
    quarantine_namespace: str = "quarantine",
    quality_namespace: str = "quality",
    control_namespace: str = "control",
    batch_id: str | None = None,
    layer_checkers: Mapping[str, LayerChecker] | None = None,
) -> int:
    """Thực thi kiểm tra chất lượng theo thứ tự Bronze -> Silver -> Gold, ghi kết quả và dừng pipeline nếu gặp ERROR."""
    selected = tuple(layers)
    if not selected:
        raise ValueError("Phải chọn ít nhất một lớp quality")
    if len(set(selected)) != len(selected):
        raise ValueError("Các lớp quality được chọn không được trùng lặp")
    unknown = tuple(layer for layer in selected if layer not in SUPPORTED_LAYERS)
    if unknown:
        raise KeyError(f"Lớp chất lượng không xác định: {unknown[0]}")
    if tuple(sorted(selected, key=SUPPORTED_LAYERS.index)) != selected:
        raise ValueError("Các lớp quality phải theo thứ tự bronze, silver, gold")

    defaults: dict[str, LayerChecker] = {
        "bronze": lambda active_spark, active_settings, active_run_id: _bronze_checker(
            active_spark,
            active_settings,
            active_run_id,
            namespace=bronze_namespace,
        ),
        "silver": lambda active_spark, active_settings, active_run_id: _silver_checker(
            active_spark,
            active_settings,
            active_run_id,
            namespace=silver_namespace,
            quarantine_namespace=quarantine_namespace,
        ),
        "gold": lambda active_spark, active_settings, active_run_id: _gold_checker(
            active_spark,
            active_settings,
            active_run_id,
            namespace=gold_namespace,
        ),
    }
    overrides = {} if layer_checkers is None else dict(layer_checkers)
    defaults.update(overrides)
    results: list[QualityResult] = []
    audit_run_id = run_id if attempt_id is None else attempt_run_id(run_id, attempt_id)
    output_count = 0
    phase = "audit_initialization"
    gx_context = None
    try:
        ensure_audit_tables(spark, namespace=control_namespace)
        phase = "audit_start"
        start_pipeline_run(
            spark,
            run_id=audit_run_id,
            logical_run_id=run_id,
            attempt_id="1" if attempt_id is None else attempt_id,
            batch_id=batch_id or run_id,
            job_name="quality_checks",
            layer="quality",
            mode="full",
            started_at=datetime.now(timezone.utc),
            namespace=control_namespace,
        )
        phase = "gx_initialization"
        default_layers = tuple(layer for layer in selected if layer not in overrides)
        if default_layers:
            try:
                from data_quality.run_checks import (
                    load_gx_context,
                    run_gx_layer_validations,
                    validate_suite_contracts,
                )
                validate_suite_contracts()
                gx_context = load_gx_context()
            except ImportError:
                gx_context = None

        namespaces = {
            "bronze": bronze_namespace,
            "silver": silver_namespace,
            "gold": gold_namespace,
        }
        for layer in selected:
            phase = f"{layer}_checks"
            layer_results: tuple[QualityResult, ...]
            if layer in overrides or gx_context is None:
                layer_results = tuple(defaults[layer](spark, settings, run_id))
            else:
                gx_results = run_gx_layer_validations(
                    context=gx_context,
                    spark=spark,
                    settings=settings,
                    layer=layer,
                    namespace=namespaces[layer],
                    run_id=run_id,
                    batch_id=batch_id if layer == "bronze" else None,
                )
                native_results = defaults[layer](spark, settings, run_id)
                layer_results = tuple(gx_results) + tuple(native_results)
            layer_results = _canonicalize_quality_results(layer_results)
            results.extend(layer_results)
            if aggregate_status(layer_results) == "ERROR":
                break

        if gx_context is not None:
            phase = "gx_data_docs"
            try:
                gx_context.build_data_docs()
            except Exception:
                pass

        phase = "quality_persistence"
        output_count = persist_quality_results(
            spark,
            settings,
            results,
            quality_namespace=quality_namespace,
        )
        gate_status = aggregate_status(results)
        audit_status = "FAILED" if gate_status == "ERROR" else "SUCCESS"
        if gate_status == "ERROR":
            # Cổng chất lượng chặn pipeline -> bắn email cảnh báo ngay cho Quản trị viên,
            # kèm danh sách cụ thể các bài kiểm định đã vi phạm để xử lý không phải mò log.
            _alert_quality_gate_failure(run_id=run_id, batch_id=batch_id, results=results)
        phase = "audit_finalization"
        audit_failure = _retry_pipeline_terminalization(
            spark=spark,
            run_id=audit_run_id,
            input_count=len(results),
            output_count=output_count,
            rejected_count=sum(item.status == "ERROR" for item in results),
            status=audit_status,
            error_summary=(
                "Cổng chất lượng trả về ERROR" if audit_status == "FAILED" else None
            ),
            control_namespace=control_namespace,
        )
        if audit_failure is not None:
            raise RuntimeError(audit_failure)
        return 2 if gate_status == "ERROR" else 0
    except Exception as exc:
        output_count = int(getattr(exc, "quality_output_count", output_count))
        try:
            finish_pipeline_run(
                spark,
                run_id=audit_run_id,
                input_count=len(results),
                output_count=output_count,
                rejected_count=sum(item.status == "ERROR" for item in results),
                status="FAILED",
                completed_at=datetime.now(timezone.utc),
                error_summary=f"{type(exc).__name__}: {phase}",
                namespace=control_namespace,
            )
        except Exception:
            pass
        raise


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Phân tích tham số dòng lệnh với cơ chế Zero-Config Defaults hoàn chỉnh."""
    parser = argparse.ArgumentParser(
        description="Run Spark-native Data Quality checks across Lakehouse layers."
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Mã phiên chạy chất lượng (mặc định: tự động sinh theo timestamp UTC).",
    )
    parser.add_argument(
        "--attempt-id",
        type=str,
        default=None,
        help="Số thứ tự lần thử của task (Airflow try_number).",
    )
    parser.add_argument(
        "--layers",
        type=str,
        default="bronze,silver,gold",
        help="Danh sách các lớp cần kiểm tra (mặc định: bronze,silver,gold).",
    )
    parser.add_argument(
        "--bronze-namespace",
        type=str,
        default="bronze",
        help="Schema Bronze trong catalog (mặc định: bronze).",
    )
    parser.add_argument(
        "--silver-namespace",
        type=str,
        default="silver",
        help="Schema Silver trong catalog (mặc định: silver).",
    )
    parser.add_argument(
        "--gold-namespace",
        type=str,
        default="gold",
        help="Schema Gold trong catalog (mặc định: gold).",
    )
    parser.add_argument(
        "--quarantine-namespace",
        type=str,
        default="quarantine",
        help="Schema Quarantine trong catalog (mặc định: quarantine).",
    )
    parser.add_argument(
        "--quality-namespace",
        type=str,
        default="quality",
        help="Schema Quality trong catalog (mặc định: quality).",
    )
    parser.add_argument(
        "--control-namespace",
        type=str,
        default="control",
        help="Schema Control trong catalog (mặc định: control).",
    )
    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="Mã lô dữ liệu (nếu có).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Điểm khởi nhập chính của job kiểm định chất lượng."""
    args = parse_args(argv)
    run_id = args.run_id or datetime.now(timezone.utc).strftime("quality_%Y%m%d_%H%M%S")
    layers = tuple(item.strip() for item in args.layers.split(","))

    settings = Settings(
        minio_root_user=os.environ.get("AWS_ACCESS_KEY_ID", "olistprocess"),
        minio_root_password=os.environ.get("AWS_SECRET_ACCESS_KEY", "admin_process"),
        iceberg_db_password=os.environ.get("ICEBERG_DB_PASSWORD", "admin"),
        s3_endpoint=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        iceberg_warehouse=os.environ.get("ICEBERG_WAREHOUSE", "s3://lakehouse/"),
        spark_master_url=os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
        postgres_host=os.environ.get("POSTGRES_HOST", "postgres"),
    )

    spark = build_spark_session(
        settings=settings,
        app_name=f"QualityChecks-{run_id}",
    )
    try:
        return run_quality_job(
            spark=spark,
            settings=settings,
            run_id=run_id,
            attempt_id=args.attempt_id,
            layers=layers,
            bronze_namespace=args.bronze_namespace,
            silver_namespace=args.silver_namespace,
            gold_namespace=args.gold_namespace,
            quarantine_namespace=args.quarantine_namespace,
            quality_namespace=args.quality_namespace,
            control_namespace=args.control_namespace,
            batch_id=args.batch_id,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys
    sys.exit(main())
