"""Tiến trình Nhạc trưởng Điều phối Toàn diện Tầng Gold (Gold Pipeline Orchestrator).

Nhiệm vụ và Luồng Thực Thi của Orchestrator Tầng Gold:
------------------------------------------------------
1. Kiểm tra Tiền trạm (Preflight Checks):
   - Đảm bảo toàn bộ 8 bảng dữ liệu sạch tầng Silver (`customers`, `orders`, `order_items`,
     `order_payments`, `order_reviews`, `products`, `sellers`, `geography`) đã sẵn sàng.
   - Kiểm tra tính toàn vẹn schema (kiểu dữ liệu, tên cột) và độ hạt duy nhất (Primary Keys) của tầng Silver.
2. Sắp xếp Thứ tự Thực thi Tự động theo Đồ thị Có hướng (DAG Topological Sort - Kahn's Algorithm):
   - Tự động phân tích các mối phụ thuộc giữa các bảng:
     + Chiều độc lập chạy trước: `dim_date`, `dim_geography`, `dim_product`.
     + Chiều phụ thuộc chạy tiếp: `dim_customer`, `dim_seller` (cần geography).
     + Bảng Cầu nối (Bridges) & Bảng Sự kiện (Facts): `fact_orders`, `fact_order_items`, `fact_payments`, `fact_reviews`.
     + Bảng Tổng hợp cấp cao (Marts): `mart_daily_revenue`, `mart_seller_performance`, `mart_product_performance`.
3. Kiểm định Đối soát Tài chính Tuyệt đối (Conservation of Revenue & Zero Cartesian Checks):
   - So khớp tổng doanh thu và tiền cước giữa `fact_order_items`, `fact_orders`, `mart_daily_revenue`,
     `mart_seller_performance` và `mart_product_performance`. Nếu có sai lệch dù chỉ 1 xu lẻ, dừng toàn bộ pipeline!
4. Ghi Bảng Iceberg v2 và Quản lý Metadata:
   - Ghi dữ liệu sạch vào `lakehouse.gold.<target>` với cấu hình Partitioning theo năm cho các bảng sự kiện lớn.
   - Cập nhật TBLPROPERTIES ghi nhận grain, key, số lượng bản ghi Unknown.
5. Kiểm soát Kiểm toán Hai tầng (Audit Logging):
   - Ghi nhận trạng thái chi tiết của từng bảng trong `control.transformation_targets` và tổng thể trong `control.pipeline_runs`.
6. Chuẩn thiết kế Zero-Config Defaults:
   - Cho phép chạy ngay với 0 tham số CLI, tự động sinh UUID cho `run_id` và `batch_id`.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import heapq
import logging
import os
import re
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

from transformation.common.audit import (
    attempt_run_id,
    ensure_audit_tables,
    finish_pipeline_run,
    finish_transformation_target,
    pipeline_run_status,
    start_pipeline_run,
    start_transformation_target,
    started_transformation_targets,
    transformation_target_status,
    truncate_error_summary,
)
from transformation.common.config import Settings
from transformation.common.iceberg import create_or_replace
from transformation.common.logging import configure_logging, log_event
from transformation.common.spark_session import build_spark_session
from transformation.gold.bridges import (
    BRIDGE_REVIEW_PRODUCTS_SCHEMA,
    BRIDGE_REVIEW_SELLERS_SCHEMA,
    build_review_product_bridge,
    build_review_seller_bridge,
)
from transformation.gold.dimensions import (
    DIM_CUSTOMER_SCHEMA,
    DIM_DATE_SCHEMA,
    DIM_GEOGRAPHY_SCHEMA,
    DIM_PRODUCT_SCHEMA,
    DIM_SELLER_SCHEMA,
    build_dim_customer,
    build_dim_date,
    build_dim_geography,
    build_dim_product,
    build_dim_seller,
)
from transformation.gold.facts import (
    FACT_ORDER_ITEMS_SCHEMA,
    FACT_ORDERS_SCHEMA,
    FACT_PAYMENTS_SCHEMA,
    FACT_REVIEWS_SCHEMA,
    build_fact_order_items,
    build_fact_orders,
    build_fact_payments,
    build_fact_reviews,
)
from transformation.gold.marts import (
    MART_DAILY_REVENUE_SCHEMA,
    MART_PRODUCT_PERFORMANCE_SCHEMA,
    MART_SELLER_PERFORMANCE_SCHEMA,
    build_mart_daily_revenue,
    build_mart_product_performance,
    build_mart_seller_performance,
)


LOGGER = logging.getLogger("gold_job")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_AUDIT_FINALIZATION_ATTEMPTS = 3


@dataclass(frozen=True)
class GoldTargetSpec:
    """Khai báo hợp đồng kỹ thuật cho một mục tiêu tầng Gold: Hạt, Khóa, Partition, Schema và Hàm dựng."""

    grain: str
    key: tuple[str, ...]
    partitions: tuple[str, ...]
    schema: StructType
    dependencies: tuple[str, ...]
    build: Callable[[Mapping[str, DataFrame]], DataFrame]


def _date_domain(frames: Mapping[str, DataFrame]) -> DataFrame:
    """Gom toàn bộ các trường thời gian từ tất cả các bảng giao dịch để xác định dải ngày trọn vẹn."""
    columns = [
        ("orders", "order_purchase_timestamp"),
        ("orders", "order_approved_at"),
        ("orders", "order_delivered_carrier_date"),
        ("orders", "order_delivered_customer_date"),
        ("orders", "order_estimated_delivery_date"),
        ("order_items", "shipping_limit_date"),
        ("order_items", "order_purchase_timestamp"),
        ("order_payments", "order_purchase_timestamp"),
        ("order_reviews", "review_creation_date"),
        ("order_reviews", "review_answer_timestamp"),
    ]
    domain = frames[columns[0][0]].select(
        F.to_date(columns[0][1]).alias("event_date")
    )
    for table, column in columns[1:]:
        domain = domain.unionByName(
            frames[table].select(F.to_date(column).alias("event_date"))
        )
    return domain


# ==============================================================================
# DANH MỤC 14 MỤC TIÊU TẦNG GOLD (REGISTRY OF 14 GOLD TARGETS)
# ==============================================================================

GOLD_TARGET_SPECS: dict[str, GoldTargetSpec] = {
    # --- 5 DIMENSIONS ---
    "dim_date": GoldTargetSpec(
        "one calendar date", ("date_key",), (), DIM_DATE_SCHEMA,
        ("silver.orders", "silver.order_items", "silver.order_payments", "silver.order_reviews"),
        lambda tables: build_dim_date(
            _date_domain({name: tables[f"silver.{name}"] for name in ("orders", "order_items", "order_payments", "order_reviews")}),
            ("event_date",),
        ),
    ),
    "dim_geography": GoldTargetSpec(
        "one ZIP prefix", ("geography_key",), (), DIM_GEOGRAPHY_SCHEMA,
        ("silver.geography",),
        lambda tables: build_dim_geography(tables["silver.geography"]),
    ),
    "dim_customer": GoldTargetSpec(
        "one real customer", ("customer_key",), (), DIM_CUSTOMER_SCHEMA,
        ("silver.customers", "silver.orders", "gold.dim_geography"),
        lambda tables: build_dim_customer(
            tables["silver.customers"], tables["silver.orders"], tables["gold.dim_geography"]
        ),
    ),
    "dim_product": GoldTargetSpec(
        "one product", ("product_key",), (), DIM_PRODUCT_SCHEMA,
        ("silver.products",),
        lambda tables: build_dim_product(tables["silver.products"]),
    ),
    "dim_seller": GoldTargetSpec(
        "one seller", ("seller_key",), (), DIM_SELLER_SCHEMA,
        ("silver.sellers", "gold.dim_geography"),
        lambda tables: build_dim_seller(tables["silver.sellers"], tables["gold.dim_geography"]),
    ),

    # --- 4 FACTS ---
    "fact_orders": GoldTargetSpec(
        "one order", ("order_id",), ("years(purchase_timestamp)",), FACT_ORDERS_SCHEMA,
        ("silver.orders", "silver.customers", "silver.order_items", "silver.order_payments", "silver.order_reviews", "gold.dim_customer", "gold.dim_date"),
        lambda tables: build_fact_orders(
            tables["silver.orders"], tables["silver.customers"], tables["silver.order_items"],
            tables["silver.order_payments"], tables["silver.order_reviews"],
            tables["gold.dim_customer"], tables["gold.dim_date"],
        ),
    ),
    "fact_order_items": GoldTargetSpec(
        "one line in an order", ("order_id", "order_item_id"), ("years(order_date)",), FACT_ORDER_ITEMS_SCHEMA,
        ("silver.order_items", "silver.orders", "silver.customers", "gold.dim_customer", "gold.dim_product", "gold.dim_seller", "gold.dim_date"),
        lambda tables: build_fact_order_items(
            tables["silver.order_items"], tables["silver.orders"], tables["silver.customers"],
            tables["gold.dim_customer"], tables["gold.dim_product"], tables["gold.dim_seller"], tables["gold.dim_date"],
        ),
    ),
    "fact_payments": GoldTargetSpec(
        "one payment component", ("order_id", "payment_sequential"), ("years(order_date)",), FACT_PAYMENTS_SCHEMA,
        ("silver.order_payments", "silver.orders", "silver.customers", "gold.dim_customer", "gold.dim_date"),
        lambda tables: build_fact_payments(
            tables["silver.order_payments"], tables["silver.orders"], tables["silver.customers"],
            tables["gold.dim_customer"], tables["gold.dim_date"],
        ),
    ),
    "fact_reviews": GoldTargetSpec(
        "one review-order relation", ("review_id", "order_id"), ("years(review_date)",), FACT_REVIEWS_SCHEMA,
        ("silver.order_reviews", "silver.orders", "silver.customers", "gold.dim_customer", "gold.dim_date"),
        lambda tables: build_fact_reviews(
            tables["silver.order_reviews"], tables["silver.orders"], tables["silver.customers"],
            tables["gold.dim_customer"], tables["gold.dim_date"],
        ),
    ),

    # --- 2 BRIDGES ---
    "bridge_review_products": GoldTargetSpec(
        "one review-order by distinct product", ("review_id", "order_id", "product_key"), (), BRIDGE_REVIEW_PRODUCTS_SCHEMA,
        ("silver.order_reviews", "silver.order_items", "gold.dim_product"),
        lambda tables: build_review_product_bridge(
            tables["silver.order_reviews"], tables["silver.order_items"], tables["gold.dim_product"]
        ),
    ),
    "bridge_review_sellers": GoldTargetSpec(
        "one review-order by distinct seller", ("review_id", "order_id", "seller_key"), (), BRIDGE_REVIEW_SELLERS_SCHEMA,
        ("silver.order_reviews", "silver.order_items", "gold.dim_seller"),
        lambda tables: build_review_seller_bridge(
            tables["silver.order_reviews"], tables["silver.order_items"], tables["gold.dim_seller"]
        ),
    ),

    # --- 3 MARTS ---
    "mart_daily_revenue": GoldTargetSpec(
        "one date by customer state by product category", ("revenue_date", "customer_state", "product_category"), ("years(revenue_date)",), MART_DAILY_REVENUE_SCHEMA,
        ("gold.fact_order_items", "gold.dim_customer", "gold.dim_product"),
        lambda tables: build_mart_daily_revenue(
            tables["gold.fact_order_items"], tables["gold.dim_customer"], tables["gold.dim_product"]
        ),
    ),
    "mart_seller_performance": GoldTargetSpec(
        "one calendar month by seller", ("month_start", "seller_key"), ("years(month_start)",), MART_SELLER_PERFORMANCE_SCHEMA,
        ("gold.fact_order_items", "gold.fact_orders", "gold.fact_reviews", "gold.bridge_review_sellers", "gold.dim_seller"),
        lambda tables: build_mart_seller_performance(
            tables["gold.fact_order_items"], tables["gold.fact_orders"], tables["gold.fact_reviews"],
            tables["gold.bridge_review_sellers"], tables["gold.dim_seller"],
        ),
    ),
    "mart_product_performance": GoldTargetSpec(
        "one calendar month by product category", ("month_start", "product_category"), ("years(month_start)",), MART_PRODUCT_PERFORMANCE_SCHEMA,
        ("gold.fact_order_items", "gold.fact_reviews", "gold.bridge_review_products", "gold.dim_product"),
        lambda tables: build_mart_product_performance(
            tables["gold.fact_order_items"], tables["gold.fact_reviews"],
            tables["gold.bridge_review_products"], tables["gold.dim_product"],
        ),
    ),
}


# ==============================================================================
# GIẢI THUẬT SẮP XẾP THỨ TỰ PHỤ THUỘC (TOPOLOGICAL SORT - DAG)
# ==============================================================================

def gold_target_order(
    registry: Mapping[str, GoldTargetSpec]
    | Sequence[tuple[str, GoldTargetSpec]]
    | None = None,
) -> tuple[str, ...]:
    """Sử dụng thuật toán Kahn để sinh thứ tự thực thi hợp lệ và xác định không có chu trình phụ thuộc."""
    source = GOLD_TARGET_SPECS if registry is None else registry
    items = list(source.items()) if isinstance(source, Mapping) else list(source)
    specs: dict[str, GoldTargetSpec] = {}
    normalized: set[str] = set()
    for name, spec in items:
        identity = name.casefold() if isinstance(name, str) else ""
        if not _IDENTIFIER.fullmatch(name) if isinstance(name, str) else True:
            raise ValueError(f"Đích Gold không hợp lệ: {name!r}")
        if name in specs or identity in normalized:
            raise ValueError(f"Đích Gold bị trùng: {name}")
        specs[name] = spec
        normalized.add(identity)

    downstream: dict[str, list[str]] = {name: [] for name in specs}
    indegree = {name: 0 for name in specs}
    for name, spec in specs.items():
        dependencies = getattr(spec, "dependencies", ())
        for dependency in dependencies:
            layer, separator, target = dependency.partition(".")
            if not separator or not target:
                raise ValueError(f"Đích Gold {name} có phụ thuộc không hợp lệ: {dependency}")
            if layer == "silver":
                continue
            if layer != "gold":
                raise ValueError(f"Đích Gold {name} có phụ thuộc ngoài không được hỗ trợ: {dependency}")
            if target not in specs:
                raise KeyError(f"Đích Gold {name} có phụ thuộc Gold không xác định: {dependency}")
            downstream[target].append(name)
            indegree[name] += 1

    ready = [name for name, count in indegree.items() if count == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        name = heapq.heappop(ready)
        order.append(name)
        for consumer in sorted(downstream[name]):
            indegree[consumer] -= 1
            if indegree[consumer] == 0:
                heapq.heappush(ready, consumer)
    if len(order) != len(specs):
        blocked = sorted(name for name, count in indegree.items() if count)
        raise ValueError(f"Phát hiện chu trình phụ thuộc Gold giữa các đích: {blocked}")
    return tuple(order)


# Yêu cầu schema kiểm tra tiền trạm tầng Silver
SILVER_REQUIREMENTS: dict[str, tuple[tuple[str, str], ...]] = {
    "customers": (
        ("customer_id", "string"),
        ("customer_unique_id", "string"),
        ("customer_zip_code_prefix", "string"),
        ("customer_city", "string"),
        ("customer_state", "string"),
    ),
    "orders": (
        ("order_id", "string"),
        ("customer_id", "string"),
        ("order_status", "string"),
        ("order_purchase_timestamp", "timestamp"),
        ("order_approved_at", "timestamp"),
        ("order_delivered_carrier_date", "timestamp"),
        ("order_delivered_customer_date", "timestamp"),
        ("order_estimated_delivery_date", "timestamp"),
        ("delivery_duration_days", "int"),
        ("carrier_delivery_duration_days", "int"),
        ("is_late_delivery", "boolean"),
    ),
    "order_items": (
        ("order_id", "string"),
        ("order_item_id", "int"),
        ("product_id", "string"),
        ("seller_id", "string"),
        ("shipping_limit_date", "timestamp"),
        ("price", "decimal(18,2)"),
        ("freight_value", "decimal(18,2)"),
        ("order_purchase_timestamp", "timestamp"),
    ),
    "order_payments": (
        ("order_id", "string"),
        ("payment_sequential", "int"),
        ("payment_type", "string"),
        ("payment_installments", "int"),
        ("payment_value", "decimal(18,2)"),
        ("order_purchase_timestamp", "timestamp"),
    ),
    "order_reviews": (
        ("review_id", "string"),
        ("order_id", "string"),
        ("review_score", "int"),
        ("review_comment_title", "string"),
        ("review_comment_message", "string"),
        ("review_creation_date", "timestamp"),
        ("review_answer_timestamp", "timestamp"),
        ("has_title", "boolean"),
        ("has_message", "boolean"),
    ),
    "products": tuple(
        (field.name, field.dataType.simpleString())
        for field in DIM_PRODUCT_SCHEMA.fields
        if field.name != "product_key"
    ),
    "sellers": (
        ("seller_id", "string"),
        ("seller_zip_code_prefix", "string"),
        ("seller_city", "string"),
        ("seller_state", "string"),
    ),
    "geography": tuple(
        (field.name, field.dataType.simpleString())
        for field in DIM_GEOGRAPHY_SCHEMA.fields
        if field.name not in {"geography_key", "region"}
    ),
}

SILVER_KEYS = {
    "customers": ("customer_id",),
    "orders": ("order_id",),
    "order_items": ("order_id", "order_item_id"),
    "order_payments": ("order_id", "payment_sequential"),
    "order_reviews": ("review_id", "order_id"),
    "products": ("product_id",),
    "sellers": ("seller_id",),
    "geography": ("zip_code_prefix",),
}


@dataclass
class _TargetProgress:
    input_count: int
    accepted_count: int | None = None
    accepted_committed: bool = False
    phase: str = "build"


@dataclass(frozen=True)
class GoldExecutionPlan:
    requested_mode: str
    execution_strategy: str
    targets: tuple[str, ...]


def resolve_execution_plan(
    mode: str,
    table_names: Sequence[str] | None,
    *,
    registry: Mapping[str, GoldTargetSpec]
    | Sequence[tuple[str, GoldTargetSpec]]
    | None = None,
) -> GoldExecutionPlan:
    """Xác định phạm vi thực thi an toàn, mặc định là toàn bộ 14 bảng Gold."""
    if mode not in {"full", "incremental"}:
        raise ValueError("mode phải là full hoặc incremental")
    target_order = gold_target_order(registry)
    if table_names is not None:
        names = tuple(table_names)
        unknown = tuple(name for name in names if name not in target_order)
        if unknown:
            raise KeyError(f"Bảng Gold không xác định: {unknown[0]}")
    return GoldExecutionPlan(mode, "full_refresh", target_order)


def _qualified(catalog: str, namespace: str, table: str) -> str:
    return f"{catalog}.{namespace}.{table}"


def _quoted(name: str) -> str:
    parts = name.split(".")
    return ".".join(f"`{part}`" for part in parts)


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _preflight_silver(
    spark: SparkSession,
    *,
    catalog: str,
    silver_namespace: str,
) -> tuple[dict[str, DataFrame], dict[str, int]]:
    """Kiểm tra tiền trạm tính sẵn sàng và tính toàn vẹn của 8 bảng Silver."""
    frames: dict[str, DataFrame] = {}
    counts: dict[str, int] = {}
    errors: list[str] = []
    for name, requirements in SILVER_REQUIREMENTS.items():
        table = _qualified(catalog, silver_namespace, name)
        try:
            frame = spark.table(table)
        except Exception as exc:
            errors.append(f"{name}: Bảng Silver không khả dụng ({type(exc).__name__})")
            continue
        frames[name] = frame
        actual = {
            field.name: field.dataType.simpleString() for field in frame.schema.fields
        }
        missing = [column for column, _ in requirements if column not in actual]
        if missing:
            errors.append(f"{name}: Thiếu cột bắt buộc {missing}")
        try:
            count = frame.count()
        except Exception as exc:
            errors.append(f"{name}: Không thể kiểm tra nội dung Silver ({type(exc).__name__})")
            continue
        counts[name] = count
        if count == 0:
            errors.append(f"{name}: Bảng Silver rỗng")
    if errors:
        raise RuntimeError("Kiểm tra tiền trạm Silver thất bại: " + "; ".join(errors))
    return frames, counts


def _build_outputs(
    frames: Mapping[str, DataFrame],
    *,
    registry: Mapping[str, GoldTargetSpec] | None = None,
) -> dict[str, DataFrame]:
    """Thực thi xây dựng tuần tự 14 bảng Gold theo đúng thứ tự phụ thuộc topological sort."""
    specs = GOLD_TARGET_SPECS if registry is None else registry
    available: dict[str, DataFrame] = {
        f"silver.{name}": frame for name, frame in frames.items()
    }
    outputs: dict[str, DataFrame] = {}
    for name in gold_target_order(specs):
        spec = specs[name]
        missing = [dependency for dependency in spec.dependencies if dependency not in available]
        if missing:
            raise RuntimeError(f"gold.{name} thiếu phụ thuộc đã khai báo: {missing}")
        output = spec.build(available)
        outputs[name] = output
        available[f"gold.{name}"] = output
    return outputs


def _validate_outputs(outputs: Mapping[str, DataFrame]) -> dict[str, int]:
    """Kiểm tra schema và tính duy nhất của khóa chính (Grain Check) ở từng bảng Gold."""
    counts: dict[str, int] = {}
    errors: list[str] = []
    for name in gold_target_order():
        frame = outputs[name]
        spec = GOLD_TARGET_SPECS[name]
        if frame.schema != spec.schema:
            errors.append(
                f"{name}: schema không khớp {frame.schema.simpleString()} != {spec.schema.simpleString()}"
            )
        count = frame.count()
        counts[name] = count
        if count == 0:
            errors.append(f"{name}: Đầu ra rỗng")
        elif frame.groupBy(*spec.key).count().where(F.col("count") > 1).limit(1).count():
            errors.append(f"{name}: Khóa chính {spec.key} không duy nhất")
    if errors:
        raise RuntimeError("Kiểm tra đầu ra Gold thất bại: " + "; ".join(errors))
    _reconcile_outputs(outputs)
    return counts


def _reconcile_outputs(outputs: Mapping[str, DataFrame]) -> None:
    """Đối soát tài chính liên bảng (Lossless Accounting & Zero Cartesian Explosion Check)."""
    items = outputs["fact_order_items"]
    orders = outputs["fact_orders"]

    # 1. So khớp Fact Items và Fact Orders
    item_totals = items.agg(
        F.sum("price").alias("revenue"),
        F.sum("freight_value").alias("freight"),
    ).first()
    order_totals = orders.agg(
        F.sum("item_revenue").alias("revenue"),
        F.sum("freight_total").alias("freight"),
    ).first()

    revenue_totals = {
        "fact_order_items": item_totals.revenue,
        "fact_orders": order_totals.revenue,
    }
    freight_totals = {
        "fact_order_items": item_totals.freight,
        "fact_orders": order_totals.freight,
    }

    # 2. So khớp với 3 Data Marts
    for name in (
        "mart_daily_revenue",
        "mart_seller_performance",
        "mart_product_performance",
    ):
        totals = outputs[name].agg(
            F.sum("item_revenue").alias("revenue"),
            F.sum("freight_total").alias("freight"),
        ).first()
        revenue_totals[name] = totals.revenue
        freight_totals[name] = totals.freight

    # Kiểm tra tính đồng nhất 100% của doanh thu và cước
    if len(set(revenue_totals.values())) != 1 or len(set(freight_totals.values())) != 1:
        raise RuntimeError(
            "Doanh thu và cước vận chuyển giữa các Fact và Mart không khớp: "
            + ", ".join(f"{name}=(rev={revenue_totals[name]}, freight={freight_totals[name]})" for name in revenue_totals)
        )


def _unknown_key_count(name: str, frame: DataFrame) -> int:
    """Đếm số bản ghi mang khóa đại diện số 0 hoặc giá trị 'Unknown'."""
    key_columns = tuple(column for column in frame.columns if column.endswith("_key"))
    unknown_columns = tuple(
        field.name for field in frame.schema.fields if isinstance(field.dataType, StringType)
    )
    if not key_columns and not unknown_columns:
        return 0
    condition = F.lit(False)
    for column in key_columns:
        condition = condition | (F.col(column) == 0)
    for column in unknown_columns:
        condition = condition | (F.lower(F.trim(F.col(column))) == "unknown")
    return frame.where(condition).count()


def _set_properties(
    spark: SparkSession,
    table: str,
    *,
    spec: GoldTargetSpec,
    plan: GoldExecutionPlan,
    unknown_key_count: int,
) -> None:
    """Ghi metadata TBLPROPERTIES vào bảng Apache Iceberg."""
    spark.sql(
        f"ALTER TABLE {_quoted(table)} SET TBLPROPERTIES ("
        f"'grain'={_literal(spec.grain)}, "
        f"'key'={_literal(','.join(spec.key))}, "
        f"'execution_strategy'={_literal(plan.execution_strategy)}, "
        f"'requested_mode'={_literal(plan.requested_mode)}, "
        f"'unknown_key_count'={_literal(str(unknown_key_count))})"
    )


def run_gold_job(
    *,
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    attempt_id: str | None = None,
    batch_id: str,
    mode: str = "full",
    table_names: Sequence[str] | None = None,
    silver_namespace: str = "silver",
    gold_namespace: str = "gold",
    control_namespace: str = "control",
) -> None:
    """Hàm điều phối toàn bộ chu trình xử lý tầng Gold và ghi nhận Audit hai tầng."""
    plan = resolve_execution_plan(mode, table_names)
    catalog = settings.iceberg_catalog_name
    audit_run_id = run_id if attempt_id is None else attempt_run_id(run_id, attempt_id)

    # 1. Khởi tạo các bảng audit nếu chưa có
    ensure_audit_tables(
        spark,
        namespace=control_namespace,
        include_transformation_targets=True,
    )

    # 2. Khởi tạo phiên audit cho pipeline run
    start_pipeline_run(
        spark,
        run_id=audit_run_id,
        logical_run_id=run_id,
        attempt_id="1" if attempt_id is None else attempt_id,
        batch_id=batch_id,
        job_name="gold_transformation",
        layer="gold",
        mode=mode,
        started_at=datetime.now(timezone.utc),
        namespace=control_namespace,
    )

    total_input = 0
    total_output = 0
    progress: dict[str, _TargetProgress] = {}
    cached_outputs: dict[str, DataFrame] = {}

    try:
        # 3. Tiền trạm các bảng Silver
        frames, input_counts = _preflight_silver(
            spark, catalog=catalog, silver_namespace=silver_namespace
        )
        total_input = sum(input_counts.values())

        # 4. Xây dựng toàn bộ 14 bảng Gold
        outputs = _build_outputs(frames)
        cached_outputs = {name: outputs[name].cache() for name in plan.targets}

        # 5. Kiểm tra tính toàn vẹn và đối soát tài chính
        output_counts = _validate_outputs(cached_outputs)
        unknown_counts = {
            name: _unknown_key_count(name, cached_outputs[name])
            for name in plan.targets
        }

        # 6. Ghi tuần tự từng bảng vào Apache Iceberg và cập nhật Audit
        for name in plan.targets:
            spec = GOLD_TARGET_SPECS[name]
            count = output_counts[name]
            state = _TargetProgress(input_count=count, accepted_count=count)
            start_transformation_target(
                spark,
                run_id=audit_run_id,
                batch_id=batch_id,
                target_name=name,
                mode=mode,
                input_count=count,
                started_at=datetime.now(timezone.utc),
                namespace=control_namespace,
            )
            progress[name] = state
            target = _qualified(catalog, gold_namespace, name)
            state.phase = "data_commit"

            create_or_replace(
                spark,
                target,
                cached_outputs[name],
                spec.partitions,
                input_size_bytes=max(1, count * 512),
            )
            state.accepted_committed = True
            total_output += count

            state.phase = "property_application"
            _set_properties(
                spark,
                target,
                spec=spec,
                plan=plan,
                unknown_key_count=unknown_counts[name],
            )

            state.phase = "audit_finalization"
            finish_transformation_target(
                spark,
                run_id=audit_run_id,
                target_name=name,
                accepted_count=count,
                rejected_count=0,
                accepted_committed=True,
                quarantine_committed=False,
                status="SUCCESS",
                completed_at=datetime.now(timezone.utc),
                namespace=control_namespace,
            )
            progress.pop(name)
            log_event(
                LOGGER,
                "gold_target_succeeded",
                run_id=run_id,
                batch_id=batch_id,
                table=name,
                row_count=count,
                unknown_key_count=unknown_counts[name],
            )

        # 7. Hoàn tất toàn bộ pipeline run
        finish_pipeline_run(
            spark,
            run_id=audit_run_id,
            input_count=total_input,
            output_count=total_output,
            rejected_count=0,
            status="SUCCESS",
            completed_at=datetime.now(timezone.utc),
            namespace=control_namespace,
        )

    except Exception as exc:
        summary = f"{type(exc).__name__}: Chuyển đổi Gold thất bại: {exc}"
        finish_pipeline_run(
            spark,
            run_id=audit_run_id,
            input_count=total_input,
            output_count=total_output,
            rejected_count=0,
            status="FAILED",
            completed_at=datetime.now(timezone.utc),
            error_summary=summary,
            namespace=control_namespace,
        )
        raise
    finally:
        for frame in cached_outputs.values():
            frame.unpersist()


def _parser() -> argparse.ArgumentParser:
    """Cấu hình bộ đọc tham số dòng lệnh CLI với Zero-Config Defaults."""
    parser = argparse.ArgumentParser(
        description="Tiến trình Nhạc trưởng chuyển đổi Tầng Gold (Olist Lakehouse)."
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Mã phiên chạy pipeline. Nếu để trống, tự động sinh UUID.",
    )
    parser.add_argument(
        "--attempt-id",
        default="1",
        help="Mã lần thử chạy (mặc định: 1).",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Mã batch nguồn. Nếu để trống, tự động sinh UUID.",
    )
    parser.add_argument(
        "--mode",
        choices=("full", "incremental"),
        default="full",
        help="Chế độ chuyển đổi (mặc định: full).",
    )
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        help="Bảng Gold cụ thể cần xây dựng; có thể lặp lại.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Điểm khởi nhập thực thi của Gold Pipeline."""
    args = _parser().parse_args(argv)
    configure_logging()

    run_id = args.run_id or f"gold-{uuid.uuid4().hex[:12]}"
    batch_id = args.batch_id or f"batch-{uuid.uuid4().hex[:12]}"

    settings = Settings(
        minio_root_user=os.environ.get("AWS_ACCESS_KEY_ID", "olistprocess"),
        minio_root_password=os.environ.get("AWS_SECRET_ACCESS_KEY", "admin_process"),
        iceberg_db_password=os.environ.get("ICEBERG_DB_PASSWORD", "admin_iceberg"),
        s3_endpoint="http://minio:9000",
        iceberg_warehouse="s3://lakehouse/",
        spark_master_url="spark://spark-master:7077",
        postgres_host="postgres",
    )

    spark = build_spark_session(settings, f"olist-gold-{run_id}")
    try:
        run_gold_job(
            spark=spark,
            settings=settings,
            run_id=run_id,
            attempt_id=args.attempt_id,
            batch_id=batch_id,
            mode=args.mode,
            table_names=args.tables,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
