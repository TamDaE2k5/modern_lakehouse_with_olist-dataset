"""Job bảo trì & tối ưu hóa bảng Apache Iceberg (phiên bản tinh gọn - KISS).

MỤC ĐÍCH CỦA MODULE
-------------------
Thực thi **đúng một** hành động bảo trì trên **đúng một** bảng Iceberg nằm trong
danh sách cho phép (allowlist), sau đó ghi lại kết quả đo lường được vào bảng
kiểm toán ``lakehouse.control.maintenance_results``.

ĐẦU VÀO (Input):
    Các đối số dòng lệnh do Airflow DAG ``olist_maintenance`` truyền xuống qua
    ``spark-submit``: ``--namespace``, ``--table``, ``--action``,
    ``--retention-hours``, ``--orphan-older-than-hours``, ``--run-id``, ``--batch-id``...

XỬ LÝ (Logic):
    1. Thẩm định tham số (allowlist namespace/table/action + ngưỡng thời gian tối thiểu).
    2. Đo trạng thái bảng TRƯỚC khi tác động (số data file, tổng dung lượng, số snapshot).
    3. Gọi trực tiếp một trong 3 thủ tục hệ thống chuẩn của Apache Iceberg:
         - ``system.rewrite_data_files``  : nén/gộp các tệp Parquet nhỏ về mục tiêu 128MB.
         - ``system.expire_snapshots``    : cắt tỉa snapshot lịch sử quá hạn lưu giữ.
         - ``system.remove_orphan_files`` : xóa tệp mồ côi không còn thuộc snapshot nào.
       (Riêng hành động ``dry-run`` chỉ khảo sát metadata, KHÔNG thay đổi dữ liệu.)
    4. Đo trạng thái bảng SAU khi tác động và tính hiệu quả thực tế.

ĐẦU RA (Output):
    - Một bản ghi trong ``lakehouse.control.maintenance_results`` (số liệu trước/sau).
    - Một bản ghi vòng đời trong ``lakehouse.control.pipeline_runs``
      (``job_name='iceberg_maintenance'``, ``layer='maintenance'``) để Prometheus
      exporter ``monitoring/exporters/pipeline_metrics.py`` scrape ra Grafana.
    - Riêng ``dry-run`` ghi thêm một file báo cáo JSON thuần (không ký số).
    - Mã thoát 0 nếu thành công, khác 0 nếu thất bại (Airflow sẽ bắt được).

VÌ SAO LÀM THEO CÁCH NÀY (Luận cứ thiết kế)?
--------------------------------------------
Phiên bản tiền nhiệm của file này dài 2.569 dòng vì tự chế một tầng điều phối
riêng: ký token HMAC-SHA256 cho báo cáo dry-run, sinh nonce, giành "lease lock"
độc quyền, tự xử lý trạng thái UNKNOWN, tự chống trùng bản ghi (deduplicate),
tự readback xác nhận từng lần ghi. Toàn bộ khối đó là **over-engineering**:

  * Việc điều tiết tài nguyên & chống chạy chồng chéo đã có sẵn ở tầng Airflow:
    Pool ``lakehouse_writes`` (8 slot) + ``max_active_runs=1`` trên DAG.
  * Việc đảm bảo nguyên tử khi ghi đã có sẵn ở tầng Apache Iceberg: mỗi thủ tục
    hệ thống commit theo cơ chế snapshot nguyên tử (atomic commit) của chính Iceberg.
  * Chuỗi Jinja template dùng để truyền token HMAC giữa các task chính là nguyên nhân
    gây lỗi lệch pha làm treo task ``rewrite_data_files``.

Vì vậy phiên bản này tuân thủ nguyên tắc KISS: chỉ giữ lại phần kiểm soát an toàn
thực sự có giá trị (allowlist đích tác động + ngưỡng thời gian tối thiểu, nhằm
chặn việc vô tình xóa snapshot/tệp còn đang được dùng), và giao mọi việc còn lại
cho đúng thành phần đã được thiết kế để lo việc đó.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import re

from pyspark.sql import SparkSession

from transformation.common.audit import (
    attempt_run_id,
    ensure_audit_tables,
    finish_pipeline_run,
    start_pipeline_run,
    truncate_error_summary,
)
from transformation.common.config import Settings
from transformation.common.logging import configure_logging, log_event
from transformation.common.spark_session import build_spark_session


LOGGER = logging.getLogger(__name__)

# Kích thước tệp Parquet mục tiêu sau khi nén: 128 MiB.
# Đây là ngưỡng cân bằng kinh điển cho object storage: đủ lớn để Trino/Spark không
# phải trả chi phí mở hàng nghìn tệp nhỏ, nhưng vẫn đủ nhỏ để song song hóa tốt.
TARGET_FILE_SIZE_BYTES = 134_217_728

# Ngưỡng an toàn tối thiểu. Không cho phép xóa snapshot trẻ hơn 7 ngày và tệp mồ côi
# trẻ hơn số giờ lưu giữ snapshot — tránh xóa nhầm tệp của một giao dịch ghi đang dở.
MIN_RETENTION_HOURS = 168  # 7 ngày
MIN_RETAINED_SNAPSHOTS = 2  # Luôn giữ lại ít nhất 2 snapshot để còn khả năng time-travel/rollback

# Chuỗi định danh runtime an toàn: chặn ký tự lạ lọt vào tên file, đường dẫn S3 và câu SQL.
SAFE_RUNTIME_VALUE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,119}$")

# Danh sách trắng các bảng được phép bảo trì, khai báo tường minh theo từng namespace.
# Vì sao cần allowlist? Thủ tục remove_orphan_files XÓA VĨNH VIỄN tệp trên MinIO S3.
# Một lỗi gõ nhầm tên bảng/namespace có thể gây mất dữ liệu không khôi phục được.
ALLOWED_TARGETS: dict[str, frozenset[str]] = {
    "bronze": frozenset(
        {
            "customers", "orders", "order_items", "order_payments", "order_reviews",
            "products", "sellers", "geolocation", "product_category_translation",
        }
    ),
    "silver": frozenset(
        {
            "customers", "orders", "order_items", "order_payments", "order_reviews",
            "products", "sellers", "geography",
        }
    ),
    "gold": frozenset(
        {
            "dim_date", "dim_customer", "dim_product", "dim_seller", "dim_geography",
            "fact_orders", "fact_order_items", "fact_payments", "fact_reviews",
            "bridge_review_products", "bridge_review_sellers",
            "mart_daily_revenue", "mart_seller_performance", "mart_product_performance",
        }
    ),
}

# Các hành động làm thay đổi dữ liệu/tệp vật lý, và hành động chỉ-đọc "dry-run".
MUTATING_ACTIONS = frozenset({"rewrite-data-files", "expire-snapshots", "remove-orphan-files"})
ALLOWED_ACTIONS = MUTATING_ACTIONS | {"dry-run"}

# Lược đồ bảng kiểm toán kết quả bảo trì (khai báo dạng SQL DDL cho dễ đọc).
# Các cột `action`, `status`, `rewritten_data_files_count`, `started_at`, `completed_at`
# là HỢP ĐỒNG BẮT BUỘC với Prometheus exporter — không được đổi tên.
MAINTENANCE_RESULT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "string"), ("logical_run_id", "string"), ("batch_id", "string"),
    ("target", "string"), ("action", "string"), ("status", "string"),
    ("files_before", "bigint"), ("files_after", "bigint"),
    ("bytes_before", "bigint"), ("bytes_after", "bigint"),
    ("snapshots_before", "bigint"), ("snapshots_after", "bigint"),
    ("rewritten_data_files_count", "bigint"), ("added_data_files_count", "bigint"),
    ("expired_snapshots_count", "bigint"), ("deleted_orphan_files_count", "bigint"),
    ("error_summary", "string"),
    ("started_at", "timestamp"), ("completed_at", "timestamp"),
)

# Bộ đếm mặc định khi hành động chưa chạy hoặc đã thất bại giữa chừng.
EMPTY_METRICS: dict[str, int] = {
    "rewritten_data_files_count": 0,
    "added_data_files_count": 0,
    "expired_snapshots_count": 0,
    "deleted_orphan_files_count": 0,
}


# ==============================================================================
# 1. CẤU TRÚC DỮ LIỆU & THẨM ĐỊNH THAM SỐ
# ==============================================================================


@dataclass(frozen=True)
class MaintenanceRequest:
    """Một yêu cầu bảo trì đã được thẩm định đầy đủ (bất biến - immutable).

    Đầu vào : các tham số thô từ dòng lệnh.
    Xử lý   : ``__post_init__`` kiểm tra allowlist và các ngưỡng an toàn tối thiểu.
    Đầu ra  : đối tượng chỉ tồn tại khi mọi ràng buộc an toàn đã thỏa mãn.
    """

    run_id: str
    batch_id: str
    mode: str
    namespace: str
    table: str
    action: str
    retention_hours: int
    orphan_older_than_hours: int

    def __post_init__(self) -> None:
        for name in ("run_id", "batch_id"):
            if SAFE_RUNTIME_VALUE.fullmatch(getattr(self, name)) is None:
                raise ValueError(f"Tham số {name} chứa ký tự không an toàn: {getattr(self, name)!r}")
        if self.mode not in {"full", "incremental"}:
            raise ValueError("Tham số mode phải là 'full' hoặc 'incremental'")
        if self.table not in ALLOWED_TARGETS.get(self.namespace, frozenset()):
            raise ValueError(
                f"Đích bảo trì '{self.namespace}.{self.table}' không nằm trong allowlist"
            )
        if self.action not in ALLOWED_ACTIONS:
            raise ValueError(f"Hành động bảo trì không hợp lệ: {self.action}")
        if self.retention_hours < MIN_RETENTION_HOURS:
            raise ValueError(f"retention_hours phải tối thiểu {MIN_RETENTION_HOURS} giờ")
        if self.orphan_older_than_hours < self.retention_hours:
            raise ValueError("orphan_older_than_hours phải lớn hơn hoặc bằng retention_hours")

    @property
    def target(self) -> str:
        """Tên bảng đích dạng ``namespace.table`` dùng trong tham số của thủ tục Iceberg."""
        return f"{self.namespace}.{self.table}"


@dataclass(frozen=True)
class TableState:
    """Ảnh chụp số liệu metadata của bảng Iceberg tại một thời điểm.

    Dùng để so sánh trước/sau nhằm chứng minh hiệu quả thực tế của việc bảo trì
    (ví dụ: nén từ 240 tệp xuống 4 tệp, thu hồi bao nhiêu byte).
    """

    data_file_count: int
    data_file_bytes: int
    snapshot_count: int


# ==============================================================================
# 2. ĐỌC METADATA & SINH CÂU LỆNH THỦ TỤC ICEBERG
# ==============================================================================


def _qualified(catalog: str, namespace: str, table: str) -> str:
    """Tạo tên bảng đầy đủ đã trích dẫn backtick: ``\\`catalog\\`.\\`ns\\`.\\`tbl\\```."""
    return f"`{catalog}`.`{namespace}`.`{table}`"


def capture_table_state(spark: SparkSession, request: MaintenanceRequest, *, catalog: str) -> TableState:
    """Đọc 3 chỉ số metadata của bảng Iceberg qua các bảng hệ thống ``.files`` và ``.snapshots``.

    Đầu vào : phiên Spark + yêu cầu bảo trì đã thẩm định.
    Xử lý   : 2 câu truy vấn gọn trên metadata (KHÔNG quét dữ liệu thật nên rất nhanh).
    Đầu ra  : ``TableState`` gồm số data file, tổng byte và số snapshot hiện có.
    """
    qualified = _qualified(catalog, request.namespace, request.table)
    files_row = spark.sql(
        f"SELECT count(*) AS file_count, coalesce(sum(file_size_in_bytes), 0) AS total_bytes "
        f"FROM {qualified}.files"
    ).first()
    snapshot_row = spark.sql(f"SELECT count(*) AS snapshot_count FROM {qualified}.snapshots").first()
    return TableState(
        data_file_count=int(files_row["file_count"]) if files_row else 0,
        data_file_bytes=int(files_row["total_bytes"]) if files_row else 0,
        snapshot_count=int(snapshot_row["snapshot_count"]) if snapshot_row else 0,
    )


def _timestamp_literal(value: datetime) -> str:
    """Định dạng mốc thời gian UTC thành chuỗi literal mà Spark SQL hiểu được."""
    return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")


def build_maintenance_sql(request: MaintenanceRequest, *, now: datetime, catalog: str) -> str:
    """Sinh đúng một câu lệnh ``CALL`` thuộc tập đóng 3 thủ tục hệ thống của Iceberg.

    Đầu vào : yêu cầu bảo trì + thời điểm hiện tại + tên catalog.
    Xử lý   : chọn thủ tục tương ứng và tính mốc thời gian cắt (cutoff) từ số giờ lưu giữ.
    Đầu ra  : chuỗi SQL hoàn chỉnh, an toàn (mọi giá trị động đều đã qua allowlist/ép kiểu int).

    Vì sao an toàn trước SQL injection? ``namespace``/``table`` đã qua allowlist cố định,
    còn ``retention_hours``/``orphan_older_than_hours`` đã bị argparse ép sang kiểu ``int``.
    """
    target = request.target
    if request.action == "rewrite-data-files":
        # Gộp các tệp nhỏ phân mảnh thành tệp ~128MB. Đây là thao tác tốn CPU/IO nhất.
        return (
            f"CALL `{catalog}`.system.rewrite_data_files("
            f"table => '{target}', "
            f"options => map('target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}'))"
        )
    if request.action == "expire-snapshots":
        # Cắt tỉa snapshot cũ hơn ngưỡng lưu giữ, nhưng luôn chừa lại 2 snapshot gần nhất.
        cutoff = now - timedelta(hours=request.retention_hours)
        return (
            f"CALL `{catalog}`.system.expire_snapshots("
            f"table => '{target}', "
            f"older_than => TIMESTAMP '{_timestamp_literal(cutoff)}', "
            f"retain_last => {MIN_RETAINED_SNAPSHOTS})"
        )
    if request.action == "remove-orphan-files":
        # Xóa tệp rác trên MinIO S3 không còn được bất kỳ snapshot nào tham chiếu.
        cutoff = now - timedelta(hours=request.orphan_older_than_hours)
        return (
            f"CALL `{catalog}`.system.remove_orphan_files("
            f"table => '{target}', "
            f"older_than => TIMESTAMP '{_timestamp_literal(cutoff)}')"
        )
    raise ValueError(f"Hành động '{request.action}' không sinh câu lệnh thay đổi dữ liệu")


def parse_procedure_metrics(action: str, rows: Sequence[object]) -> dict[str, int]:
    """Bóc các chỉ số đo lường từ kết quả trả về của thủ tục Iceberg.

    Đầu vào : tên hành động + danh sách ``Row`` do ``spark.sql(...).collect()`` trả về.
    Xử lý   : mỗi thủ tục trả về bộ cột khác nhau nên đọc theo tên cột một cách khoan dung
              (cột nào không có thì mặc định 0) để không vỡ job khi nâng cấp phiên bản Iceberg.
    Đầu ra  : dict các bộ đếm chuẩn hóa để ghi vào bảng kiểm toán.
    """
    metrics = dict(EMPTY_METRICS)
    if action == "remove-orphan-files":
        # Thủ tục này trả về MỖI TỆP MỒ CÔI LÀ MỘT DÒNG, nên đếm số dòng chính là số tệp đã xóa.
        metrics["deleted_orphan_files_count"] = len(rows)
        return metrics

    values: Mapping[str, object] = rows[0].asDict() if rows else {}  # type: ignore[attr-defined]

    def _count(name: str) -> int:
        value = values.get(name)
        return int(value) if value is not None else 0

    if action == "rewrite-data-files":
        metrics["rewritten_data_files_count"] = _count("rewritten_data_files_count")
        metrics["added_data_files_count"] = _count("added_data_files_count")
    elif action == "expire-snapshots":
        # Iceberg báo số tệp dữ liệu bị xóa kèm theo snapshot; số snapshot hết hạn
        # được tính chính xác hơn từ chênh lệch trước/sau trong `run_maintenance_job`.
        metrics["expired_snapshots_count"] = _count("deleted_data_files_count")
    return metrics


# ==============================================================================
# 3. BẢNG KIỂM TOÁN KẾT QUẢ BẢO TRÌ
# ==============================================================================


def ensure_maintenance_results(spark: SparkSession, *, namespace: str = "control") -> str:
    """Tạo bảng ``control.maintenance_results`` nếu chưa có và trả về tên đầy đủ của nó.

    Đầu vào : phiên Spark + namespace kiểm toán.
    Xử lý   : CREATE TABLE IF NOT EXISTS theo ``MAINTENANCE_RESULT_COLUMNS``. Nếu phát hiện
              bảng cũ còn sót lược đồ của bản over-engineered (có cột ``approval_nonce``
              NOT NULL mà bản mới không còn ghi), bảng cũ được ĐỔI TÊN sang
              ``maintenance_results_legacy`` để giữ lịch sử thay vì xóa mất.
    Đầu ra  : tên bảng đầy đủ đã trích dẫn, sẵn sàng để INSERT.
    """
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    qualified = f"`{catalog}`.`{namespace}`.`maintenance_results`"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog}`.`{namespace}`")

    if spark.catalog.tableExists(f"{catalog}.{namespace}.maintenance_results"):
        existing = {field.name for field in spark.table(qualified).schema.fields}
        expected = {name for name, _ in MAINTENANCE_RESULT_COLUMNS}
        if not expected <= existing:
            legacy = f"`{catalog}`.`{namespace}`.`maintenance_results_legacy`"
            log_event(LOGGER, "maintenance_legacy_table_renamed", table=qualified, renamed_to=legacy)
            spark.sql(f"DROP TABLE IF EXISTS {legacy}")
            spark.sql(f"ALTER TABLE {qualified} RENAME TO {legacy}")

    columns = ", ".join(f"`{name}` {sql_type}" for name, sql_type in MAINTENANCE_RESULT_COLUMNS)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {qualified} ({columns}) USING iceberg "
        "TBLPROPERTIES ('format-version'='2', "
        "'grain'='one row per maintenance run and action', "
        "'write.distribution-mode'='hash', "
        f"'write.target-file-size-bytes'='{TARGET_FILE_SIZE_BYTES}')"
    )
    return qualified


def record_maintenance_result(spark: SparkSession, *, table: str, row: dict[str, object]) -> None:
    """Ghi một dòng kết quả bảo trì vào bảng kiểm toán.

    Đầu vào : tên bảng kiểm toán + dict giá trị khớp với ``MAINTENANCE_RESULT_COLUMNS``.
    Xử lý   : dựng DataFrame một dòng đúng thứ tự cột rồi ``append`` (Iceberg commit nguyên tử).
    Đầu ra  : không trả về giá trị; ngoại lệ sẽ được đẩy lên để Airflow đánh dấu task FAILED.
    """
    ordered = [row.get(name) for name, _ in MAINTENANCE_RESULT_COLUMNS]
    schema = ", ".join(f"`{name}` {sql_type}" for name, sql_type in MAINTENANCE_RESULT_COLUMNS)
    spark.createDataFrame([tuple(ordered)], schema).writeTo(table).append()


# ==============================================================================
# 4. LUỒNG THỰC THI CHÍNH
# ==============================================================================


def run_maintenance_job(
    spark: SparkSession,
    request: MaintenanceRequest,
    *,
    attempt_id: str,
    dry_run_report: Path | None,
) -> None:
    """Chạy trọn vẹn một hành động bảo trì kèm ghi nhận kiểm toán đầy đủ.

    Đầu vào : phiên Spark, yêu cầu đã thẩm định, mã lần thử và đường dẫn báo cáo dry-run.
    Xử lý   : đo trước → thực thi (hoặc chỉ khảo sát nếu là dry-run) → đo sau → ghi kiểm toán.
    Đầu ra  : không trả về giá trị. Thành công thì 2 bảng kiểm toán có trạng thái SUCCESS;
              thất bại thì ghi FAILED kèm tóm tắt lỗi rồi ném lại ngoại lệ cho Airflow.
    """
    catalog = str(spark.conf.get("spark.sql.defaultCatalog"))
    audit_run_id = attempt_run_id(request.run_id, attempt_id)
    started_at = datetime.now(timezone.utc)

    # Chuẩn bị 2 bảng kiểm toán trước khi làm bất cứ việc gì có thể thất bại.
    ensure_audit_tables(spark, namespace="control")
    results_table = ensure_maintenance_results(spark, namespace="control")
    start_pipeline_run(
        spark,
        run_id=audit_run_id,
        batch_id=request.batch_id,
        job_name="iceberg_maintenance",
        layer="maintenance",
        mode=request.mode,
        started_at=started_at,
        logical_run_id=request.run_id,
        attempt_id=attempt_id,
    )

    before = capture_table_state(spark, request, catalog=catalog)
    log_event(
        LOGGER,
        "maintenance_started",
        run_id=audit_run_id,
        target=request.target,
        action=request.action,
        files_before=before.data_file_count,
        bytes_before=before.data_file_bytes,
        snapshots_before=before.snapshot_count,
    )

    status, error_summary = "SUCCESS", None
    metrics, after = dict(EMPTY_METRICS), before
    try:
        if request.action == "dry-run":
            # Khảo sát không phá hủy: chỉ ghi lại ảnh chụp metadata ra file JSON làm bằng chứng
            # cho người vận hành xem trước khi chấp nhận chạy các bước thay đổi dữ liệu phía sau.
            _write_dry_run_report(request, before, path=dry_run_report, run_id=audit_run_id)
        else:
            sql = build_maintenance_sql(request, now=started_at, catalog=catalog)
            log_event(LOGGER, "maintenance_sql", run_id=audit_run_id, sql=sql)
            rows = spark.sql(sql).collect()
            after = capture_table_state(spark, request, catalog=catalog)
            metrics = parse_procedure_metrics(request.action, rows)
            if request.action == "expire-snapshots":
                # Số snapshot thực sự bị cắt tỉa = chênh lệch đếm được trước/sau (chính xác tuyệt đối).
                metrics["expired_snapshots_count"] = max(before.snapshot_count - after.snapshot_count, 0)
    except Exception as error:  # noqa: BLE001 - bắt mọi lỗi để kịp chốt kiểm toán FAILED rồi ném lại
        status = "FAILED"
        error_summary = truncate_error_summary(f"{type(error).__name__}: {error}")
        log_event(LOGGER, "maintenance_failed", run_id=audit_run_id, error=error_summary)
        raise
    finally:
        # Khối `finally` đảm bảo kiểm toán LUÔN được chốt sổ dù thành công hay thất bại,
        # nhờ vậy chỉ cần viết lời gọi `_finalize` đúng một lần duy nhất.
        _finalize(
            spark,
            results_table=results_table,
            request=request,
            audit_run_id=audit_run_id,
            before=before,
            after=after,
            metrics=metrics,
            status=status,
            error_summary=error_summary,
            started_at=started_at,
        )

    log_event(
        LOGGER,
        "maintenance_completed",
        run_id=audit_run_id,
        target=request.target,
        action=request.action,
        files_before=before.data_file_count,
        files_after=after.data_file_count,
        bytes_reclaimed=before.data_file_bytes - after.data_file_bytes,
        **metrics,
    )


def _finalize(
    spark: SparkSession,
    *,
    results_table: str,
    request: MaintenanceRequest,
    audit_run_id: str,
    before: TableState,
    after: TableState,
    metrics: Mapping[str, int],
    status: str,
    error_summary: str | None,
    started_at: datetime,
) -> None:
    """Chốt sổ kiểm toán: ghi bản ghi kết quả bảo trì và đóng bản ghi vòng đời pipeline."""
    completed_at = datetime.now(timezone.utc)
    record_maintenance_result(
        spark,
        table=results_table,
        row={
            "run_id": audit_run_id,
            "logical_run_id": request.run_id,
            "batch_id": request.batch_id,
            "target": request.target,
            "action": request.action,
            "status": status,
            "files_before": before.data_file_count,
            "files_after": after.data_file_count,
            "bytes_before": before.data_file_bytes,
            "bytes_after": after.data_file_bytes,
            "snapshots_before": before.snapshot_count,
            "snapshots_after": after.snapshot_count,
            "error_summary": error_summary,
            "started_at": started_at.replace(tzinfo=None),
            "completed_at": completed_at.replace(tzinfo=None),
            **metrics,
        },
    )
    finish_pipeline_run(
        spark,
        run_id=audit_run_id,
        input_count=before.data_file_count,
        output_count=after.data_file_count,
        rejected_count=0,
        status=status,
        completed_at=completed_at,
        error_summary=error_summary,
    )


def _write_dry_run_report(
    request: MaintenanceRequest,
    state: TableState,
    *,
    path: Path | None,
    run_id: str,
) -> None:
    """Ghi báo cáo khảo sát dry-run dạng JSON thuần (KHÔNG ký HMAC như bản cũ).

    Vì sao bỏ chữ ký HMAC? Báo cáo này chỉ là bằng chứng vận hành để con người đọc;
    nó không còn đóng vai trò "giấy phép" cho các task sau nữa, nên không cần chống giả mạo.
    Chính cơ chế truyền token giữa các task qua Jinja template là nguồn gốc lỗi treo DAG.
    """
    if path is None:
        return
    report = {
        "run_id": run_id,
        "target": request.target,
        "mode": request.mode,
        "retention_hours": request.retention_hours,
        "orphan_older_than_hours": request.orphan_older_than_hours,
        "surveyed_at": datetime.now(timezone.utc).isoformat(),
        "table_state": asdict(state),
        "planned_actions": sorted(MUTATING_ACTIONS),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    log_event(LOGGER, "maintenance_dry_run_report_written", run_id=run_id, path=str(path))


# ==============================================================================
# 5. GIAO DIỆN DÒNG LỆNH (CLI)
# ==============================================================================


def _runtime_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Nạp cấu hình kết nối, chấp nhận cả biến AWS_* mà spark-submit đưa vào container."""
    values = dict(os.environ if env is None else env)
    values.setdefault("MINIO_ROOT_USER", values.get("AWS_ACCESS_KEY_ID", ""))
    values.setdefault("MINIO_ROOT_PASSWORD", values.get("AWS_SECRET_ACCESS_KEY", ""))
    return Settings.from_env(values)


def _parser() -> argparse.ArgumentParser:
    """Khai báo các đối số dòng lệnh, có sẵn giá trị mặc định để chạy thử nhanh từ CLI."""
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    parser = argparse.ArgumentParser(
        description="Bảo trì và tối ưu hóa một bảng Apache Iceberg trong allowlist."
    )
    parser.add_argument("--run-id", default=f"maint-{now}", help="Mã định danh phiên chạy bảo trì.")
    parser.add_argument("--attempt-id", default="1", help="Số thứ tự lần thử của task Airflow.")
    parser.add_argument("--batch-id", default=f"batch-{now}", help="Mã lô dữ liệu liên quan.")
    parser.add_argument(
        "--mode", choices=("full", "incremental"), default="full", help="Chế độ của phiên chạy."
    )
    parser.add_argument(
        "--namespace",
        choices=tuple(ALLOWED_TARGETS),
        default="gold",
        help="Namespace chứa bảng Iceberg cần bảo trì.",
    )
    parser.add_argument("--table", default="fact_orders", help="Tên bảng đích trong allowlist.")
    parser.add_argument(
        "--action",
        choices=tuple(sorted(ALLOWED_ACTIONS)),
        default="dry-run",
        help="Hành động bảo trì cần thực hiện.",
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=MIN_RETENTION_HOURS,
        help=f"Số giờ giữ snapshot tối thiểu (>= {MIN_RETENTION_HOURS}).",
    )
    parser.add_argument(
        "--orphan-older-than-hours",
        type=int,
        default=336,
        help="Tuổi tối thiểu (giờ) của tệp mồ côi trước khi được phép xóa.",
    )
    parser.add_argument(
        "--dry-run-report",
        type=Path,
        default=None,
        help="Đường dẫn ghi báo cáo JSON của hành động dry-run.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Điểm khởi nhập CLI: thẩm định tham số, dựng SparkSession, chạy bảo trì rồi dọn dẹp."""
    args = _parser().parse_args(argv)
    request = MaintenanceRequest(
        run_id=args.run_id,
        batch_id=args.batch_id,
        mode=args.mode,
        namespace=args.namespace,
        table=args.table,
        action=args.action,
        retention_hours=args.retention_hours,
        orphan_older_than_hours=args.orphan_older_than_hours,
    )
    configure_logging()
    settings = _runtime_settings()
    spark = build_spark_session(settings, f"olist-maintenance-{request.action}-{request.run_id}")
    try:
        run_maintenance_job(
            spark,
            request,
            attempt_id=args.attempt_id,
            dry_run_report=args.dry_run_report,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
