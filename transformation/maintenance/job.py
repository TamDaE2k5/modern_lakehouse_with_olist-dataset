"""Run one allowlisted Iceberg maintenance action after a verified dry run."""

from __future__ import annotations

import argparse
import base64
import binascii
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, fields, replace
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from transformation.common.audit import (
    PipelineResolutionEvidence,
    PipelineRunRecord,
    attempt_run_id,
    ensure_audit_tables,
    evolve_table_schema,
    finish_pipeline_run,
    pipeline_run_outcome,
    pipeline_run_record,
    resolve_pipeline_run,
    start_pipeline_run,
    truncate_error_summary,
)
from transformation.common.config import Settings
from transformation.common.iceberg import advisory_ownership_lock
from transformation.common.logging import configure_logging
from transformation.common.spark_session import build_spark_session


MIN_RETENTION_HOURS = 168
MAX_REPORT_AGE = timedelta(hours=1)
TARGET_FILE_SIZE_BYTES = 134_217_728
MIN_APPROVAL_SECRET_BYTES = 32
_SAFE_RUNTIME_VALUE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,119}$")
_SAFE_NONCE = re.compile(r"^[a-f0-9]{32,64}$")
_SAFE_EXECUTION_LEASE = re.compile(r"^[a-f0-9]{64}$")
_FINALIZATION_ATTEMPTS = 3
EXECUTION_LEASE_STALE_AFTER = timedelta(hours=1)
MAINTENANCE_RESULT_GRAIN = "approval_run_id + consumer_run_id + action"

ALLOWED_TARGETS: dict[str, frozenset[str]] = {
    "bronze": frozenset(
        {
            "customers",
            "orders",
            "order_items",
            "order_payments",
            "order_reviews",
            "products",
            "sellers",
            "geolocation",
            "product_category_translation",
        }
    ),
    "silver": frozenset(
        {
            "customers",
            "orders",
            "order_items",
            "order_payments",
            "order_reviews",
            "products",
            "sellers",
            "geography",
        }
    ),
    "gold": frozenset(
        {
            "dim_date",
            "dim_customer",
            "dim_product",
            "dim_seller",
            "dim_geography",
            "fact_orders",
            "fact_order_items",
            "fact_payments",
            "fact_reviews",
            "bridge_review_products",
            "bridge_review_sellers",
            "mart_daily_revenue",
            "mart_seller_performance",
            "mart_product_performance",
        }
    ),
}
MUTATING_ACTIONS = frozenset({"rewrite-data-files", "expire-snapshots", "remove-orphan-files"})
ALLOWED_ACTIONS = MUTATING_ACTIONS | {"dry-run"}
_ACTION_RUN_SUFFIX = {
    "rewrite-data-files": "maintenance-rewrite",
    "expire-snapshots": "maintenance-expire",
    "remove-orphan-files": "maintenance-orphans",
}


@dataclass(frozen=True)
class MaintenanceRequest:
    """Validated, explicitly bounded maintenance request."""

    run_id: str
    approval_run_id: str
    batch_id: str
    mode: str
    namespace: str
    table: str
    action: str
    retention_hours: int
    orphan_older_than_hours: int
    confirm_orphan_cleanup: bool = False

    def __post_init__(self) -> None:
        for name in ("run_id", "approval_run_id", "batch_id"):
            if _SAFE_RUNTIME_VALUE.fullmatch(getattr(self, name)) is None:
                raise ValueError(f"{name} chứa ký tự không an toàn")
        if self.mode not in {"full", "incremental"}:
            raise ValueError("mode phải là full hoặc incremental")
        if self.namespace not in ALLOWED_TARGETS or self.table not in ALLOWED_TARGETS.get(
            self.namespace, frozenset()
        ):
            raise ValueError("Đích bảo trì không nằm trong allowlist")
        if self.action not in ALLOWED_ACTIONS:
            raise ValueError("Hành động bảo trì không nằm trong allowlist")
        if self.retention_hours < MIN_RETENTION_HOURS:
            raise ValueError(f"retention_hours phải tối thiểu là {MIN_RETENTION_HOURS}")
        if self.orphan_older_than_hours < self.retention_hours:
            raise ValueError("orphan_older_than_hours phải tối thiểu bằng retention_hours")

    @property
    def target(self) -> str:
        return f"{self.namespace}.{self.table}"


@dataclass(frozen=True)
class ProcedureMetrics:
    """Named, durable metrics emitted by supported Iceberg procedures."""

    rewritten_data_files_count: int = 0
    added_data_files_count: int = 0
    rewritten_bytes_count: int = 0
    failed_data_files_count: int = 0
    expired_snapshots_count: int = 0
    deleted_data_files_count: int = 0
    deleted_position_delete_files_count: int = 0
    deleted_equality_delete_files_count: int = 0
    deleted_manifest_files_count: int = 0
    deleted_manifest_lists_count: int = 0
    deleted_orphan_files_count: int = 0

    def primary_count(self, action: str) -> int:
        if action == "rewrite-data-files":
            return self.rewritten_data_files_count
        if action == "expire-snapshots":
            return self.expired_snapshots_count
        if action == "remove-orphan-files":
            return self.deleted_orphan_files_count
        return 0


@dataclass(frozen=True)
class TableState:
    """Bounded metadata needed to reconcile an ambiguous procedure response."""

    snapshot_ids: tuple[int, ...]
    current_snapshot_id: int | None
    current_snapshot_summary: Mapping[str, str]
    data_file_count: int
    data_file_bytes: int


@dataclass(frozen=True)
class ProcedureExecution:
    status: str
    metrics: ProcedureMetrics
    before: TableState
    after: TableState
    reconciled_after_error: bool = False


class MaintenanceOutcomeUnknown(RuntimeError):
    """A mutation may have committed, so automatic execution must not repeat it."""

    def __init__(
        self,
        action: str,
        before: TableState | None = None,
        after: TableState | None = None,
    ) -> None:
        super().__init__(
            f"Hành động bảo trì {action} có kết quả commit chưa xác định"
        )
        self.action = action
        self.before = before
        self.after = after


@dataclass(frozen=True)
class MaintenanceResultRecord:
    approval_nonce: str
    approval_run_id: str
    consumer_run_id: str
    batch_id: str
    target: str
    action: str
    status: str
    pre_state: TableState | None
    metrics: ProcedureMetrics
    error_summary: str | None
    original_status: str | None = None
    original_error_summary: str | None = None
    resolution_note: str | None = None
    resolution_evidence_json: str | None = None
    resolved_at: datetime | None = None
    execution_lease: str | None = None
    execution_lease_acquired_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class MaintenanceDuplicateError(RuntimeError):
    """Raised when one ownership identity maps to ambiguous persisted ledger rows."""


@dataclass(frozen=True)
class MaintenanceIdentitySummary:
    row_count: int
    distinct_payload_count: int
    min_payload_hash: str | None
    max_payload_hash: str | None
    representative: MaintenanceResultRecord | None


_METRIC_FIELDS = tuple(field.name for field in fields(ProcedureMetrics))
MAINTENANCE_RESULT_SCHEMA = StructType(
    [
        StructField("approval_nonce", StringType(), False),
        StructField("approval_run_id", StringType(), False),
        StructField("consumer_run_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("target", StringType(), False),
        StructField("action", StringType(), False),
        StructField("status", StringType(), False),
        StructField("pre_state_json", StringType(), True),
        *(StructField(name, LongType(), False) for name in _METRIC_FIELDS),
        StructField("error_summary", StringType(), True),
        StructField("original_status", StringType(), True),
        StructField("original_error_summary", StringType(), True),
        StructField("resolution_note", StringType(), True),
        StructField("resolution_evidence_json", StringType(), True),
        StructField("resolved_at", TimestampType(), True),
        StructField("execution_lease", StringType(), True),
        StructField("execution_lease_acquired_at", TimestampType(), True),
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
    ]
)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Mốc thời gian bảo trì phải có thông tin múi giờ")
    return value.astimezone(timezone.utc)


def load_approval_secret(env: Mapping[str, str] | None = None) -> bytes:
    """Nạp secret HMAC 256-bit chuẩn (chuỗi base64 độ dài chính xác 32 byte) dùng để ký và thẩm định báo cáo bảo trì.

    Cơ chế thẩm định phân quyền:
    - Nhằm ngăn chặn các tác vụ phá hủy dữ liệu ngoài ý muốn hoặc chạy trực tiếp không qua phê duyệt,
      mọi thao tác bảo trì (compaction, expire snapshots, remove orphan files) đều yêu cầu một token HMAC.
    - Token này được sinh ra từ bước `dry-run` và được thẩm định bằng một secret 256-bit dùng chung.

    Chiến lược tìm nạp secret:
    1. Ưu tiên biến môi trường MAINTENANCE_APPROVAL_SECRET (chuỗi base64).
    2. Đọc tệp cấu hình theo đường dẫn MAINTENANCE_APPROVAL_SECRET_FILE.
    3. Tự động dò tìm fallback tại các vị trí mặc định an toàn:
       - /run/secrets/maintenance_approval_secret (Docker Compose Secrets)
       - /opt/lakehouse/secrets/maintenance_approval_secret (Lakehouse workspace mount)
       - secrets/maintenance_approval_secret (Đường dẫn tương đối cục bộ)
    """
    values = os.environ if env is None else env
    inline = values.get("MAINTENANCE_APPROVAL_SECRET", "")
    source = values.get("MAINTENANCE_APPROVAL_SECRET_FILE", "")
    if inline and source:
        raise ValueError("Chỉ được cấu hình một nguồn secret phê duyệt bảo trì")

    # Dò tìm fallback tự động nếu chạy cục bộ hoặc môi trường không truyền rõ
    if not inline and not source:
        search_paths = [
            Path("/run/secrets/maintenance_approval_secret"),
            Path("/opt/lakehouse/secrets/maintenance_approval_secret"),
            Path("secrets/maintenance_approval_secret"),
            Path(__file__).resolve().parents[2] / "secrets" / "maintenance_approval_secret",
        ]
        for candidate in search_paths:
            if candidate.exists():
                source = str(candidate)
                break

    if source:
        try:
            encoded = Path(source).read_text(encoding="ascii").strip()
        except (OSError, UnicodeError) as exc:
            raise ValueError(f"Không thể đọc tệp secret phê duyệt bảo trì tại {source}") from exc
    else:
        encoded = inline
    error = "Secret phê duyệt bảo trì phải là chuỗi base64 chuẩn mã hóa chính xác 32 byte"
    try:
        secret = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(error) from exc
    if len(secret) != MIN_APPROVAL_SECRET_BYTES:
        raise ValueError(error)
    if base64.b64encode(secret).decode("ascii") != encoded:
        raise ValueError(error)
    return secret


def _canonical_report(report: Mapping[str, object]) -> bytes:
    content = {key: value for key, value in report.items() if key != "approval_token"}
    return json.dumps(content, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _approval_token(report: Mapping[str, object], secret: bytes) -> str:
    return hmac.new(secret, _canonical_report(report), hashlib.sha256).hexdigest()


def _row_values(row: object) -> dict[str, Any]:
    try:
        return dict(row.asDict(recursive=True))  # type: ignore[attr-defined]
    except TypeError:
        return dict(row.asDict())  # type: ignore[attr-defined]
    except AttributeError:
        return dict(vars(row))


def _single_metrics_row(query: object) -> dict[str, Any]:
    rows = query.collect()  # type: ignore[attr-defined]
    if len(rows) != 1:
        raise RuntimeError("Truy vấn metadata bảo trì trả về số hàng không như dự kiến")
    return _row_values(rows[0])


def _consumer_run_ids(approval_run_id: str) -> dict[str, str]:
    return {
        action: f"{approval_run_id}-{suffix}"
        for action, suffix in sorted(_ACTION_RUN_SUFFIX.items())
    }


def create_dry_run_report(
    spark: SparkSession,
    request: MaintenanceRequest,
    destination: Path,
    *,
    now: datetime,
    catalog: str,
    secret: bytes,
    nonce: str | None = None,
) -> dict[str, object]:
    """Inspect one table and atomically persist a signed, expiring action plan."""

    if request.action != "dry-run":
        raise ValueError("Báo cáo dry run yêu cầu hành động dry-run")
    if len(secret) != MIN_APPROVAL_SECRET_BYTES:
        raise ValueError("Secret phê duyệt bảo trì phải có chính xác 32 byte")
    current = _utc(now)
    if destination.exists():
        try:
            existing = json.loads(destination.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("không thể đọc báo cáo dry run hiện có") from exc
        expected_token = _approval_token(existing, secret)
        identity = {
            "approval_run_id": request.approval_run_id,
            "dry_run_id": request.run_id,
            "batch_id": request.batch_id,
            "targets": [request.target],
        }
        if not hmac.compare_digest(str(existing.get("approval_token", "")), expected_token):
            raise ValueError("Token phê duyệt của báo cáo dry run hiện có không hợp lệ")
        mismatches = [key for key, value in identity.items() if existing.get(key) != value]
        if mismatches:
            raise RuntimeError(
                "Danh tính phê duyệt dry-run không khớp: " + ", ".join(mismatches)
            )
        return existing
    approval_nonce = secrets.token_hex(16) if nonce is None else nonce
    if _SAFE_NONCE.fullmatch(approval_nonce) is None:
        raise ValueError("Nonce phê duyệt bảo trì không hợp lệ")
    qualified = f"`{catalog}`.`{request.namespace}`.`{request.table}`"
    file_metrics = _single_metrics_row(
        spark.sql(
            "SELECT count(*) AS file_count, "
            f"coalesce(sum(file_size_in_bytes), 0) AS total_bytes FROM {qualified}.files"
        )
    )
    snapshot_metrics = _single_metrics_row(
        spark.sql(f"SELECT count(*) AS snapshot_count FROM {qualified}.snapshots")
    )
    report: dict[str, object] = {
        "version": 2,
        "nonce": approval_nonce,
        "approval_run_id": request.approval_run_id,
        "consumer_run_ids": _consumer_run_ids(request.approval_run_id),
        "dry_run_id": request.run_id,
        "batch_id": request.batch_id,
        "targets": [request.target],
        "allowed_actions": sorted(MUTATING_ACTIONS),
        "created_at": current.isoformat(),
        "expires_at": (current + MAX_REPORT_AGE).isoformat(),
        "retention_hours": request.retention_hours,
        "orphan_older_than_hours": request.orphan_older_than_hours,
        "metrics": {
            "data_file_count": int(file_metrics["file_count"]),
            "data_file_bytes": int(file_metrics["total_bytes"]),
            "snapshot_count": int(snapshot_metrics["snapshot_count"]),
        },
    }
    report["approval_token"] = _approval_token(report, secret)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode("utf-8")
    try:
        descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return create_dry_run_report(
            spark,
            request,
            destination,
            now=now,
            catalog=catalog,
            secret=secret,
            nonce=nonce,
        )
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    return report


def validate_dry_run_report(
    source: Path,
    request: MaintenanceRequest,
    *,
    now: datetime,
    secret: bytes,
) -> dict[str, object]:
    """Verify HMAC, identity, scope, action, and expiry before mutation."""

    if len(secret) != MIN_APPROVAL_SECRET_BYTES:
        raise ValueError("Secret phê duyệt bảo trì phải có chính xác 32 byte")
    try:
        report = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("cần báo cáo dry run có thể đọc được") from exc
    if not isinstance(report, dict):
        raise ValueError("Báo cáo dry run phải là đối tượng JSON")
    token = report.get("approval_token")
    expected_token = _approval_token(report, secret)
    if not isinstance(token, str) or not hmac.compare_digest(token, expected_token):
        raise ValueError("Token phê duyệt của báo cáo dry run không hợp lệ")
    if report.get("version") != 2:
        raise ValueError("Phiên bản của báo cáo dry run không được hỗ trợ")
    nonce = report.get("nonce")
    if not isinstance(nonce, str) or _SAFE_NONCE.fullmatch(nonce) is None:
        raise ValueError("Nonce của báo cáo dry run không hợp lệ")
    if report.get("approval_run_id") != request.approval_run_id:
        raise ValueError("Run phê duyệt của báo cáo dry run không khớp yêu cầu")
    actions = report.get("allowed_actions")
    if (
        request.action not in MUTATING_ACTIONS
        or not isinstance(actions, list)
        or request.action not in actions
    ):
        raise ValueError("Hành động trong báo cáo dry run chưa được phê duyệt")
    consumers = report.get("consumer_run_ids")
    if not isinstance(consumers, dict) or consumers.get(request.action) != request.run_id:
        raise ValueError("Run tiêu thụ của báo cáo dry run không khớp yêu cầu")
    if report.get("batch_id") != request.batch_id:
        raise ValueError("Batch trong báo cáo dry run không khớp yêu cầu")
    if report.get("targets") != [request.target]:
        raise ValueError("Đích trong báo cáo dry run không khớp yêu cầu")
    if (
        report.get("retention_hours") != request.retention_hours
        or report.get("orphan_older_than_hours") != request.orphan_older_than_hours
    ):
        raise ValueError("Thiết lập lưu giữ trong báo cáo dry run không khớp yêu cầu")
    try:
        created_at = _utc(datetime.fromisoformat(str(report["created_at"])))
        expires_at = _utc(datetime.fromisoformat(str(report["expires_at"])))
    except (KeyError, ValueError) as exc:
        raise ValueError("Mốc thời gian trong báo cáo dry run không hợp lệ") from exc
    current = _utc(now)
    if expires_at - created_at != MAX_REPORT_AGE:
        raise ValueError("Khoảng hiệu lực của báo cáo dry run không hợp lệ")
    if current < created_at - timedelta(minutes=5):
        raise ValueError("báo cáo dry run có thời điểm tạo trong tương lai")
    if current > expires_at:
        raise ValueError("báo cáo dry run đã hết hạn")
    return report


def _timestamp(value: datetime) -> str:
    return _utc(value).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")


def build_maintenance_sql(
    request: MaintenanceRequest,
    *,
    now: datetime,
    catalog: str,
) -> str:
    """Build one statement from a closed set of Iceberg system procedures."""

    target = request.target
    if request.action == "rewrite-data-files":
        return (
            f"CALL `{catalog}`.system.rewrite_data_files(table => '{target}', "
            f"options => map('target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}'))"
        )
    if request.action == "expire-snapshots":
        cutoff = _utc(now) - timedelta(hours=request.retention_hours)
        return (
            f"CALL `{catalog}`.system.expire_snapshots(table => '{target}', "
            f"older_than => TIMESTAMP '{_timestamp(cutoff)}', retain_last => 2)"
        )
    if request.action == "remove-orphan-files":
        cutoff = _utc(now) - timedelta(hours=request.orphan_older_than_hours)
        return (
            f"CALL `{catalog}`.system.remove_orphan_files(table => '{target}', "
            f"older_than => TIMESTAMP '{_timestamp(cutoff)}')"
        )
    raise ValueError("Hành động dry-run không tạo câu lệnh SQL thay đổi dữ liệu")


def capture_table_state(
    spark: SparkSession,
    request: MaintenanceRequest,
    *,
    catalog: str,
) -> TableState:
    """Read bounded file and snapshot metadata for ambiguity reconciliation."""

    qualified = f"`{catalog}`.`{request.namespace}`.`{request.table}`"
    files = _single_metrics_row(
        spark.sql(
            "SELECT count(*) AS file_count, "
            f"coalesce(sum(file_size_in_bytes), 0) AS total_bytes FROM {qualified}.files"
        )
    )
    snapshot_rows = spark.sql(
        f"SELECT snapshot_id, committed_at, summary FROM {qualified}.snapshots "
        "ORDER BY committed_at, snapshot_id"
    ).collect()
    snapshots = [_row_values(row) for row in snapshot_rows]
    current = snapshots[-1] if snapshots else None
    summary = {} if current is None else current.get("summary") or {}
    return TableState(
        snapshot_ids=tuple(int(row["snapshot_id"]) for row in snapshots),
        current_snapshot_id=None if current is None else int(current["snapshot_id"]),
        current_snapshot_summary={str(key): str(value) for key, value in dict(summary).items()},
        data_file_count=int(files["file_count"]),
        data_file_bytes=int(files["total_bytes"]),
    )


def _int(values: Mapping[str, Any], name: str) -> int:
    if name not in values or values[name] is None:
        raise RuntimeError("Thủ tục Iceberg trả về các cột kết quả không như dự kiến")
    return int(values[name])


def parse_procedure_metrics(
    action: str,
    rows: Sequence[object],
    *,
    before: TableState,
    after: TableState,
) -> ProcedureMetrics:
    """Parse documented named procedure columns into durable counters."""

    if action == "rewrite-data-files":
        if len(rows) != 1:
            raise RuntimeError("Thủ tục rewrite của Iceberg trả về số hàng không như dự kiến")
        values = _row_values(rows[0])
        return ProcedureMetrics(
            rewritten_data_files_count=_int(values, "rewritten_data_files_count"),
            added_data_files_count=_int(values, "added_data_files_count"),
            rewritten_bytes_count=_int(values, "rewritten_bytes_count"),
            failed_data_files_count=_int(values, "failed_data_files_count"),
        )
    if action == "expire-snapshots":
        if len(rows) != 1:
            raise RuntimeError(
                "Thủ tục hết hạn snapshot của Iceberg trả về số hàng không như dự kiến"
            )
        values = _row_values(rows[0])
        return ProcedureMetrics(
            expired_snapshots_count=len(set(before.snapshot_ids) - set(after.snapshot_ids)),
            deleted_data_files_count=_int(values, "deleted_data_files_count"),
            deleted_position_delete_files_count=_int(values, "deleted_position_delete_files_count"),
            deleted_equality_delete_files_count=_int(values, "deleted_equality_delete_files_count"),
            deleted_manifest_files_count=_int(values, "deleted_manifest_files_count"),
            deleted_manifest_lists_count=_int(values, "deleted_manifest_lists_count"),
        )
    if action == "remove-orphan-files":
        locations: set[str] = set()
        for row in rows:
            values = _row_values(row)
            location = values.get("orphan_file_location")
            if not isinstance(location, str) or not location:
                raise RuntimeError(
                    "Thủ tục Iceberg trả về các cột kết quả không như dự kiến"
                )
            locations.add(location)
        return ProcedureMetrics(deleted_orphan_files_count=len(locations))
    raise ValueError("Hành động bảo trì thay đổi dữ liệu không được hỗ trợ")


def _reconcile_changed_state(
    action: str,
    before: TableState,
    after: TableState,
) -> ProcedureMetrics | None:
    if action == "rewrite-data-files" and (
        after.current_snapshot_id is not None
        and after.current_snapshot_id != before.current_snapshot_id
    ):
        summary = after.current_snapshot_summary
        required = {"deleted-data-files", "added-data-files", "removed-files-size"}
        if required <= set(summary):
            return ProcedureMetrics(
                rewritten_data_files_count=int(summary["deleted-data-files"]),
                added_data_files_count=int(summary["added-data-files"]),
                rewritten_bytes_count=int(summary["removed-files-size"]),
            )
    if action == "expire-snapshots":
        removed = set(before.snapshot_ids) - set(after.snapshot_ids)
        if removed:
            return ProcedureMetrics(expired_snapshots_count=len(removed))
    return None


def _capture_state_with_retries(
    spark: SparkSession,
    request: MaintenanceRequest,
    *,
    catalog: str,
) -> TableState:
    last_error: BaseException | None = None
    for _ in range(_FINALIZATION_ATTEMPTS):
        try:
            return capture_table_state(spark, request, catalog=catalog)
        except BaseException as exc:
            last_error = exc
    error_name = "none" if last_error is None else type(last_error).__name__
    raise RuntimeError(f"Đã hết số lần đọc lại metadata bảo trì; error={error_name}")


def execute_maintenance_procedure(
    spark: SparkSession,
    request: MaintenanceRequest,
    *,
    catalog: str,
    now: datetime,
    before: TableState,
) -> ProcedureExecution:
    """Execute once; reconcile response ambiguity without repeating the mutation."""

    sql = build_maintenance_sql(request, now=now, catalog=catalog)
    try:
        rows = spark.sql(sql).collect()
    except BaseException as exc:
        try:
            after = _capture_state_with_retries(spark, request, catalog=catalog)
        except BaseException:
            raise MaintenanceOutcomeUnknown(request.action, before, None) from exc
        metrics = _reconcile_changed_state(request.action, before, after)
        if metrics is None:
            raise MaintenanceOutcomeUnknown(request.action, before, after) from exc
        return ProcedureExecution(
            status="SUCCESS",
            metrics=metrics,
            before=before,
            after=after,
            reconciled_after_error=True,
        )
    try:
        after = _capture_state_with_retries(spark, request, catalog=catalog)
        metrics = parse_procedure_metrics(request.action, rows, before=before, after=after)
    except BaseException as exc:
        raise MaintenanceOutcomeUnknown(request.action, before, locals().get("after")) from exc
    return ProcedureExecution(status="SUCCESS", metrics=metrics, before=before, after=after)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _utc_naive(value: datetime) -> datetime:
    return _utc(value).replace(tzinfo=None)


def _maintenance_table(spark: SparkSession, namespace: str) -> str:
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    return f"`{catalog}`.`{namespace}`.`maintenance_results`"


def _state_json(state: TableState) -> str:
    return json.dumps(
        {
            "snapshot_ids": list(state.snapshot_ids),
            "current_snapshot_id": state.current_snapshot_id,
            "current_snapshot_summary": dict(state.current_snapshot_summary),
            "data_file_count": state.data_file_count,
            "data_file_bytes": state.data_file_bytes,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _state_from_json(value: str | None) -> TableState | None:
    if value is None:
        return None
    payload = json.loads(value)
    return TableState(
        snapshot_ids=tuple(int(item) for item in payload["snapshot_ids"]),
        current_snapshot_id=payload["current_snapshot_id"],
        current_snapshot_summary={
            str(key): str(item) for key, item in payload["current_snapshot_summary"].items()
        },
        data_file_count=int(payload["data_file_count"]),
        data_file_bytes=int(payload["data_file_bytes"]),
    )


def ensure_maintenance_results(
    spark: SparkSession,
    *,
    namespace: str = "control",
) -> None:
    """Create the append-once maintenance action ledger."""

    catalog = spark.conf.get("spark.sql.defaultCatalog")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog}`.`{namespace}`")
    columns = ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}"
        + (" NOT NULL" if not field.nullable else "")
        for field in MAINTENANCE_RESULT_SCHEMA.fields
    )
    table = f"`{catalog}`.`{namespace}`.`maintenance_results`"
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} "
        f"({columns}) USING iceberg TBLPROPERTIES ("
        "'format-version'='2', "
        f"'grain'='{MAINTENANCE_RESULT_GRAIN}', "
        "'write.distribution-mode'='hash', "
        "'write.target-file-size-bytes'='134217728')"
    )
    evolve_table_schema(
        spark,
        table=table,
        expected_schema=MAINTENANCE_RESULT_SCHEMA,
    )
    property_query = f"SHOW TBLPROPERTIES {table} ('grain')"
    property_row = spark.sql(property_query).first()
    observed_grain = None if property_row is None else str(property_row.value)
    if observed_grain != MAINTENANCE_RESULT_GRAIN:
        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES "
            f"('grain'={_sql_literal(MAINTENANCE_RESULT_GRAIN)})"
        )
        property_row = spark.sql(property_query).first()
        observed_grain = None if property_row is None else str(property_row.value)
    if observed_grain != MAINTENANCE_RESULT_GRAIN:
        raise RuntimeError(
            "Đọc lại chính xác grain của kết quả bảo trì sau migration thất bại"
        )


def _record_from_row(row: object) -> MaintenanceResultRecord:
    values = _row_values(row)
    metrics = ProcedureMetrics(**{name: int(values[name]) for name in _METRIC_FIELDS})
    return MaintenanceResultRecord(
        approval_nonce=str(values["approval_nonce"]),
        approval_run_id=str(values["approval_run_id"]),
        consumer_run_id=str(values["consumer_run_id"]),
        batch_id=str(values["batch_id"]),
        target=str(values["target"]),
        action=str(values["action"]),
        status=str(values["status"]),
        pre_state=_state_from_json(values.get("pre_state_json")),
        metrics=metrics,
        error_summary=values.get("error_summary"),
        original_status=values.get("original_status"),
        original_error_summary=values.get("original_error_summary"),
        resolution_note=values.get("resolution_note"),
        resolution_evidence_json=values.get("resolution_evidence_json"),
        resolved_at=values.get("resolved_at"),
        execution_lease=values.get("execution_lease"),
        execution_lease_acquired_at=values.get("execution_lease_acquired_at"),
        started_at=values.get("started_at"),
        completed_at=values.get("completed_at"),
    )


def maintenance_result_records(
    spark: SparkSession,
    *,
    approval_nonce: str | None = None,
    approval_run_id: str | None = None,
    consumer_run_id: str | None = None,
    action: str,
    namespace: str = "control",
) -> tuple[MaintenanceResultRecord, ...]:
    table = _maintenance_table(spark, namespace)
    if approval_run_id is not None and consumer_run_id is not None:
        condition = (
            f"`approval_run_id` = {_sql_literal(approval_run_id)} AND "
            f"`consumer_run_id` = {_sql_literal(consumer_run_id)}"
        )
    elif approval_nonce is not None:
        condition = f"`approval_nonce` = {_sql_literal(approval_nonce)}"
    else:
        raise ValueError("Tra cứu kết quả bảo trì yêu cầu danh tính quyền sở hữu")
    rows = spark.sql(
        f"SELECT * FROM {table} WHERE {condition} "
        f"AND `action` = {_sql_literal(action)} "
        "ORDER BY `approval_nonce`, `status`, `started_at`"
    ).collect()
    return tuple(_record_from_row(row) for row in rows)


def _duplicate_error(records: Sequence[MaintenanceResultRecord]) -> MaintenanceDuplicateError:
    identities = sorted(
        {
            f"{record.approval_nonce[:16]}:{record.status}:{record.target[:80]}"
            for record in records
        }
    )
    sample = ",".join(identities[:5])
    return MaintenanceDuplicateError(
        f"Quyền sở hữu bảo trì bị trùng count={len(records)} identities=[{sample}]; "
        "cần người vận hành xử lý"
    )


def _single_maintenance_result(
    records: Sequence[MaintenanceResultRecord],
) -> MaintenanceResultRecord | None:
    if len(records) > 1:
        raise _duplicate_error(records)
    return None if not records else records[0]


def maintenance_result_record(
    spark: SparkSession,
    *,
    approval_nonce: str | None = None,
    approval_run_id: str | None = None,
    consumer_run_id: str | None = None,
    action: str,
    namespace: str = "control",
) -> MaintenanceResultRecord | None:
    return _single_maintenance_result(
        maintenance_result_records(
            spark,
            approval_nonce=approval_nonce,
            approval_run_id=approval_run_id,
            consumer_run_id=consumer_run_id,
            action=action,
            namespace=namespace,
        )
    )


@contextmanager
def _maintenance_ownership_lock(
    spark: SparkSession,
    request: MaintenanceRequest,
    *,
    namespace: str,
) -> Iterator[None]:
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    if catalog is None:
        raise RuntimeError("Catalog mặc định của Spark chưa được cấu hình")
    identity = "\0".join(
        (request.approval_run_id, request.run_id, request.action)
    )
    with advisory_ownership_lock(
        spark,
        catalog=catalog,
        scope=f"{namespace}.maintenance_results",
        identity="maintenance-ledger-mutation",
    ):
        with advisory_ownership_lock(
            spark,
            catalog=catalog,
            scope=f"{namespace}.maintenance_results",
            identity=identity,
        ):
            yield


def start_maintenance_result(
    spark: SparkSession,
    *,
    request: MaintenanceRequest,
    approval_nonce: str,
    started_at: datetime,
    namespace: str = "control",
) -> MaintenanceResultRecord:
    with _maintenance_ownership_lock(spark, request, namespace=namespace):
        existing = _single_maintenance_result(
            maintenance_result_records(
                spark,
                approval_run_id=request.approval_run_id,
                consumer_run_id=request.run_id,
                action=request.action,
                namespace=namespace,
            )
        )
        if existing is not None:
            _validate_action_record(existing, request, approval_nonce)
            return existing
        metrics = ProcedureMetrics()
        row = (
            approval_nonce,
            request.approval_run_id,
            request.run_id,
            request.batch_id,
            request.target,
            request.action,
            "APPROVED",
            None,
            *(getattr(metrics, name) for name in _METRIC_FIELDS),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            _utc_naive(started_at),
            None,
        )
        append_error: BaseException | None = None
        try:
            spark.createDataFrame([row], MAINTENANCE_RESULT_SCHEMA).writeTo(
                _maintenance_table(spark, namespace)
            ).append()
        except BaseException as exc:
            append_error = exc
        observed = _single_maintenance_result(
            maintenance_result_records(
                spark,
                approval_run_id=request.approval_run_id,
                consumer_run_id=request.run_id,
                action=request.action,
                namespace=namespace,
            )
        )
        if observed is not None:
            _validate_action_record(observed, request, approval_nonce)
            return observed
        if append_error is not None:
            raise append_error
        raise RuntimeError("Đọc lại chính xác sau khi ghi phê duyệt bảo trì thất bại")


def _lease_age(acquired_at: datetime | None, now: datetime) -> timedelta | None:
    if acquired_at is None:
        return None
    normalized = (
        acquired_at.replace(tzinfo=timezone.utc)
        if acquired_at.tzinfo is None
        else acquired_at.astimezone(timezone.utc)
    )
    return _utc(now) - normalized


def _same_lease_acquisition(
    observed: datetime | None,
    expected: datetime | None,
) -> bool:
    if observed is None or expected is None:
        return observed is expected
    return _lease_acquisition_identity(observed) == _lease_acquisition_identity(expected)


def _lease_acquisition_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _lease_acquisition_identity(value: datetime) -> str:
    return _lease_acquisition_naive(value).isoformat(sep=" ", timespec="seconds")


def _validate_terminal_execution_identity(
    execution_lease: str | None,
    execution_lease_acquired_at: datetime | None,
    *,
    from_status: str,
) -> None:
    if from_status == "EXECUTING":
        if execution_lease is None or execution_lease_acquired_at is None:
            raise ValueError(
                "Kết thúc trạng thái EXECUTING yêu cầu danh tính lease thực thi đầy đủ"
            )
        if _SAFE_EXECUTION_LEASE.fullmatch(execution_lease) is None:
            raise ValueError(
                "Lease thực thi bảo trì phải gồm 64 ký tự hex viết thường"
            )
    elif execution_lease is not None or execution_lease_acquired_at is not None:
        raise ValueError("Kết thúc trạng thái trước commit không được mang lease thực thi")


def claim_maintenance_execution(
    spark: SparkSession,
    *,
    request: MaintenanceRequest,
    approval_nonce: str,
    before: TableState,
    execution_lease: str,
    claimed_at: datetime,
    namespace: str = "control",
) -> MaintenanceResultRecord:
    """Atomically claim one approved action for exactly one execution lease."""

    if _SAFE_EXECUTION_LEASE.fullmatch(execution_lease) is None:
        raise ValueError("Lease thực thi bảo trì phải gồm 64 ký tự hex viết thường")

    def read() -> MaintenanceResultRecord:
        observed = _single_maintenance_result(
            maintenance_result_records(
                spark,
                approval_run_id=request.approval_run_id,
                consumer_run_id=request.run_id,
                action=request.action,
                namespace=namespace,
            )
        )
        if observed is None:
            raise RuntimeError("Kết quả bảo trì không tồn tại")
        _validate_action_record(observed, request, approval_nonce)
        return observed

    def validate_claim(observed: MaintenanceResultRecord) -> MaintenanceResultRecord:
        if observed.status == "EXECUTING":
            if observed.execution_lease == execution_lease:
                if observed.pre_state != before:
                    raise RuntimeError("pre_state của lần thực thi cùng lease không khớp")
                return observed
            age = _lease_age(observed.execution_lease_acquired_at, claimed_at)
            if age is None or age >= EXECUTION_LEASE_STALE_AFTER:
                raise RuntimeError(
                    "Lease thực thi bảo trì đã quá hạn; cần người vận hành xử lý"
                )
            raise RuntimeError("Lease thực thi bảo trì đang do bên yêu cầu khác nắm giữ")
        if observed.status != "APPROVED":
            raise RuntimeError(
                "Hành động bảo trì không sẵn sàng để nhận quyền thực thi"
            )
        return observed

    with _maintenance_ownership_lock(spark, request, namespace=namespace):
        current = validate_claim(read())
        if current.status == "EXECUTING":
            return current
        table = _maintenance_table(spark, namespace)
        update_error: BaseException | None = None
        try:
            spark.sql(
                f"UPDATE {table} SET `status` = 'EXECUTING', "
                f"`pre_state_json` = {_sql_literal(_state_json(before))}, "
                f"`execution_lease` = {_sql_literal(execution_lease)}, "
                f"`execution_lease_acquired_at` = TIMESTAMP '{_timestamp(claimed_at)}' "
                f"WHERE `approval_run_id` = {_sql_literal(request.approval_run_id)} "
                f"AND `consumer_run_id` = {_sql_literal(request.run_id)} "
                f"AND `action` = {_sql_literal(request.action)} "
                f"AND `approval_nonce` = {_sql_literal(approval_nonce)} "
                "AND `status` = 'APPROVED' AND `execution_lease` IS NULL"
            )
        except BaseException as exc:
            update_error = exc
        observed = read()
        if observed.status == "EXECUTING":
            return validate_claim(observed)
        if update_error is not None:
            raise update_error
        raise RuntimeError("Đọc lại chính xác yêu cầu nhận quyền thực thi bảo trì thất bại")


def _maintenance_record_payload(record: MaintenanceResultRecord) -> str:
    started_at = getattr(record, "started_at", None)
    completed_at = getattr(record, "completed_at", None)
    return json.dumps(
        {
            "approval_nonce": record.approval_nonce,
            "approval_run_id": record.approval_run_id,
            "consumer_run_id": record.consumer_run_id,
            "batch_id": record.batch_id,
            "target": record.target,
            "action": record.action,
            "status": record.status,
            "pre_state_json": None if record.pre_state is None else _state_json(record.pre_state),
            **{name: getattr(record.metrics, name) for name in _METRIC_FIELDS},
            "error_summary": record.error_summary,
            "original_status": record.original_status,
            "original_error_summary": record.original_error_summary,
            "resolution_note": record.resolution_note,
            "resolution_evidence_json": record.resolution_evidence_json,
            "resolved_at": None if record.resolved_at is None else record.resolved_at.isoformat(),
            "execution_lease": record.execution_lease,
            "execution_lease_acquired_at": (
                None
                if record.execution_lease_acquired_at is None
                else record.execution_lease_acquired_at.isoformat()
            ),
            "started_at": None if started_at is None else started_at.isoformat(),
            "completed_at": None if completed_at is None else completed_at.isoformat(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _maintenance_record_row(record: MaintenanceResultRecord) -> tuple[object, ...]:
    return (
        record.approval_nonce,
        record.approval_run_id,
        record.consumer_run_id,
        record.batch_id,
        record.target,
        record.action,
        record.status,
        None if record.pre_state is None else _state_json(record.pre_state),
        *(getattr(record.metrics, name) for name in _METRIC_FIELDS),
        record.error_summary,
        record.original_status,
        record.original_error_summary,
        record.resolution_note,
        record.resolution_evidence_json,
        record.resolved_at,
        record.execution_lease,
        record.execution_lease_acquired_at,
        getattr(record, "started_at", None),
        getattr(record, "completed_at", None),
    )


def _maintenance_identity_condition(request: MaintenanceRequest) -> str:
    return (
        f"`approval_run_id` = {_sql_literal(request.approval_run_id)} AND "
        f"`consumer_run_id` = {_sql_literal(request.run_id)} AND "
        f"`action` = {_sql_literal(request.action)}"
    )


def _maintenance_identity_summary(
    spark: SparkSession,
    *,
    request: MaintenanceRequest,
    namespace: str,
) -> MaintenanceIdentitySummary:
    """Return constant-size distributed evidence for one legacy ownership identity."""

    table = _maintenance_table(spark, namespace)
    columns = ", ".join(f"`{field.name}`" for field in MAINTENANCE_RESULT_SCHEMA.fields)
    condition = _maintenance_identity_condition(request)
    aggregate = spark.sql(
        "WITH identity_rows AS ("
        "SELECT sha2(to_json(struct("
        f"{columns}), map('ignoreNullFields', 'false')), 256) AS `payload_hash` "
        f"FROM {table} WHERE {condition}) "
        "SELECT COUNT(*) AS `row_count`, "
        "COUNT(DISTINCT `payload_hash`) AS `distinct_payload_count`, "
        "MIN(`payload_hash`) AS `min_payload_hash`, "
        "MAX(`payload_hash`) AS `max_payload_hash` FROM identity_rows"
    ).first()
    if aggregate is None:
        raise RuntimeError("Bản tổng hợp trùng lặp của bảo trì không trả về hàng nào")
    row_count = int(aggregate.row_count)
    representative = None
    if row_count:
        row = spark.sql(
            f"SELECT {columns} FROM {table} WHERE {condition} "
            "ORDER BY `approval_nonce`, `status`, `started_at` LIMIT 1"
        ).first()
        if row is None:
            raise RuntimeError("Bản ghi đại diện trong tổng hợp trùng lặp bảo trì bị thiếu")
        representative = _record_from_row(row)
    return MaintenanceIdentitySummary(
        row_count=row_count,
        distinct_payload_count=int(aggregate.distinct_payload_count),
        min_payload_hash=aggregate.min_payload_hash,
        max_payload_hash=aggregate.max_payload_hash,
        representative=representative,
    )


def _duplicate_summary_error(summary: MaintenanceIdentitySummary) -> MaintenanceDuplicateError:
    minimum = "none" if summary.min_payload_hash is None else summary.min_payload_hash[:16]
    maximum = "none" if summary.max_payload_hash is None else summary.max_payload_hash[:16]
    return MaintenanceDuplicateError(
        f"Quyền sở hữu bảo trì bị trùng count={summary.row_count} "
        f"distinct_payloads={summary.distinct_payload_count} "
        f"payload_hash_range={minimum}:{maximum}; cần người vận hành xử lý"
    )


def deduplicate_maintenance_results(
    spark: SparkSession,
    *,
    request: MaintenanceRequest,
    namespace: str = "control",
) -> MaintenanceResultRecord:
    """Atomically collapse byte-identical legacy ownership rows, otherwise fail closed."""

    with _maintenance_ownership_lock(spark, request, namespace=namespace):
        summary = _maintenance_identity_summary(
            spark, request=request, namespace=namespace
        )
        if summary.row_count == 0:
            raise RuntimeError("Hàng quyền sở hữu bảo trì không tồn tại")
        if summary.representative is None:
            raise RuntimeError("Bản ghi đại diện quyền sở hữu bảo trì không tồn tại")
        if summary.row_count == 1:
            return summary.representative
        if (
            summary.distinct_payload_count != 1
            or summary.min_payload_hash is None
            or summary.min_payload_hash != summary.max_payload_hash
        ):
            raise _duplicate_summary_error(summary)
        table = _maintenance_table(spark, namespace)
        catalog = spark.conf.get("spark.sql.defaultCatalog")
        staging = (
            f"`{catalog}`.`{namespace}`."
            f"`maintenance_results_dedup_{secrets.token_hex(8)}`"
        )
        columns = ", ".join(f"`{field.name}`" for field in MAINTENANCE_RESULT_SCHEMA.fields)
        identity = _maintenance_identity_condition(request)
        ranked = (
            f"SELECT {columns}, ROW_NUMBER() OVER ("
            "PARTITION BY `approval_run_id`, `consumer_run_id`, `action` "
            "ORDER BY `approval_nonce`, `status`, `started_at`) AS `__dedup_rank` "
            f"FROM {table}"
        )
        staging_attempted = False
        primary_error: BaseException | None = None
        try:
            staging_attempted = True
            spark.sql(
                f"CREATE TABLE {staging} USING iceberg AS "
                f"SELECT {columns} FROM ({ranked}) ranked "
                f"WHERE NOT ({identity}) OR `__dedup_rank` = 1"
            )
            # Iceberg commits INSERT OVERWRITE as one new snapshot. The shared global
            # mutation lock prevents a transition from being lost between staging and commit.
            spark.sql(
                f"INSERT OVERWRITE {table} ({columns}) "
                f"SELECT {columns} FROM {staging}"
            )
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            if staging_attempted:
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {staging} PURGE")
                except BaseException as cleanup_error:
                    if primary_error is None:
                        raise
                    primary_error.add_note(
                        "Dọn dẹp staging bảo trì thất bại "
                        f"({type(cleanup_error).__name__})"
                    )
        observed = _maintenance_identity_summary(
            spark, request=request, namespace=namespace
        )
        if (
            observed.row_count != 1
            or observed.distinct_payload_count != 1
            or observed.min_payload_hash != summary.min_payload_hash
            or observed.representative is None
        ):
            raise RuntimeError(
                "Đọc lại chính xác migration loại bỏ trùng lặp bảo trì thất bại"
            )
        return observed.representative


def finish_maintenance_result(
    spark: SparkSession,
    *,
    request: MaintenanceRequest,
    approval_nonce: str,
    execution_lease: str | None,
    execution_lease_acquired_at: datetime | None,
    status: str,
    metrics: ProcedureMetrics,
    completed_at: datetime,
    error_summary: str | None,
    from_status: str = "EXECUTING",
    namespace: str = "control",
) -> MaintenanceResultRecord:
    if status not in {"SUCCESS", "FAILED", "UNKNOWN"}:
        raise ValueError("Trạng thái kết quả bảo trì không hợp lệ")
    if from_status not in {"APPROVED", "EXECUTING"}:
        raise ValueError("Trạng thái nguồn của kết quả bảo trì không hợp lệ")
    if from_status == "APPROVED" and status != "FAILED":
        raise ValueError("APPROVED chỉ được chuyển sang FAILED")
    _validate_terminal_execution_identity(
        execution_lease,
        execution_lease_acquired_at,
        from_status=from_status,
    )
    summary = truncate_error_summary(error_summary)

    def read() -> MaintenanceResultRecord:
        observed = _single_maintenance_result(
            maintenance_result_records(
                spark,
                approval_run_id=request.approval_run_id,
                consumer_run_id=request.run_id,
                action=request.action,
                namespace=namespace,
            )
        )
        if observed is None:
            raise RuntimeError("Kết quả bảo trì không tồn tại")
        _validate_action_record(observed, request, approval_nonce)
        return observed

    def exact_terminal(observed: MaintenanceResultRecord) -> bool:
        return (
            observed.status == status
            and observed.metrics == metrics
            and observed.error_summary == summary
            and observed.execution_lease == execution_lease
            and _same_lease_acquisition(
                observed.execution_lease_acquired_at,
                execution_lease_acquired_at,
            )
        )

    with _maintenance_ownership_lock(spark, request, namespace=namespace):
        current = read()
        if exact_terminal(current):
            return current
        if current.status != from_status:
            raise RuntimeError("Trạng thái nguồn của kết quả bảo trì không khớp")
        if from_status == "EXECUTING" and current.execution_lease != execution_lease:
            raise RuntimeError("Lease thực thi bảo trì không khớp")
        if from_status == "EXECUTING" and not _same_lease_acquisition(
            current.execution_lease_acquired_at,
            execution_lease_acquired_at,
        ):
            raise RuntimeError("Thời điểm nhận lease thực thi bảo trì không khớp")
        if from_status == "APPROVED" and (
            execution_lease is not None
            or execution_lease_acquired_at is not None
            or current.execution_lease is not None
            or current.execution_lease_acquired_at is not None
        ):
            raise RuntimeError(
                "Kết thúc trạng thái trước commit không được mang lease thực thi"
            )
        table = _maintenance_table(spark, namespace)
        assignments = [f"`status` = {_sql_literal(status)}"]
        assignments.extend(f"`{name}` = {getattr(metrics, name)}" for name in _METRIC_FIELDS)
        assignments.extend(
            (
                "`error_summary` = "
                + ("NULL" if summary is None else _sql_literal(summary)),
                f"`completed_at` = TIMESTAMP '{_timestamp(completed_at)}'",
            )
        )
        lease_condition = (
            "`execution_lease` IS NULL"
            if execution_lease is None
            else f"`execution_lease` = {_sql_literal(execution_lease)}"
        )
        acquired_condition = (
            "`execution_lease_acquired_at` IS NULL"
            if execution_lease_acquired_at is None
            else "`execution_lease_acquired_at` = "
            f"TIMESTAMP '{_lease_acquisition_identity(execution_lease_acquired_at)}'"
        )
        update_error: BaseException | None = None
        try:
            spark.sql(
                f"UPDATE {table} SET {', '.join(assignments)} "
                f"WHERE `approval_run_id` = {_sql_literal(request.approval_run_id)} "
                f"AND `consumer_run_id` = {_sql_literal(request.run_id)} "
                f"AND `approval_nonce` = {_sql_literal(approval_nonce)} "
                f"AND `action` = {_sql_literal(request.action)} "
                f"AND `status` = {_sql_literal(from_status)} AND {lease_condition} "
                f"AND {acquired_condition}"
            )
        except BaseException as exc:
            update_error = exc
        observed = read()
        if exact_terminal(observed):
            return observed
        if update_error is not None:
            raise update_error
        raise RuntimeError("Đọc lại chính xác trạng thái cuối của kết quả bảo trì thất bại")


def _resolve_maintenance_result_locked(
    spark: SparkSession,
    *,
    request: MaintenanceRequest,
    approval_nonce: str,
    execution_lease: str,
    execution_lease_acquired_at: datetime,
    status: str,
    metrics: ProcedureMetrics,
    evidence: Mapping[str, object],
    resolution_note: str,
    resolved_at: datetime,
    namespace: str = "control",
) -> MaintenanceResultRecord:
    """Resolve one terminal ledger row only from exact immutable evidence."""

    if status not in {"SUCCESS", "FAILED"}:
        raise ValueError("Trạng thái xử lý bảo trì phải là SUCCESS hoặc FAILED")
    if not resolution_note.strip() or not str(evidence.get("proof", "")).strip():
        raise ValueError("Xử lý bảo trì yêu cầu ghi chú và bằng chứng")
    current = maintenance_result_record(
        spark,
        approval_run_id=request.approval_run_id,
        consumer_run_id=request.run_id,
        action=request.action,
        namespace=namespace,
    )
    if current is None:
        raise RuntimeError("Kết quả bảo trì không tồn tại")
    _validate_action_record(current, request, approval_nonce)
    _validate_terminal_execution_identity(
        current.execution_lease,
        current.execution_lease_acquired_at,
        from_status="EXECUTING",
    )
    if current.execution_lease != execution_lease:
        raise RuntimeError("Lease thực thi bảo trì không khớp")
    if not _same_lease_acquisition(
        current.execution_lease_acquired_at,
        execution_lease_acquired_at,
    ):
        raise RuntimeError("Thời điểm nhận lease thực thi bảo trì không khớp")
    evidence_json = json.dumps(dict(evidence), sort_keys=True, separators=(",", ":"))
    if current.resolution_note is not None:
        persisted_evidence = (
            None
            if current.resolution_evidence_json is None
            else json.loads(current.resolution_evidence_json)
        )
        if current.status == status and persisted_evidence == dict(evidence):
            return current
        raise RuntimeError("Lịch sử xử lý bảo trì là bất biến")
    if current.status not in {"FAILED", "UNKNOWN"}:
        raise RuntimeError("Xử lý bảo trì yêu cầu trạng thái FAILED hoặc UNKNOWN")
    committed = evidence.get("committed") is True
    if status == "SUCCESS" and not committed:
        raise RuntimeError("Xử lý thành SUCCESS yêu cầu bằng chứng commit chính xác")
    if status == "FAILED" and (current.status != "UNKNOWN" or committed):
        raise RuntimeError(
            "UNKNOWN chỉ có thể được xử lý thành FAILED khi có bằng chứng chưa commit"
        )
    required_identity = {
        "approval_run_id": request.approval_run_id,
        "consumer_run_id": request.run_id,
        "batch_id": request.batch_id,
        "target": request.target,
        "action": request.action,
        "execution_lease": execution_lease,
        "execution_lease_acquired_at": _lease_acquisition_identity(
            execution_lease_acquired_at
        ),
    }
    mismatches = [key for key, value in required_identity.items() if evidence.get(key) != value]
    if mismatches:
        raise RuntimeError("Bằng chứng xử lý bảo trì không khớp: " + ", ".join(mismatches))
    table = _maintenance_table(spark, namespace)
    assignments = [f"`status` = {_sql_literal(status)}"]
    assignments.extend(f"`{name}` = {getattr(metrics, name)}" for name in _METRIC_FIELDS)
    assignments.extend(
        (
            f"`original_status` = {_sql_literal(current.status)}",
            "`original_error_summary` = "
            + (
                "NULL"
                if current.error_summary is None
                else _sql_literal(current.error_summary)
            ),
            f"`resolution_note` = {_sql_literal(resolution_note)}",
            f"`resolution_evidence_json` = {_sql_literal(evidence_json)}",
            f"`resolved_at` = TIMESTAMP '{_timestamp(resolved_at)}'",
        )
    )
    spark.sql(
        f"UPDATE {table} SET {', '.join(assignments)} "
        f"WHERE `approval_run_id` = {_sql_literal(request.approval_run_id)} "
        f"AND `consumer_run_id` = {_sql_literal(request.run_id)} "
        f"AND `action` = {_sql_literal(request.action)} "
        f"AND `execution_lease` = {_sql_literal(execution_lease)} "
        "AND `execution_lease_acquired_at` = "
        f"TIMESTAMP '{_lease_acquisition_identity(execution_lease_acquired_at)}' "
        f"AND `status` = {_sql_literal(current.status)} AND `resolution_note` IS NULL"
    )
    observed = maintenance_result_record(
        spark,
        approval_run_id=request.approval_run_id,
        consumer_run_id=request.run_id,
        action=request.action,
        namespace=namespace,
    )
    if observed is None or observed.status != status or observed.metrics != metrics:
        raise RuntimeError("Đọc lại chính xác kết quả xử lý bảo trì thất bại")
    persisted_evidence = (
        None
        if observed.resolution_evidence_json is None
        else json.loads(observed.resolution_evidence_json)
    )
    if persisted_evidence != dict(evidence):
        raise RuntimeError("Đọc lại bằng chứng xử lý bảo trì thất bại")
    return observed


def resolve_maintenance_result(
    spark: SparkSession,
    *,
    request: MaintenanceRequest,
    approval_nonce: str,
    execution_lease: str,
    execution_lease_acquired_at: datetime,
    status: str,
    metrics: ProcedureMetrics,
    evidence: Mapping[str, object],
    resolution_note: str,
    resolved_at: datetime,
    namespace: str = "control",
) -> MaintenanceResultRecord:
    """Resolve one terminal result under the shared mutation and identity locks."""

    _validate_terminal_execution_identity(
        execution_lease,
        execution_lease_acquired_at,
        from_status="EXECUTING",
    )
    with _maintenance_ownership_lock(spark, request, namespace=namespace):
        return _resolve_maintenance_result_locked(
            spark,
            request=request,
            approval_nonce=approval_nonce,
            execution_lease=execution_lease,
            execution_lease_acquired_at=execution_lease_acquired_at,
            status=status,
            metrics=metrics,
            evidence=evidence,
            resolution_note=resolution_note,
            resolved_at=resolved_at,
            namespace=namespace,
        )


def _retry_failed_maintenance_result_locked(
    spark: SparkSession,
    *,
    request: MaintenanceRequest,
    approval_nonce: str,
    resolution_note: str,
    resolved_at: datetime,
    namespace: str = "control",
) -> MaintenanceResultRecord:
    """Re-open only a proven pre-commit FAILED action for an explicit retry."""

    current = maintenance_result_record(
        spark,
        approval_run_id=request.approval_run_id,
        consumer_run_id=request.run_id,
        action=request.action,
        namespace=namespace,
    )
    if current is None:
        raise RuntimeError("Kết quả bảo trì không tồn tại")
    _validate_action_record(current, request, approval_nonce)
    if current.status != "FAILED" or current.pre_state is not None:
        raise RuntimeError(
            "Chỉ hành động FAILED trước commit đã được chứng minh mới có thể thử lại"
        )
    if current.resolution_note is not None:
        raise RuntimeError("Lịch sử xử lý bảo trì là bất biến")
    evidence = {
        "approval_run_id": request.approval_run_id,
        "consumer_run_id": request.run_id,
        "batch_id": request.batch_id,
        "target": request.target,
        "action": request.action,
        "committed": False,
        "proof": "no execution pre_state was persisted",
    }
    evidence_json = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
    table = _maintenance_table(spark, namespace)
    spark.sql(
        f"UPDATE {table} SET `status` = 'APPROVED', "
        "`execution_lease` = NULL, `execution_lease_acquired_at` = NULL, "
        "`original_status` = 'FAILED', `original_error_summary` = "
        + ("NULL" if current.error_summary is None else _sql_literal(current.error_summary))
        + f", `resolution_note` = {_sql_literal(resolution_note)}, "
        f"`resolution_evidence_json` = {_sql_literal(evidence_json)}, "
        f"`resolved_at` = TIMESTAMP '{_timestamp(resolved_at)}' "
        f"WHERE `approval_run_id` = {_sql_literal(request.approval_run_id)} "
        f"AND `consumer_run_id` = {_sql_literal(request.run_id)} "
        f"AND `action` = {_sql_literal(request.action)} AND `status` = 'FAILED' "
        "AND `pre_state_json` IS NULL AND `resolution_note` IS NULL"
    )
    observed = maintenance_result_record(
        spark,
        approval_run_id=request.approval_run_id,
        consumer_run_id=request.run_id,
        action=request.action,
        namespace=namespace,
    )
    if observed is None or observed.status != "APPROVED":
        raise RuntimeError("Đọc lại quyền thử lại bảo trì thất bại")
    return observed


def retry_failed_maintenance_result(
    spark: SparkSession,
    *,
    request: MaintenanceRequest,
    approval_nonce: str,
    resolution_note: str,
    resolved_at: datetime,
    namespace: str = "control",
) -> MaintenanceResultRecord:
    """Re-open a proven pre-commit failure under the shared mutation protocol."""

    with _maintenance_ownership_lock(spark, request, namespace=namespace):
        return _retry_failed_maintenance_result_locked(
            spark,
            request=request,
            approval_nonce=approval_nonce,
            resolution_note=resolution_note,
            resolved_at=resolved_at,
            namespace=namespace,
        )


def _validate_action_record(
    record: MaintenanceResultRecord,
    request: MaintenanceRequest,
    approval_nonce: str,
) -> None:
    expected = {
        "approval_nonce": approval_nonce,
        "approval_run_id": request.approval_run_id,
        "consumer_run_id": request.run_id,
        "batch_id": request.batch_id,
        "target": request.target,
        "action": request.action,
    }
    mismatches = [name for name, value in expected.items() if getattr(record, name) != value]
    if mismatches:
        raise RuntimeError(
            "Danh tính tiêu thụ phê duyệt bảo trì không khớp: " + ", ".join(mismatches)
        )


def _begin_action_with_readback(
    *,
    spark: SparkSession,
    request: MaintenanceRequest,
    report: Mapping[str, object],
    now: datetime,
    control_namespace: str,
) -> MaintenanceResultRecord:
    nonce = str(report["nonce"])
    last_error: BaseException | None = None
    for _ in range(_FINALIZATION_ATTEMPTS):
        try:
            return start_maintenance_result(
                spark,
                request=request,
                approval_nonce=nonce,
                started_at=now,
                namespace=control_namespace,
            )
        except MaintenanceDuplicateError:
            raise
        except BaseException as exc:
            last_error = exc
    error_name = "none" if last_error is None else type(last_error).__name__
    raise RuntimeError(
        f"Đã hết số lần đọc lại khi bắt đầu phê duyệt bảo trì; error={error_name}"
    )


def _mark_action_executing_with_readback(
    *,
    spark: SparkSession,
    request: MaintenanceRequest,
    approval_nonce: str,
    before: TableState,
    execution_lease: str,
    claimed_at: datetime,
    control_namespace: str,
) -> MaintenanceResultRecord:
    return claim_maintenance_execution(
        spark,
        request=request,
        approval_nonce=approval_nonce,
        before=before,
        execution_lease=execution_lease,
        claimed_at=claimed_at,
        namespace=control_namespace,
    )


def _finish_action_with_readback(
    *,
    spark: SparkSession,
    request: MaintenanceRequest,
    approval_nonce: str,
    execution_lease: str | None,
    execution_lease_acquired_at: datetime | None,
    status: str,
    metrics: ProcedureMetrics,
    error_summary: str | None,
    from_status: str = "EXECUTING",
    control_namespace: str,
) -> None:
    if from_status not in {"APPROVED", "EXECUTING"}:
        raise ValueError("Trạng thái nguồn của kết quả bảo trì không hợp lệ")
    _validate_terminal_execution_identity(
        execution_lease,
        execution_lease_acquired_at,
        from_status=from_status,
    )
    expected_summary = truncate_error_summary(error_summary)
    last_error: BaseException | None = None
    for _ in range(_FINALIZATION_ATTEMPTS):
        try:
            finish_maintenance_result(
                spark,
                request=request,
                approval_nonce=approval_nonce,
                execution_lease=execution_lease,
                execution_lease_acquired_at=execution_lease_acquired_at,
                status=status,
                metrics=metrics,
                completed_at=datetime.now(timezone.utc),
                error_summary=error_summary,
                from_status=from_status,
                namespace=control_namespace,
            )
        except BaseException as exc:
            last_error = exc
        try:
            observed = maintenance_result_record(
                spark,
                approval_run_id=request.approval_run_id,
                consumer_run_id=request.run_id,
                action=request.action,
                namespace=control_namespace,
            )
        except BaseException as exc:
            last_error = exc
            continue
        if observed is None:
            continue
        _validate_action_record(observed, request, approval_nonce)
        mismatches = []
        if observed.status != status:
            mismatches.append("status")
        if observed.execution_lease != execution_lease:
            mismatches.append("execution_lease")
        if not _same_lease_acquisition(
            observed.execution_lease_acquired_at,
            execution_lease_acquired_at,
        ):
            mismatches.append("execution_lease_acquired_at")
        for name in _METRIC_FIELDS:
            if getattr(observed.metrics, name) != getattr(metrics, name):
                mismatches.append(name)
        if observed.error_summary != expected_summary:
            mismatches.append("error_summary")
        if not mismatches:
            return
        if observed.status in {"SUCCESS", "FAILED", "UNKNOWN"}:
            raise RuntimeError(
                "Kết quả đọc lại trạng thái cuối của bảo trì không khớp: "
                + ", ".join(mismatches)
            )
    error_name = "none" if last_error is None else type(last_error).__name__
    raise MaintenanceOutcomeUnknown(request.action) from RuntimeError(
        f"Đã hết số lần đọc lại kết quả bảo trì; error={error_name}"
    )


def _ensure_audit_state(
    spark: SparkSession,
    *,
    control_namespace: str,
) -> None:
    last_error: BaseException | None = None
    for _ in range(_FINALIZATION_ATTEMPTS):
        try:
            ensure_audit_tables(spark, namespace=control_namespace)
            ensure_maintenance_results(spark, namespace=control_namespace)
            return
        except BaseException as exc:
            last_error = exc
    error_name = "none" if last_error is None else type(last_error).__name__
    raise RuntimeError(f"Đã hết số lần khởi tạo audit bảo trì; error={error_name}")


def _start_pipeline_with_readback(
    *,
    spark: SparkSession,
    request: MaintenanceRequest,
    now: datetime,
    logical_run_id: str | None = None,
    attempt_id: str | None = None,
    control_namespace: str,
) -> PipelineRunRecord:
    expected_logical_run_id = request.run_id if logical_run_id is None else str(logical_run_id)
    new_attempt_id = "1" if attempt_id is None else str(attempt_id)
    expected = {
        "run_id": request.run_id,
        "logical_run_id": expected_logical_run_id,
        "batch_id": request.batch_id,
        "job_name": "iceberg_maintenance",
        "layer": "maintenance",
        "mode": request.mode,
    }
    def validate(observed: PipelineRunRecord, expected_attempt_id: str) -> PipelineRunRecord:
        identity = {**expected, "attempt_id": expected_attempt_id}
        mismatches = [
            name
            for name, value in identity.items()
            if getattr(observed, name, None) is None
            or str(getattr(observed, name)) != str(value)
        ]
        if mismatches:
            raise RuntimeError(
                "Danh tính bắt đầu audit pipeline không khớp: " + ", ".join(mismatches)
            )
        return observed

    last_error: BaseException | None = None
    for _ in range(_FINALIZATION_ATTEMPTS):
        try:
            observed = pipeline_run_record(
                spark, run_id=request.run_id, namespace=control_namespace
            )
        except BaseException as exc:
            last_error = exc
            continue
        if observed is not None:
            expected_attempt_id = (
                "legacy"
                if attempt_id is None and getattr(observed, "attempt_id", None) == "legacy"
                else new_attempt_id
            )
            return validate(observed, expected_attempt_id)
        try:
            started = start_pipeline_run(
                spark,
                run_id=request.run_id,
                batch_id=request.batch_id,
                job_name="iceberg_maintenance",
                layer="maintenance",
                mode=request.mode,
                started_at=now,
                logical_run_id=expected_logical_run_id,
                attempt_id=new_attempt_id,
                namespace=control_namespace,
            )
            return validate(started, new_attempt_id)
        except BaseException as exc:
            last_error = exc
        try:
            observed = pipeline_run_record(
                spark, run_id=request.run_id, namespace=control_namespace
            )
        except BaseException as exc:
            last_error = exc
            continue
        if observed is not None:
            return validate(observed, new_attempt_id)
    error_name = "none" if last_error is None else type(last_error).__name__
    raise RuntimeError(
        f"Đã hết số lần đọc lại khi bắt đầu audit pipeline; error={error_name}"
    )


def _finish_with_readback(
    spark: SparkSession,
    *,
    request: MaintenanceRequest,
    status: str,
    input_count: int,
    output_count: int,
    rejected_count: int,
    error_summary: str | None,
    control_namespace: str,
) -> None:
    expected_summary = truncate_error_summary(error_summary)
    expected = {
        "status": status,
        "input_count": input_count,
        "output_count": output_count,
        "rejected_count": rejected_count,
        "error_summary": expected_summary,
    }
    last_error: BaseException | None = None
    for _ in range(_FINALIZATION_ATTEMPTS):
        try:
            observed = pipeline_run_outcome(
                spark, run_id=request.run_id, namespace=control_namespace
            )
        except BaseException as exc:
            last_error = exc
            observed = None
        if observed is not None and observed.status in {"SUCCESS", "FAILED", "UNKNOWN"}:
            mismatches = [
                name for name, value in expected.items() if getattr(observed, name) != value
            ]
            if not mismatches:
                return
            raise RuntimeError(
                "Kết quả đọc lại trạng thái cuối audit pipeline không khớp: "
                + ", ".join(mismatches)
            )
        try:
            finish_pipeline_run(
                spark,
                run_id=request.run_id,
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
            observed = pipeline_run_outcome(
                spark, run_id=request.run_id, namespace=control_namespace
            )
        except BaseException as exc:
            last_error = exc
            continue
        if observed is None:
            continue
        mismatches = [name for name, value in expected.items() if getattr(observed, name) != value]
        if not mismatches:
            return
        if observed.status in {"SUCCESS", "FAILED", "UNKNOWN"}:
            raise RuntimeError(
                "Kết quả đọc lại trạng thái cuối audit pipeline không khớp: "
                + ", ".join(mismatches)
            )
    error_name = "none" if last_error is None else type(last_error).__name__
    raise RuntimeError(f"Đã hết số lần kết thúc audit pipeline; error={error_name}")


def _report_input_count(report: Mapping[str, object]) -> int:
    metrics = report.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("Chỉ số trong báo cáo dry run không hợp lệ")
    return int(metrics["data_file_count"])


def _reconcile_prior_execution(
    spark: SparkSession,
    request: MaintenanceRequest,
    record: MaintenanceResultRecord,
    *,
    catalog: str,
) -> ProcedureExecution:
    if record.pre_state is None:
        raise MaintenanceOutcomeUnknown(request.action)
    after = _capture_state_with_retries(spark, request, catalog=catalog)
    metrics = _reconcile_changed_state(request.action, record.pre_state, after)
    if metrics is None:
        raise MaintenanceOutcomeUnknown(request.action, record.pre_state, after)
    return ProcedureExecution(
        status="SUCCESS",
        metrics=metrics,
        before=record.pre_state,
        after=after,
        reconciled_after_error=True,
    )


def run_maintenance_job(
    *,
    spark: SparkSession,
    settings: Settings,
    request: MaintenanceRequest,
    attempt_id: str | None = None,
    dry_run_report: Path,
    control_namespace: str = "control",
    now: datetime | None = None,
    approval_secret: bytes | None = None,
    resolve_unknown: bool = False,
    retry_failed: bool = False,
    resolution_note: str | None = None,
) -> ProcedureMetrics:
    """Audit and execute one report-gated, replay-safe maintenance operation."""

    current = datetime.now(timezone.utc) if now is None else _utc(now)
    secret = load_approval_secret() if approval_secret is None else approval_secret
    if len(secret) != MIN_APPROVAL_SECRET_BYTES:
        raise ValueError("Secret phê duyệt bảo trì phải có chính xác 32 byte")
    catalog = settings.iceberg_catalog_name
    input_count = 0
    output_count = 0
    nonce: str | None = None
    action_record: MaintenanceResultRecord | None = None
    execution_lease: str | None = None
    execution_claimed = False
    pipeline_attempt_id = "1" if attempt_id is None else str(attempt_id)
    pipeline_request = (
        request
        if attempt_id is None
        else replace(request, run_id=attempt_run_id(request.run_id, attempt_id))
    )
    if (resolve_unknown or retry_failed) and not (resolution_note or "").strip():
        raise ValueError("Cần ghi chú xử lý tường minh")

    def resolve_pipeline_success(metrics: ProcedureMetrics, proof: str) -> None:
        resolve_pipeline_run(
            spark,
            run_id=pipeline_request.run_id,
            status="SUCCESS",
            evidence=PipelineResolutionEvidence(
                logical_run_id=request.run_id,
                attempt_id=pipeline_attempt_id,
                batch_id=request.batch_id,
                job_name="iceberg_maintenance",
                layer="maintenance",
                mode=request.mode,
                committed=True,
                input_count=input_count,
                output_count=metrics.primary_count(request.action),
                rejected_count=0,
                proof=proof,
            ),
            resolution_note=resolution_note or "evidence reconciliation",
            resolved_at=current,
            namespace=control_namespace,
        )
    try:
        _ensure_audit_state(spark, control_namespace=control_namespace)
        pipeline_record = _start_pipeline_with_readback(
            spark=spark,
            request=pipeline_request,
            now=current,
            logical_run_id=request.run_id,
            attempt_id=attempt_id,
            control_namespace=control_namespace,
        )
        pipeline_attempt_id = pipeline_record.attempt_id
        if request.action == "dry-run":
            report = create_dry_run_report(
                spark,
                request,
                dry_run_report,
                now=current,
                catalog=catalog,
                secret=secret,
            )
            input_count = _report_input_count(report)
            _finish_with_readback(
                spark,
                request=pipeline_request,
                status="SUCCESS",
                input_count=input_count,
                output_count=1,
                rejected_count=0,
                error_summary=None,
                control_namespace=control_namespace,
            )
            return ProcedureMetrics()

        report = validate_dry_run_report(dry_run_report, request, now=current, secret=secret)
        input_count = _report_input_count(report)
        nonce = str(report["nonce"])
        action_record = _begin_action_with_readback(
            spark=spark,
            request=request,
            report=report,
            now=current,
            control_namespace=control_namespace,
        )
        execution_lease = secrets.token_hex(32)
        if action_record.status == "SUCCESS":
            metrics = action_record.metrics
            output_count = metrics.primary_count(request.action)
            if resolve_unknown:
                resolve_pipeline_success(metrics, "maintenance result exact SUCCESS readback")
            else:
                _finish_with_readback(
                    spark,
                    request=pipeline_request,
                    status="SUCCESS",
                    input_count=input_count,
                    output_count=output_count,
                    rejected_count=0,
                    error_summary=None,
                    control_namespace=control_namespace,
                )
            return metrics
        if action_record.status == "UNKNOWN":
            if not resolve_unknown:
                raise MaintenanceOutcomeUnknown(request.action)
            if (
                action_record.execution_lease is None
                or action_record.execution_lease_acquired_at is None
            ):
                raise RuntimeError(
                    "Hàng UNKNOWN của bảo trì không có lease thực thi đầy đủ; "
                    "cần người vận hành xử lý"
                )
            execution = _reconcile_prior_execution(
                spark, request, action_record, catalog=catalog
            )
            metrics = execution.metrics
            evidence = {
                "approval_run_id": request.approval_run_id,
                "consumer_run_id": request.run_id,
                "batch_id": request.batch_id,
                "target": request.target,
                "action": request.action,
                "execution_lease": action_record.execution_lease,
                "execution_lease_acquired_at": _lease_acquisition_identity(
                    action_record.execution_lease_acquired_at
                ),
                "committed": True,
                "proof": "pre_state and current Iceberg metadata reconcile to one commit",
                "pre_state": json.loads(_state_json(execution.before)),
                "post_state": json.loads(_state_json(execution.after)),
            }
            resolve_maintenance_result(
                spark,
                request=request,
                approval_nonce=nonce,
                execution_lease=action_record.execution_lease,
                execution_lease_acquired_at=action_record.execution_lease_acquired_at,
                status="SUCCESS",
                metrics=metrics,
                evidence=evidence,
                resolution_note=resolution_note or "evidence reconciliation",
                resolved_at=current,
                namespace=control_namespace,
            )
            resolve_pipeline_success(metrics, str(evidence["proof"]))
            return metrics
        if action_record.status == "FAILED":
            if not retry_failed:
                raise RuntimeError("Hành động bảo trì trước đó đã FAILED trước commit")
            action_record = retry_failed_maintenance_result(
                spark,
                request=request,
                approval_nonce=nonce,
                resolution_note=resolution_note or "explicit pre-commit retry",
                resolved_at=current,
                namespace=control_namespace,
            )
        if request.action == "remove-orphan-files" and not request.confirm_orphan_cleanup:
            raise ValueError("remove-orphan-files yêu cầu xác nhận tường minh")

        if action_record.status == "EXECUTING":
            if action_record.pre_state is None:
                raise RuntimeError("Lease thực thi bảo trì không có pre_state đã lưu")
            action_record = _mark_action_executing_with_readback(
                spark=spark,
                request=request,
                approval_nonce=nonce,
                before=action_record.pre_state,
                execution_lease=execution_lease,
                claimed_at=current,
                control_namespace=control_namespace,
            )
            execution_claimed = (
                action_record.execution_lease == execution_lease
                and _same_lease_acquisition(
                    action_record.execution_lease_acquired_at,
                    current,
                )
            )
            if not execution_claimed:
                raise RuntimeError(
                    "Lease khi nhận quyền thực thi bảo trì không khớp chính xác"
                )
            execution = _reconcile_prior_execution(spark, request, action_record, catalog=catalog)
        elif action_record.status == "APPROVED":
            before = _capture_state_with_retries(spark, request, catalog=catalog)
            action_record = _mark_action_executing_with_readback(
                spark=spark,
                request=request,
                approval_nonce=nonce,
                before=before,
                execution_lease=execution_lease,
                claimed_at=current,
                control_namespace=control_namespace,
            )
            execution_claimed = (
                action_record.execution_lease == execution_lease
                and _same_lease_acquisition(
                    action_record.execution_lease_acquired_at,
                    current,
                )
            )
            if not execution_claimed:
                raise RuntimeError(
                    "Lease khi nhận quyền thực thi bảo trì không khớp chính xác"
                )
            execution = execute_maintenance_procedure(
                spark,
                request,
                catalog=catalog,
                now=current,
                before=before,
            )
        else:
            raise RuntimeError("Hành động bảo trì có trạng thái đã lưu không hợp lệ")

        metrics = execution.metrics
        output_count = metrics.primary_count(request.action)
        _finish_action_with_readback(
            spark=spark,
            request=request,
            approval_nonce=nonce,
            execution_lease=execution_lease,
            execution_lease_acquired_at=action_record.execution_lease_acquired_at,
            status="SUCCESS",
            metrics=metrics,
            error_summary=None,
            control_namespace=control_namespace,
        )
        _finish_with_readback(
            spark,
            request=pipeline_request,
            status="SUCCESS",
            input_count=input_count,
            output_count=output_count,
            rejected_count=0,
            error_summary=None,
            control_namespace=control_namespace,
        )
        return metrics
    except MaintenanceOutcomeUnknown as exc:
        summary = (
            f"MaintenanceOutcomeUnknown: hành động {request.action} cần người vận hành đối soát"
        )
        if (
            execution_claimed
            and nonce is not None
            and action_record is not None
        ):
            try:
                _finish_action_with_readback(
                    spark=spark,
                    request=request,
                    approval_nonce=nonce,
                    execution_lease=action_record.execution_lease,
                    execution_lease_acquired_at=(
                        action_record.execution_lease_acquired_at
                    ),
                    status="UNKNOWN",
                    metrics=ProcedureMetrics(),
                    error_summary=summary,
                    control_namespace=control_namespace,
                )
            except BaseException as audit_exc:
                exc.add_note(
                    f"Kết thúc kết quả bảo trì thất bại ({type(audit_exc).__name__})"
                )
        if execution_claimed or request.action == "dry-run":
            try:
                _finish_with_readback(
                    spark,
                    request=pipeline_request,
                    status="UNKNOWN",
                    input_count=input_count,
                    output_count=output_count,
                    rejected_count=0,
                    error_summary=summary,
                    control_namespace=control_namespace,
                )
            except BaseException as audit_exc:
                exc.add_note(
                    f"Kết thúc audit pipeline thất bại ({type(audit_exc).__name__})"
                )
        raise
    except Exception as exc:
        summary = (
            f"{type(exc).__name__}: hành động bảo trì {request.action} "
            "thất bại trước khi commit được xác nhận"
        )
        if (
            execution_claimed
            and nonce is not None
            and action_record is not None
        ):
            unknown = MaintenanceOutcomeUnknown(request.action, action_record.pre_state)
            unknown.add_note(
                f"Lỗi chính xảy ra sau khi khóa thực thi ({type(exc).__name__})"
            )
            try:
                _finish_action_with_readback(
                    spark=spark,
                    request=request,
                    approval_nonce=nonce,
                    execution_lease=execution_lease,
                    execution_lease_acquired_at=(
                        action_record.execution_lease_acquired_at
                    ),
                    status="UNKNOWN",
                    metrics=ProcedureMetrics(),
                    error_summary=(
                        "MaintenanceOutcomeUnknown: kết quả khóa thực thi cần được đối soát"
                    ),
                    control_namespace=control_namespace,
                )
            except BaseException as audit_exc:
                unknown.add_note(
                    f"Kết thúc kết quả bảo trì thất bại ({type(audit_exc).__name__})"
                )
            try:
                _finish_with_readback(
                    spark,
                    request=pipeline_request,
                    status="UNKNOWN",
                    input_count=input_count,
                    output_count=output_count,
                    rejected_count=0,
                    error_summary=(
                        "MaintenanceOutcomeUnknown: kết quả khóa thực thi cần được đối soát"
                    ),
                    control_namespace=control_namespace,
                )
            except BaseException as audit_exc:
                unknown.add_note(
                    f"Kết thúc audit pipeline thất bại ({type(audit_exc).__name__})"
                )
            raise unknown from exc
        precommit_owned = False
        if (
            nonce is not None
            and action_record is not None
            and action_record.status == "APPROVED"
        ):
            try:
                _finish_action_with_readback(
                    spark=spark,
                    request=request,
                    approval_nonce=nonce,
                    execution_lease=None,
                    execution_lease_acquired_at=None,
                    status="FAILED",
                    metrics=ProcedureMetrics(),
                    error_summary=summary,
                    from_status="APPROVED",
                    control_namespace=control_namespace,
                )
                precommit_owned = True
            except BaseException as audit_exc:
                exc.add_note(
                    "Kết thúc kết quả bảo trì trước commit thất bại "
                    f"({type(audit_exc).__name__})"
                )
        if precommit_owned or request.action == "dry-run":
            try:
                _finish_with_readback(
                    spark,
                    request=pipeline_request,
                    status="FAILED",
                    input_count=input_count,
                    output_count=output_count,
                    rejected_count=1,
                    error_summary=summary,
                    control_namespace=control_namespace,
                )
            except BaseException as audit_exc:
                exc.add_note(f"Kết thúc audit thất bại ({type(audit_exc).__name__})")
        raise


def _runtime_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Nạp cấu hình runtime Settings với tài khoản quyền ghi Lakehouse (MINIO_PROCESSING_USER)."""
    values = dict(os.environ if env is None else env)
    values.setdefault("MINIO_ROOT_USER", values.get("AWS_ACCESS_KEY_ID", ""))
    values.setdefault("MINIO_ROOT_PASSWORD", values.get("AWS_SECRET_ACCESS_KEY", ""))
    return Settings.from_env(values)


def _parser() -> argparse.ArgumentParser:
    """Khởi tạo trình phân tích đối số dòng lệnh chuẩn (argparse) với Zero-Config Defaults.

    Thiết kế Zero-Config Defaults cho phép:
    1. Chạy tương thích 100% với Airflow DAG khi truyền đầy đủ các tham số (namespaced, run_id, dry_run_report, v.v.).
    2. Chạy kiểm thử thủ công từ CLI mà không cần gõ hàng loạt cờ cấu hình phức tạp (mặc định dry-run bảng gold.fact_orders).
    """
    now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(
        description="Thực hiện bảo trì an toàn và tối ưu hóa cho một table Iceberg trong Lakehouse."
    )
    parser.add_argument(
        "--run-id",
        default=f"maint_{now_str}",
        help="Mã định danh run bảo trì (mặc định: maint_<timestamp>).",
    )
    parser.add_argument(
        "--attempt-id",
        default="1",
        help="Mã attempt bảo trì (mặc định: 1).",
    )
    parser.add_argument(
        "--approval-run-id",
        default=None,
        help="Mã run phê duyệt (mặc định: trùng với run-id nếu không truyền).",
    )
    parser.add_argument(
        "--batch-id",
        default=f"batch_{now_str}",
        help="Mã batch nguồn (mặc định: batch_<timestamp>).",
    )
    parser.add_argument(
        "--mode",
        choices=("full", "incremental"),
        default="full",
        help="Chế độ của run: full hoặc incremental (mặc định: full).",
    )
    parser.add_argument(
        "--namespace",
        choices=tuple(ALLOWED_TARGETS),
        default="gold",
        help="Namespace đích chứa table Iceberg (mặc định: gold).",
    )
    parser.add_argument(
        "--table",
        default="fact_orders",
        help="Tên table Iceberg đích trong allowlist (mặc định: fact_orders).",
    )
    parser.add_argument(
        "--action",
        choices=tuple(sorted(ALLOWED_ACTIONS)),
        default="dry-run",
        help="Hành động bảo trì: dry-run, rewrite-data-files, expire-snapshots, remove-orphan-files (mặc định: dry-run).",
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=MIN_RETENTION_HOURS,
        help=f"Số giờ giữ snapshot tối thiểu (mặc định: {MIN_RETENTION_HOURS}).",
    )
    parser.add_argument(
        "--orphan-older-than-hours",
        type=int,
        default=336,
        help="Tuổi tối thiểu theo giờ của orphan file để được phép xóa (mặc định: 336).",
    )
    parser.add_argument(
        "--dry-run-report",
        type=Path,
        default=None,
        help="Đường dẫn file báo cáo dry run JSON (mặc định: logs/maintenance/<approval-run-id>/dry-run.json).",
    )
    parser.add_argument(
        "--confirm-orphan-cleanup",
        action="store_true",
        help="Cờ xác nhận đồng ý xóa vĩnh viễn các orphan files trên S3/MinIO.",
    )
    parser.add_argument(
        "--resolve-unknown",
        action="store_true",
        help="Cho phép xử lý trạng thái unknown của lần chạy trước.",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Cho phép thử lại hành động đang ở trạng thái failed.",
    )
    parser.add_argument(
        "--deduplicate-legacy",
        action="store_true",
        help="Loại bỏ các bản ghi audit legacy bị trùng lặp.",
    )
    parser.add_argument(
        "--resolution-note",
        help="Ghi chú giải trình bắt buộc khi thực hiện resolve thủ công.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Điểm khởi nhập chính của CLI bảo trì Apache Iceberg có kiểm soát an toàn."""
    args = _parser().parse_args(argv)
    approval_run_id = args.approval_run_id or args.run_id

    # Thiết lập đường dẫn báo cáo dry-run thông minh
    dry_run_report = args.dry_run_report
    if dry_run_report is None:
        candidate_dir = Path(f"/opt/airflow/logs/maintenance/{approval_run_id}")
        if not candidate_dir.parent.parent.exists():
            candidate_dir = Path(f"logs/maintenance/{approval_run_id}")
        dry_run_report = candidate_dir / "dry-run.json"

    request = MaintenanceRequest(
        run_id=args.run_id,
        approval_run_id=approval_run_id,
        batch_id=args.batch_id,
        mode=args.mode,
        namespace=args.namespace,
        table=args.table,
        action=args.action,
        retention_hours=args.retention_hours,
        orphan_older_than_hours=args.orphan_older_than_hours,
        confirm_orphan_cleanup=args.confirm_orphan_cleanup,
    )
    configure_logging()
    settings = _runtime_settings()
    spark = build_spark_session(settings, f"olist-maintenance-{request.run_id}")
    try:
        if args.deduplicate_legacy:
            _ensure_audit_state(spark, control_namespace="control")
            deduplicate_maintenance_results(spark, request=request)
            return 0
        secret = load_approval_secret()
        run_maintenance_job(
            spark=spark,
            settings=settings,
            request=request,
            attempt_id=args.attempt_id,
            dry_run_report=dry_run_report,
            approval_secret=secret,
            resolve_unknown=args.resolve_unknown,
            retry_failed=args.retry_failed,
            resolution_note=args.resolution_note,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
