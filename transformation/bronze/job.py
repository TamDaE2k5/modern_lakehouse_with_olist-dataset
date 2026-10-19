"""Tiến trình Nhạc trưởng Điều phối nạp dữ liệu Tầng Bronze (Bronze Orchestrator).

Nhiệm vụ:
1. Đọc các file CSV từ MinIO bucket `raw/olist/`.
2. Kiểm tra Preflight (kích thước file, header cột khớp với registry).
3. Ghi vết Audit hai tầng vào PostgreSQL qua Apache Iceberg (`pipeline_runs` và `ingestion_batches`).
4. Nạp dữ liệu vào bảng Iceberg v2 với cơ chế Zero-Config Defaults (chạy được ngay không cần cờ CLI).
5. Hỗ trợ 2 chế độ: full (ghi đè toàn bộ) và incremental (chèn thêm theo batch).
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from transformation.bronze.ingest import add_ingestion_metadata
from transformation.common.audit import (
    attempt_run_id,
    ensure_audit_tables,
    finish_ingestion_batch,
    finish_pipeline_run,
    start_ingestion_batch,
    start_pipeline_run,
    started_ingestion_sources,
    truncate_error_summary,
)
from transformation.common.config import Settings
from transformation.common.iceberg import (
    BatchAlreadyExistsError,
    append_batch,
    create_or_replace,
    replace_batch,
)
from transformation.common.logging import configure_logging, log_event
from transformation.common.models import TableSpec
from transformation.common.registry import BRONZE_METADATA_COLUMNS, TABLE_SPECS, get_table_spec
from transformation.common.spark_session import build_spark_session


LOGGER = logging.getLogger(__name__)
RAW_BUCKET = "raw"
RAW_PREFIX = "olist"


class StagedFilesError(RuntimeError):
    """Raised when one or more selected staged objects fail preflight validation."""

    def __init__(
        self,
        message: str,
        *,
        validated: Mapping[str, StagedObject] | None = None,
    ) -> None:
        super().__init__(message)
        self.validated = dict(validated or {})


@dataclass(frozen=True)
class StagedObject:
    """Validated object metadata needed by ingestion and audit."""

    key: str
    size_bytes: int


def _source_columns(spec: TableSpec) -> tuple[str, ...]:
    metadata_names = {column.name for column in BRONZE_METADATA_COLUMNS}
    return tuple(column.name for column in spec.columns if column.name not in metadata_names)


def _header_from_object(s3_client: Any, bucket: str, key: str) -> tuple[str, ...]:
    response = s3_client.get_object(Bucket=bucket, Key=key, Range="bytes=0-65535")
    prefix = response["Body"].read().decode("utf-8-sig")
    lines = prefix.splitlines()
    if not lines:
        return ()
    return tuple(next(csv.reader([lines[0]])))


def validate_staged_files(
    specs: Sequence[TableSpec],
    *,
    s3_client: Any,
    bucket: str = RAW_BUCKET,
    prefix: str = RAW_PREFIX,
) -> dict[str, StagedObject]:
    """Validate every selected object in MinIO before creating Iceberg tables."""
    validated: dict[str, StagedObject] = {}
    errors: list[str] = []

    for spec in specs:
        key = f"{prefix}/{spec.csv_file}"
        try:
            head = s3_client.head_object(Bucket=bucket, Key=key)
            size_bytes = int(head.get("ContentLength", 0))
            header = _header_from_object(s3_client, bucket, key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", "unknown"))
            errors.append(f"{spec.csv_file}: staged object unavailable ({code})")
            continue
        except (UnicodeDecodeError, csv.Error, OSError, ValueError) as exc:
            errors.append(f"{spec.csv_file}: failed to read staged header ({exc})")
            continue

        if size_bytes <= 0:
            errors.append(f"{spec.csv_file}: staged object is empty")
        elif header != _source_columns(spec):
            errors.append(
                f"{spec.csv_file}: expected header {_source_columns(spec)}, got {header}"
            )
        else:
            validated[spec.name] = StagedObject(key, size_bytes)

    if errors:
        raise StagedFilesError(
            "Staged file validation failed: " + "; ".join(errors),
            validated=validated,
        )
    return validated


def _runtime_settings(env: Mapping[str, str] | None = None) -> Settings:
    values = dict(os.environ if env is None else env)
    values.setdefault("MINIO_ROOT_USER", values.get("AWS_ACCESS_KEY_ID", ""))
    values.setdefault("MINIO_ROOT_PASSWORD", values.get("AWS_SECRET_ACCESS_KEY", ""))
    return Settings.from_env(values)


def _s3_client(settings: Settings) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.minio_root_user,
        aws_secret_access_key=settings.minio_root_password,
        region_name=settings.s3_region,
    )


def _read_source(spark: SparkSession, spec: TableSpec, path: str) -> DataFrame:
    schema = StructType(
        [
            StructField(column.name, StringType(), column.nullable)
            for column in spec.columns
            if column.name in _source_columns(spec)
        ]
    )
    return (
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .option("enforceSchema", "true")
        .schema(schema)
        .csv(path)
    )


def _cleanup_failure(phase: str, exc: BaseException) -> str:
    return f"{phase} ({type(exc).__name__})"


def _finish_failed_audits(
    *,
    spark: SparkSession,
    specs: Sequence[TableSpec],
    started_sources: set[str],
    source_counts: Mapping[str, int],
    committed_sources: set[str],
    run_id: str,
    batch_id: str,
    total_input: int,
    total_output: int,
    summary: str,
    control_namespace: str,
) -> tuple[str, ...]:
    """Best-effort update of all started source and pipeline records to FAILED."""
    failures: list[str] = []
    specs_by_file = {spec.csv_file: spec for spec in specs}
    in_memory_files = [
        spec.csv_file for spec in specs if spec.name in started_sources
    ]
    try:
        source_files = list(
            started_ingestion_sources(
                spark,
                run_id=run_id,
                batch_id=batch_id,
                namespace=control_namespace,
            )
        )
    except BaseException as cleanup_exc:
        failures.append(_cleanup_failure("reconcile_started_sources", cleanup_exc))
        source_files = []
    source_files.extend(name for name in in_memory_files if name not in source_files)

    for source_file in source_files:
        spec = specs_by_file.get(source_file)
        source_row_count = 0 if spec is None else source_counts.get(spec.name, 0)
        target_committed = spec is not None and spec.name in committed_sources
        try:
            finish_ingestion_batch(
                spark,
                run_id=run_id,
                batch_id=batch_id,
                source_file=source_file,
                accepted_count=source_row_count if target_committed else 0,
                rejected_count=0,
                status="FAILED",
                completed_at=datetime.now(timezone.utc),
                error_summary=summary,
                checksum="",
                source_row_count=source_row_count,
                source_row_hash="",
                target_committed=target_committed,
                namespace=control_namespace,
            )
        except BaseException as cleanup_exc:
            failures.append(_cleanup_failure(f"finish_ingestion_batch[{source_file}]", cleanup_exc))

    try:
        finish_pipeline_run(
            spark,
            run_id=run_id,
            input_count=total_input,
            output_count=total_output,
            rejected_count=0,
            status="FAILED",
            completed_at=datetime.now(timezone.utc),
            error_summary=summary,
            namespace=control_namespace,
        )
    except BaseException as cleanup_exc:
        failures.append(_cleanup_failure("finish_pipeline_run", cleanup_exc))
    return tuple(failures)


def run_bronze_job(
    *,
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    attempt_id: str | None = None,
    batch_id: str,
    mode: str = "full",
    replay_batch: bool = False,
    table_names: Sequence[str] | None = None,
    raw_bucket: str = RAW_BUCKET,
    raw_prefix: str = RAW_PREFIX,
    bronze_namespace: str = "bronze",
    control_namespace: str = "control",
    s3_client: Any | None = None,
) -> None:
    """Ingest selected staged sources into Iceberg Bronze tables with durable audit."""
    if mode not in {"full", "incremental"}:
        raise ValueError("mode must be 'full' or 'incremental'")
    if replay_batch and mode != "incremental":
        raise ValueError("--replay-batch requires --mode incremental")

    selected_names = tuple(TABLE_SPECS) if table_names is None else tuple(table_names)
    if not selected_names:
        raise ValueError("must select at least one table")
    if len(set(selected_names)) != len(selected_names):
        raise ValueError("selected tables cannot contain duplicates")

    specs = tuple(get_table_spec(name) for name in selected_names)
    now = datetime.now(timezone.utc)
    catalog = settings.iceberg_catalog_name
    audit_run_id = run_id if attempt_id is None else attempt_run_id(run_id, attempt_id)

    ensure_audit_tables(spark, namespace=control_namespace)
    start_pipeline_run(
        spark,
        run_id=audit_run_id,
        logical_run_id=run_id,
        attempt_id="1" if attempt_id is None else attempt_id,
        batch_id=batch_id,
        job_name="bronze_ingestion",
        layer="bronze",
        mode=mode,
        started_at=now,
        namespace=control_namespace,
    )

    total_input = 0
    total_output = 0
    staged: dict[str, StagedObject] = {}
    source_counts: dict[str, int] = {}
    committed_sources: set[str] = set()
    started_sources: set[str] = set()

    try:
        for spec in specs:
            start_ingestion_batch(
                spark,
                run_id=audit_run_id,
                batch_id=batch_id,
                source_file=spec.csv_file,
                checksum="",
                source_row_count=0,
                started_at=now,
                namespace=control_namespace,
            )
            started_sources.add(spec.name)

        try:
            staged = validate_staged_files(
                specs,
                s3_client=_s3_client(settings) if s3_client is None else s3_client,
                bucket=raw_bucket,
                prefix=raw_prefix,
            )
        except StagedFilesError as exc:
            staged.update(exc.validated)
            raise

        # Check and count source rows
        for spec in specs:
            source = _read_source(spark, spec, f"s3a://{raw_bucket}/{staged[spec.name].key}")
            count = source.count()
            if count <= 0:
                raise RuntimeError(f"{spec.csv_file}: staged source contains zero data rows")
            source_counts[spec.name] = count

        total_input = sum(source_counts.values())
        ingested_at = datetime.now(timezone.utc)

        # Ingest each table into Iceberg
        for spec in specs:
            staged_object = staged[spec.name]
            source = _read_source(spark, spec, f"s3a://{raw_bucket}/{staged_object.key}")
            bronze = add_ingestion_metadata(
                source,
                batch_id=batch_id,
                source_file=spec.csv_file,
                ingested_at=ingested_at,
            )
            row_count = source_counts[spec.name]
            target = f"{catalog}.{bronze_namespace}.{spec.name}"

            if mode == "full":
                create_or_replace(
                    spark,
                    target,
                    bronze,
                    spec.bronze_partitions,
                    input_size_bytes=staged_object.size_bytes,
                )
                committed_sources.add(spec.name)
            elif replay_batch:
                replace_batch(
                    spark,
                    target,
                    bronze,
                    batch_id=batch_id,
                    input_size_bytes=staged_object.size_bytes,
                )
                committed_sources.add(spec.name)
            else:
                try:
                    append_batch(
                        spark,
                        target,
                        bronze,
                        batch_id=batch_id,
                        input_size_bytes=staged_object.size_bytes,
                    )
                    committed_sources.add(spec.name)
                except BatchAlreadyExistsError:
                    replace_batch(
                        spark,
                        target,
                        bronze,
                        batch_id=batch_id,
                        input_size_bytes=staged_object.size_bytes,
                    )
                    committed_sources.add(spec.name)

            finish_ingestion_batch(
                spark,
                run_id=audit_run_id,
                batch_id=batch_id,
                source_file=spec.csv_file,
                accepted_count=row_count,
                rejected_count=0,
                status="SUCCESS",
                completed_at=datetime.now(timezone.utc),
                checksum="",
                source_row_count=row_count,
                source_row_hash="",
                target_committed=True,
                namespace=control_namespace,
            )
            started_sources.remove(spec.name)
            total_output += row_count
            log_event(
                LOGGER,
                "bronze_table_succeeded",
                run_id=run_id,
                batch_id=batch_id,
                table=spec.name,
                rows=row_count,
            )

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
        log_event(
            LOGGER,
            "bronze_job_succeeded",
            run_id=run_id,
            batch_id=batch_id,
            total_rows=total_output,
        )

    except Exception as exc:
        summary = f"{type(exc).__name__}: {exc}"
        cleanup_failures = _finish_failed_audits(
            spark=spark,
            specs=specs,
            started_sources=started_sources,
            source_counts=source_counts,
            committed_sources=committed_sources,
            run_id=audit_run_id,
            batch_id=batch_id,
            total_input=total_input,
            total_output=total_output,
            summary=summary,
            control_namespace=control_namespace,
        )
        if cleanup_failures:
            cleanup_summary = truncate_error_summary(
                "Audit cleanup failed: " + "; ".join(cleanup_failures)
            )
            assert cleanup_summary is not None
            exc.add_note(cleanup_summary)
            try:
                log_event(
                    LOGGER,
                    "bronze_job_cleanup_failed",
                    run_id=run_id,
                    batch_id=batch_id,
                    error_summary=cleanup_summary,
                )
            except BaseException as logging_exc:
                exc.add_note(_cleanup_failure("log_cleanup_failures", logging_exc))
        try:
            log_event(
                LOGGER,
                "bronze_job_failed",
                run_id=run_id,
                batch_id=batch_id,
                error_summary=summary[:2_000],
            )
        except BaseException as logging_exc:
            exc.add_note(_cleanup_failure("log_job_failure", logging_exc))
        raise


def _build_parser() -> argparse.ArgumentParser:
    """Build standard English CLI parser with zero-config defaults."""
    now_utc = datetime.now(timezone.utc)
    default_run_id = os.environ.get("RUN_ID", f"bronze_{now_utc.strftime('%Y%m%d_%H%M%S')}")
    default_batch_id = os.environ.get("BATCH_ID", f"batch_{now_utc.strftime('%Y%m%d')}")

    parser = argparse.ArgumentParser(
        description="Ingest staged Olist CSV files into Apache Iceberg Bronze layer."
    )
    parser.add_argument(
        "--run-id",
        default=default_run_id,
        help=f"Pipeline run identifier. Default: env RUN_ID or current timestamp ({default_run_id}).",
    )
    parser.add_argument(
        "--batch-id",
        default=default_batch_id,
        help=f"Source batch identifier. Default: env BATCH_ID or current date ({default_batch_id}).",
    )
    parser.add_argument(
        "--mode",
        choices=("full", "incremental"),
        default="full",
        help="Ingestion mode: full (create/replace) or incremental (append). Default: full.",
    )
    parser.add_argument(
        "--replay-batch",
        action="store_true",
        help="Replace only rows of the specified batch (requires --mode incremental).",
    )
    parser.add_argument(
        "--attempt-id",
        default="1",
        help="Pipeline attempt identifier. Default: 1.",
    )
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        help="Specific table to ingest (repeatable). Ingests all tables if omitted.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run Bronze ingestion CLI."""
    args = _build_parser().parse_args(argv)
    configure_logging()
    settings = _runtime_settings()
    spark = build_spark_session(settings, f"olist-bronze-{args.run_id}")
    try:
        run_bronze_job(
            spark=spark,
            settings=settings,
            run_id=args.run_id,
            attempt_id=args.attempt_id,
            batch_id=args.batch_id,
            mode=args.mode,
            replay_batch=args.replay_batch,
            table_names=args.tables,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# ==============================================================================
# LEGACY / OVER-ENGINEERED CODE (COMMENTED OUT FOR REFERENCE)
#
# Reason for removal:
# 1. Custom VietnameseArgumentParser: Replaced with standard Python argparse.
# 2. SourceRowEvidence & _count_source_rows: The original project collected all
#    _row_hash strings across millions of rows into driver memory using
#    collect_list(col("_row_hash")) to compute multiset SHA-256 digests.
#    In production Big Data systems, collecting millions of row hashes onto driver
#    RAM causes severe OutOfMemory (OOM) errors. Standard row counts and audit
#    lineage columns are significantly safer, faster, and idiomatic.
# ==============================================================================
#
# from transformation.common.vietnamese_argparse import VietnameseArgumentParser
#
# def _legacy_parser() -> argparse.ArgumentParser:
#     parser = VietnameseArgumentParser(description="Nạp dữ liệu Olist vào lớp Bronze.")
#     parser.add_argument("--run-id", required=True, help="Mã run của job.")
#     parser.add_argument("--attempt-id", default="1", help="Mã attempt của job.")
#     parser.add_argument("--batch-id", required=True, help="Mã batch nguồn.")
#     parser.add_argument(
#         "--mode",
#         choices=("full", "incremental"),
#         required=True,
#         help="Chế độ nạp dữ liệu.",
#     )
#     parser.add_argument(
#         "--replay-batch",
#         action="store_true",
#         help="Chỉ thay thế batch hiện có đã chọn; yêu cầu mode incremental.",
#     )
#     parser.add_argument(
#         "--table", action="append", dest="tables", help="Table nguồn cần nạp; có thể lặp lại."
#     )
#     return parser
#
# @dataclass(frozen=True)
# class SourceRowEvidence:
#     row_count: int
#     row_hash: str
#
# def source_row_evidence(frame: DataFrame) -> SourceRowEvidence:
#     row = frame.agg(
#         F.count("*").alias("row_count"),
#         F.sha2(
#             F.concat_ws("", F.sort_array(F.collect_list(F.col("_row_hash")))),
#             256,
#         ).alias("row_hash"),
#     ).first()
#     if row is None:
#         raise RuntimeError("Phép tổng hợp bằng chứng hàng Bronze không trả về kết quả")
#     return SourceRowEvidence(row_count=int(row.row_count), row_hash=str(row.row_hash))
