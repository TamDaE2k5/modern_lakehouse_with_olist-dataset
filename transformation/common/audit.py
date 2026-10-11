"""Durable Iceberg audit state transitions for pipeline and ingestion runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformation.common.iceberg import advisory_ownership_lock


MAX_ERROR_SUMMARY_LENGTH = 2_000
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TERMINAL_STATUSES = {"SUCCESS", "FAILED", "UNKNOWN"}


@dataclass(frozen=True)
class PipelineRunOutcome:
    """Terminal fields that must match after an ambiguous pipeline audit write."""

    status: str
    input_count: int | None
    output_count: int | None
    rejected_count: int | None
    error_summary: str | None


@dataclass(frozen=True)
class PipelineRunRecord:
    """Complete identity and outcome used for ambiguous-start reconciliation."""

    run_id: str
    batch_id: str
    job_name: str
    layer: str
    mode: str
    status: str
    input_count: int | None
    output_count: int | None
    rejected_count: int | None
    error_summary: str | None
    logical_run_id: str
    attempt_id: str
    original_status: str | None
    original_error_summary: str | None
    resolution_note: str | None
    resolution_evidence_json: str | None
    resolved_at: datetime | None


@dataclass(frozen=True)
class PipelineResolutionEvidence:
    """Exact write/readback evidence permitted to resolve one terminal attempt."""

    logical_run_id: str
    attempt_id: str
    batch_id: str
    job_name: str
    layer: str
    mode: str
    committed: bool
    input_count: int
    output_count: int
    rejected_count: int
    proof: str


class PipelineDuplicateError(RuntimeError):
    """Raised when one pipeline run identity maps to multiple audit rows."""


PIPELINE_RUN_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("logical_run_id", StringType(), False),
        StructField("attempt_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("job_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("mode", StringType(), False),
        StructField("input_count", LongType(), True),
        StructField("output_count", LongType(), True),
        StructField("rejected_count", LongType(), True),
        StructField("status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("error_summary", StringType(), True),
        StructField("original_status", StringType(), True),
        StructField("original_error_summary", StringType(), True),
        StructField("resolution_note", StringType(), True),
        StructField("resolution_evidence_json", StringType(), True),
        StructField("resolved_at", TimestampType(), True),
    ]
)

INGESTION_BATCH_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("checksum", StringType(), False),
        StructField("source_row_count", LongType(), False),
        StructField("source_row_hash", StringType(), True),
        StructField("target_committed", BooleanType(), True),
        StructField("accepted_count", LongType(), True),
        StructField("rejected_count", LongType(), True),
        StructField("status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("error_summary", StringType(), True),
    ]
)

TRANSFORMATION_TARGET_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("target_name", StringType(), False),
        StructField("mode", StringType(), False),
        StructField("input_count", LongType(), False),
        StructField("accepted_count", LongType(), True),
        StructField("rejected_count", LongType(), True),
        StructField("accepted_committed", BooleanType(), False),
        StructField("quarantine_committed", BooleanType(), False),
        StructField("status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("error_summary", StringType(), True),
    ]
)


def truncate_error_summary(error_summary: str | None) -> str | None:
    """Bound failure text stored in audit tables."""
    return None if error_summary is None else error_summary[:MAX_ERROR_SUMMARY_LENGTH]


def attempt_run_id(logical_run_id: str, attempt_id: str) -> str:
    """Return the unique audit identity for a stable logical run attempt."""
    if not logical_run_id or not attempt_id or re.fullmatch(r"[A-Za-z0-9_.:-]+", attempt_id) is None:
        raise ValueError("Attempt identifier must contain only safe characters")
    return f"{logical_run_id}.attempt-{attempt_id}"


def _name(catalog: str | None, namespace: str, table: str | None = None) -> str:
    if catalog is None:
        raise ValueError("Spark default catalog is not configured")
    parts = [catalog, namespace] if table is None else [catalog, namespace, table]
    if any(not _IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError(f"Invalid audit identifier: {'.'.join(parts)}")
    return ".".join(f"`{part}`" for part in parts)


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _timestamp_literal(value: datetime) -> str:
    return f"TIMESTAMP '{_utc_naive(value).isoformat(sep=' ', timespec='microseconds')}'"


def evolve_table_schema(
    spark: SparkSession,
    *,
    table: str,
    expected_schema: StructType,
    legacy_backfills: dict[str, str] | None = None,
) -> None:
    """Add newly introduced nullable columns and backfill legacy identity fields."""
    existing_fields = {field.name for field in spark.table(table).schema.fields}
    for field in expected_schema.fields:
        if field.name not in existing_fields:
            spark.sql(
                f"ALTER TABLE {table} ADD COLUMN `{field.name}` "
                f"{field.dataType.simpleString()}"
            )
    for column, expression in (legacy_backfills or {}).items():
        missing = spark.sql(
            f"SELECT 1 AS `found` FROM {table} WHERE `{column}` IS NULL LIMIT 1"
        ).first()
        if missing is not None:
            spark.sql(
                f"UPDATE {table} SET `{column}` = {expression} WHERE `{column}` IS NULL"
            )


def ensure_audit_tables(
    spark: SparkSession,
    *,
    namespace: str = "control",
    include_transformation_targets: bool = False,
) -> None:
    """Create format-v2 control tables without replacing existing audit history."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_name(catalog, namespace)}")
    pipeline_columns = ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}"
        + (" NOT NULL" if not field.nullable else "")
        for field in PIPELINE_RUN_SCHEMA.fields
    )
    batch_columns = ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}"
        + (" NOT NULL" if not field.nullable else "")
        for field in INGESTION_BATCH_SCHEMA.fields
    )
    target_columns = ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}"
        + (" NOT NULL" if not field.nullable else "")
        for field in TRANSFORMATION_TARGET_SCHEMA.fields
    )
    tables: tuple[tuple[str, str, str], ...] = (
        ("pipeline_runs", pipeline_columns, "one row per pipeline run"),
        (
            "ingestion_batches",
            batch_columns,
            "one row per pipeline run, batch, and source file",
        ),
    )
    if include_transformation_targets:
        tables += (
            (
                "transformation_targets",
                target_columns,
                "one row per pipeline run and transformation target",
            ),
        )
    for table, columns, grain in tables:
        qualified_table = _name(catalog, namespace, table)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {qualified_table} ({columns}) "
            "USING iceberg TBLPROPERTIES ("
            "'format-version'='2', "
            f"'grain'={_literal(grain)}, "
            "'write.distribution-mode'='hash', "
            "'write.target-file-size-bytes'='134217728')"
        )
        if table == "pipeline_runs":
            evolve_table_schema(
                spark,
                table=qualified_table,
                expected_schema=PIPELINE_RUN_SCHEMA,
                legacy_backfills={
                    "logical_run_id": "`run_id`",
                    "attempt_id": "'legacy'",
                },
            )
        elif table == "ingestion_batches":
            evolve_table_schema(
                spark,
                table=qualified_table,
                expected_schema=INGESTION_BATCH_SCHEMA,
            )


def _assert_absent(spark: SparkSession, table: str, condition: str, message: str) -> None:
    if spark.sql(f"SELECT 1 FROM {table} WHERE {condition} LIMIT 1").count():
        raise RuntimeError(message)


def start_pipeline_run(
    spark: SparkSession,
    *,
    run_id: str,
    batch_id: str,
    job_name: str,
    layer: str,
    mode: str,
    started_at: datetime,
    logical_run_id: str | None = None,
    attempt_id: str = "1",
    namespace: str = "control",
) -> PipelineRunRecord:
    """Persist a unique STARTED pipeline state before processing begins."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "pipeline_runs")
    row = (
        run_id,
        run_id if logical_run_id is None else logical_run_id,
        attempt_id,
        batch_id,
        job_name,
        layer,
        mode,
        None,
        None,
        None,
        "STARTED",
        _utc_naive(started_at),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    expected_identity = {
        "run_id": run_id,
        "logical_run_id": run_id if logical_run_id is None else logical_run_id,
        "attempt_id": attempt_id,
        "batch_id": batch_id,
        "job_name": job_name,
        "layer": layer,
        "mode": mode,
    }

    def exact(records: tuple[PipelineRunRecord, ...]) -> PipelineRunRecord | None:
        if not records:
            return None
        if len(records) > 1:
            raise _pipeline_duplicate(run_id, records)
        observed = records[0]
        mismatches = [
            name
            for name, value in expected_identity.items()
            if getattr(observed, name) != value
        ]
        if mismatches:
            raise RuntimeError("Pipeline start identity mismatch: " + ", ".join(mismatches))
        return observed

    with advisory_ownership_lock(
        spark,
        catalog=str(catalog),
        scope=f"{namespace}.pipeline_runs",
        identity=run_id,
    ):
        observed = exact(pipeline_run_records(spark, run_id=run_id, namespace=namespace))
        if observed is not None:
            return observed
        append_error: BaseException | None = None
        try:
            spark.createDataFrame([row], PIPELINE_RUN_SCHEMA).writeTo(table).append()
        except BaseException as exc:
            append_error = exc
        observed = exact(pipeline_run_records(spark, run_id=run_id, namespace=namespace))
        if observed is not None:
            return observed
        if append_error is not None:
            raise append_error
        raise RuntimeError("Exact readback failed after appending pipeline start")


def start_ingestion_batch(
    spark: SparkSession,
    *,
    run_id: str,
    batch_id: str,
    source_file: str,
    checksum: str,
    source_row_count: int,
    started_at: datetime,
    namespace: str = "control",
) -> None:
    """Persist a unique STARTED source-batch state."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "ingestion_batches")
    condition = (
        f"`run_id` = {_literal(run_id)} AND `batch_id` = {_literal(batch_id)} "
        f"AND `source_file` = {_literal(source_file)}"
    )
    _assert_absent(spark, table, condition, f"ingestion batch already exists: {source_file}")
    row = (
        run_id,
        batch_id,
        source_file,
        checksum,
        source_row_count,
        "",
        False,
        None,
        None,
        "STARTED",
        _utc_naive(started_at),
        None,
        None,
        None,
    )
    spark.createDataFrame([row], INGESTION_BATCH_SCHEMA).writeTo(table).append()


def start_transformation_target(
    spark: SparkSession,
    *,
    run_id: str,
    batch_id: str,
    target_name: str,
    mode: str,
    input_count: int,
    started_at: datetime,
    namespace: str = "control",
) -> None:
    """Persist a unique STARTED state for one transformation target."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "transformation_targets")
    condition = f"`run_id` = {_literal(run_id)} AND `target_name` = {_literal(target_name)}"
    _assert_absent(
        spark,
        table,
        condition,
        f"transformation target already exists: {run_id}/{target_name}",
    )
    row = (
        run_id,
        batch_id,
        target_name,
        mode,
        input_count,
        None,
        None,
        False,
        False,
        "STARTED",
        _utc_naive(started_at),
        None,
        None,
        None,
    )
    spark.createDataFrame([row], TRANSFORMATION_TARGET_SCHEMA).writeTo(table).append()


def started_ingestion_sources(
    spark: SparkSession,
    *,
    run_id: str,
    batch_id: str,
    namespace: str = "control",
) -> tuple[str, ...]:
    """Read persisted source files that still require a terminal audit transition."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "ingestion_batches")
    rows = spark.sql(
        f"SELECT `source_file` FROM {table} "
        f"WHERE `run_id` = {_literal(run_id)} "
        f"AND `batch_id` = {_literal(batch_id)} AND `status` = 'STARTED'"
    ).collect()
    return tuple(row.source_file for row in rows)


def started_transformation_targets(
    spark: SparkSession,
    *,
    run_id: str,
    namespace: str = "control",
) -> tuple[str, ...]:
    """Read persisted transformation targets still requiring terminalization."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "transformation_targets")
    rows = spark.sql(
        f"SELECT `target_name` FROM {table} WHERE `run_id` = {_literal(run_id)} "
        "AND `status` = 'STARTED' ORDER BY `target_name`"
    ).collect()
    return tuple(row.target_name for row in rows)


def transformation_target_status(
    spark: SparkSession,
    *,
    run_id: str,
    target_name: str,
    namespace: str = "control",
) -> str | None:
    """Read one persisted transformation-target status for reconciliation."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "transformation_targets")
    row = spark.sql(
        f"SELECT `status` FROM {table} WHERE `run_id` = {_literal(run_id)} "
        f"AND `target_name` = {_literal(target_name)} LIMIT 1"
    ).first()
    return None if row is None else str(row.status)


def pipeline_run_status(
    spark: SparkSession,
    *,
    run_id: str,
    namespace: str = "control",
) -> str | None:
    """Read one persisted pipeline status for terminalization readback."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "pipeline_runs")
    row = spark.sql(
        f"SELECT `status` FROM {table} WHERE `run_id` = {_literal(run_id)} LIMIT 1"
    ).first()
    return None if row is None else str(row.status)


def pipeline_run_outcome(
    spark: SparkSession,
    *,
    run_id: str,
    namespace: str = "control",
) -> PipelineRunOutcome | None:
    """Read the exact terminal fields used to reconcile a pipeline audit write."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "pipeline_runs")
    row = spark.sql(
        "SELECT `status`, `input_count`, `output_count`, `rejected_count`, "
        f"`error_summary` FROM {table} WHERE `run_id` = {_literal(run_id)} LIMIT 1"
    ).first()
    if row is None:
        return None
    return PipelineRunOutcome(
        status=str(row.status),
        input_count=row.input_count,
        output_count=row.output_count,
        rejected_count=row.rejected_count,
        error_summary=row.error_summary,
    )


def _pipeline_record_from_row(row: object) -> PipelineRunRecord:
    return PipelineRunRecord(
        run_id=str(row.run_id),  # type: ignore[attr-defined]
        batch_id=str(row.batch_id),  # type: ignore[attr-defined]
        job_name=str(row.job_name),  # type: ignore[attr-defined]
        layer=str(row.layer),  # type: ignore[attr-defined]
        mode=str(row.mode),  # type: ignore[attr-defined]
        status=str(row.status),  # type: ignore[attr-defined]
        input_count=row.input_count,  # type: ignore[attr-defined]
        output_count=row.output_count,  # type: ignore[attr-defined]
        rejected_count=row.rejected_count,  # type: ignore[attr-defined]
        error_summary=row.error_summary,  # type: ignore[attr-defined]
        logical_run_id=str(row.logical_run_id),  # type: ignore[attr-defined]
        attempt_id=str(row.attempt_id),  # type: ignore[attr-defined]
        original_status=row.original_status,  # type: ignore[attr-defined]
        original_error_summary=row.original_error_summary,  # type: ignore[attr-defined]
        resolution_note=row.resolution_note,  # type: ignore[attr-defined]
        resolution_evidence_json=row.resolution_evidence_json,  # type: ignore[attr-defined]
        resolved_at=row.resolved_at,  # type: ignore[attr-defined]
    )


def pipeline_run_records(
    spark: SparkSession,
    *,
    run_id: str,
    namespace: str = "control",
) -> tuple[PipelineRunRecord, ...]:
    """Read the complete persisted set for one pipeline run identity."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "pipeline_runs")
    rows = spark.sql(
        "SELECT `run_id`, `logical_run_id`, `attempt_id`, `batch_id`, `job_name`, "
        "`layer`, `mode`, `status`, `input_count`, `output_count`, `rejected_count`, "
        "`error_summary`, `original_status`, `original_error_summary`, `resolution_note`, "
        "`resolution_evidence_json`, `resolved_at` "
        f"FROM {table} WHERE `run_id` = {_literal(run_id)} "
        "ORDER BY `logical_run_id`, `attempt_id`, `batch_id`, `job_name`, `layer`, `mode`"
    ).collect()
    return tuple(_pipeline_record_from_row(row) for row in rows)


def _pipeline_duplicate(
    run_id: str, records: tuple[PipelineRunRecord, ...]
) -> PipelineDuplicateError:
    identities = sorted(
        {
            ":".join(
                (
                    record.logical_run_id[:40],
                    record.attempt_id[:20],
                    record.batch_id[:40],
                    record.job_name[:40],
                    record.status[:20],
                )
            )
            for record in records
        }
    )
    return PipelineDuplicateError(
        f"pipeline audit duplicate run_id={run_id[:80]} count={len(records)} "
        f"identities=[{','.join(identities[:5])}]"
    )


def pipeline_run_record(
    spark: SparkSession,
    *,
    run_id: str,
    namespace: str = "control",
) -> PipelineRunRecord | None:
    """Read one pipeline row and fail closed on all legacy duplicates."""
    records = pipeline_run_records(spark, run_id=run_id, namespace=namespace)
    if len(records) > 1:
        raise _pipeline_duplicate(run_id, records)
    return None if not records else records[0]


def resolve_pipeline_run(
    spark: SparkSession,
    *,
    run_id: str,
    status: str,
    evidence: PipelineResolutionEvidence,
    resolution_note: str,
    resolved_at: datetime,
    namespace: str = "control",
) -> PipelineRunRecord:
    """Evidence-gate one immutable FAILED/UNKNOWN terminal resolution."""
    if status not in {"SUCCESS", "FAILED"}:
        raise ValueError("Resolution status must be SUCCESS or FAILED")
    if not resolution_note.strip() or not evidence.proof.strip():
        raise ValueError("Resolution note and proof are required")
    current = pipeline_run_record(spark, run_id=run_id, namespace=namespace)
    if current is None:
        raise RuntimeError("Pipeline audit row does not exist")
    expected_identity = {
        "logical_run_id": evidence.logical_run_id,
        "attempt_id": evidence.attempt_id,
        "batch_id": evidence.batch_id,
        "job_name": evidence.job_name,
        "layer": evidence.layer,
        "mode": evidence.mode,
    }
    mismatches = [
        name for name, expected in expected_identity.items() if getattr(current, name) != expected
    ]
    if mismatches:
        raise RuntimeError("Pipeline resolution identity mismatch: " + ", ".join(mismatches))
    if current.resolution_note is not None:
        expected_evidence = json.dumps(evidence.__dict__, sort_keys=True, separators=(",", ":"))
        if current.status == status and current.resolution_evidence_json == expected_evidence:
            return current
        raise RuntimeError("Pipeline resolution history is immutable")
    if current.status not in {"FAILED", "UNKNOWN"}:
        raise RuntimeError("Pipeline resolution requires FAILED or UNKNOWN status")
    if status == "SUCCESS" and not evidence.committed:
        raise RuntimeError("Resolving to SUCCESS requires exact commit evidence")
    if status == "FAILED" and (current.status != "UNKNOWN" or evidence.committed):
        raise RuntimeError("UNKNOWN can only resolve to FAILED with proof of uncommitted state")
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "pipeline_runs")
    evidence_json = json.dumps(evidence.__dict__, sort_keys=True, separators=(",", ":"))
    summary = current.error_summary
    spark.sql(
        f"UPDATE {table} SET `status` = {_literal(status)}, "
        f"`input_count` = {evidence.input_count}, `output_count` = {evidence.output_count}, "
        f"`rejected_count` = {evidence.rejected_count}, "
        f"`original_status` = {_literal(current.status)}, "
        "`original_error_summary` = "
        + ("NULL" if summary is None else _literal(summary))
        + f", `resolution_note` = {_literal(resolution_note)}, "
        f"`resolution_evidence_json` = {_literal(evidence_json)}, "
        f"`resolved_at` = {_timestamp_literal(resolved_at)} "
        f"WHERE `run_id` = {_literal(run_id)} AND `status` = {_literal(current.status)} "
        "AND `resolution_note` IS NULL"
    )
    observed = pipeline_run_record(spark, run_id=run_id, namespace=namespace)
    if observed is None or observed.status != status or observed.resolution_evidence_json != evidence_json:
        raise RuntimeError("Exact readback of pipeline resolution failed")
    return observed


def _finish(
    spark: SparkSession,
    *,
    table: str,
    condition: str,
    assignments: dict[str, str],
    status: str,
) -> None:
    if status not in _TERMINAL_STATUSES:
        raise ValueError(f"Invalid terminal audit status: {status}")
    assignment_sql = ", ".join(f"`{name}` = {value}" for name, value in assignments.items())
    spark.sql(f"UPDATE {table} SET {assignment_sql} WHERE {condition} AND `status` = 'STARTED'")
    terminal = spark.sql(f"SELECT `status` FROM {table} WHERE {condition} LIMIT 1").first()
    if terminal is None or terminal.status != status:
        raise RuntimeError("Audit state transition requires exactly one STARTED row")


def finish_pipeline_run(
    spark: SparkSession,
    *,
    run_id: str,
    input_count: int,
    output_count: int,
    rejected_count: int,
    status: str,
    completed_at: datetime,
    error_summary: str | None = None,
    namespace: str = "control",
) -> None:
    """Transition one pipeline audit row from STARTED to a terminal status."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "pipeline_runs")
    completed = _timestamp_literal(completed_at)
    summary = truncate_error_summary(error_summary)
    _finish(
        spark,
        table=table,
        condition=f"`run_id` = {_literal(run_id)}",
        status=status,
        assignments={
            "input_count": str(input_count),
            "output_count": str(output_count),
            "rejected_count": str(rejected_count),
            "status": _literal(status),
            "completed_at": completed,
            "duration_ms": f"CAST((unix_timestamp({completed}) - unix_timestamp(`started_at`)) * 1000 AS BIGINT)",
            "error_summary": "NULL" if summary is None else _literal(summary),
        },
    )


def finish_transformation_target(
    spark: SparkSession,
    *,
    run_id: str,
    target_name: str,
    accepted_count: int | None,
    rejected_count: int | None,
    accepted_committed: bool,
    quarantine_committed: bool,
    status: str,
    completed_at: datetime,
    error_summary: str | None = None,
    namespace: str = "control",
) -> None:
    """Transition one transformation target from STARTED to a terminal state."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "transformation_targets")
    completed = _timestamp_literal(completed_at)
    summary = truncate_error_summary(error_summary)
    condition = f"`run_id` = {_literal(run_id)} AND `target_name` = {_literal(target_name)}"
    _finish(
        spark,
        table=table,
        condition=condition,
        status=status,
        assignments={
            "accepted_count": "NULL" if accepted_count is None else str(accepted_count),
            "rejected_count": "NULL" if rejected_count is None else str(rejected_count),
            "accepted_committed": str(accepted_committed).upper(),
            "quarantine_committed": str(quarantine_committed).upper(),
            "status": _literal(status),
            "completed_at": completed,
            "duration_ms": f"CAST((unix_timestamp({completed}) - unix_timestamp(`started_at`)) * 1000 AS BIGINT)",
            "error_summary": "NULL" if summary is None else _literal(summary),
        },
    )


def finish_ingestion_batch(
    spark: SparkSession,
    *,
    run_id: str,
    batch_id: str,
    source_file: str,
    accepted_count: int,
    rejected_count: int,
    status: str,
    completed_at: datetime,
    error_summary: str | None = None,
    checksum: str | None = None,
    source_row_count: int | None = None,
    source_row_hash: str | None = None,
    target_committed: bool | None = None,
    namespace: str = "control",
) -> None:
    """Transition one source-batch audit row from STARTED to a terminal status."""
    catalog = spark.conf.get("spark.sql.defaultCatalog")
    table = _name(catalog, namespace, "ingestion_batches")
    completed = _timestamp_literal(completed_at)
    summary = truncate_error_summary(error_summary)
    condition = (
        f"`run_id` = {_literal(run_id)} AND `batch_id` = {_literal(batch_id)} "
        f"AND `source_file` = {_literal(source_file)}"
    )
    assignments = {
        "accepted_count": str(accepted_count),
        "rejected_count": str(rejected_count),
        "status": _literal(status),
        "completed_at": completed,
        "duration_ms": f"CAST((unix_timestamp({completed}) - unix_timestamp(`started_at`)) * 1000 AS BIGINT)",
        "error_summary": "NULL" if summary is None else _literal(summary),
    }
    if checksum is not None:
        assignments["checksum"] = _literal(checksum)
    if source_row_count is not None:
        assignments["source_row_count"] = str(source_row_count)
    if source_row_hash is not None:
        assignments["source_row_hash"] = _literal(source_row_hash)
    if target_committed is not None:
        assignments["target_committed"] = "TRUE" if target_committed else "FALSE"
    _finish(
        spark,
        table=table,
        condition=condition,
        status=status,
        assignments=assignments,
    )
