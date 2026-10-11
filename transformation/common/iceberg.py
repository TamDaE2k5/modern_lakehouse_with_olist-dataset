"""Shared Iceberg table creation and batch-safe write operations."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from hashlib import sha256
from typing import Any
from uuid import uuid4

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F


TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024
MIN_OUTPUT_TASKS = 1
MAX_OUTPUT_TASKS = 32
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TRANSFORM = re.compile(r"^(days|years)\(([A-Za-z_][A-Za-z0-9_]*)\)$")
_BUCKET_TRANSFORM = re.compile(r"^bucket\(([1-9][0-9]*),\s*([A-Za-z_][A-Za-z0-9_]*)\)$")


class BatchAlreadyExistsError(RuntimeError):
    """Raised when an append would replay an existing batch."""


class BatchCommitConfirmedAfterError(RuntimeError):
    """Raised when exact locked readback proves a write committed before its response failed."""


class BatchLockCleanupError(RuntimeError):
    """Raised when JDBC lock cleanup fails after the protected operation succeeds."""


def _batch_lock_key(qualified_table: str, batch_id: str) -> int:
    """Return a stable signed PostgreSQL advisory-lock key for a table batch."""
    digest = sha256(f"{qualified_table}\0{batch_id}".encode()).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def _cleanup_summary(failures: Sequence[str]) -> str:
    return "batch ownership lock cleanup failed during: " + ", ".join(failures)


def _close_jdbc_resource(resource: Any | None, phase: str, failures: list[str]) -> None:
    if resource is None:
        return
    try:
        resource.close()
    except BaseException:
        failures.append(phase)


def _release_batch_lock(connection: Any, lock_key: int, failures: list[str]) -> None:
    statement = None
    result = None
    try:
        try:
            statement = connection.prepareStatement("SELECT pg_advisory_unlock(?)")
        except BaseException:
            failures.append("unlock_prepare")
            return
        try:
            statement.setLong(1, lock_key)
        except BaseException:
            failures.append("unlock_bind")
            return
        try:
            result = statement.executeQuery()
        except BaseException:
            failures.append("unlock_execute")
            return
        try:
            result.next()
        except BaseException:
            failures.append("unlock_result_read")
    finally:
        _close_jdbc_resource(result, "unlock_result_close", failures)
        _close_jdbc_resource(statement, "unlock_statement_close", failures)


@contextmanager
def _batch_ownership_lock(
    spark: SparkSession, qualified_table: str, batch_id: str
) -> Iterator[None]:
    """Serialize ownership decisions for one table batch in the catalog database."""
    catalog = qualified_table.split(".", maxsplit=1)[0]
    prefix = f"spark.sql.catalog.{catalog}"
    jvm = spark._jvm  # noqa: SLF001
    if jvm is None:
        raise RuntimeError("Spark JVM is not available for batch ownership lock")
    connection = jvm.java.sql.DriverManager.getConnection(
        spark.conf.get(f"{prefix}.uri"),
        spark.conf.get(f"{prefix}.jdbc.user"),
        spark.conf.get(f"{prefix}.jdbc.password"),
    )
    lock_key = _batch_lock_key(qualified_table, batch_id)
    locked = False
    primary_error: BaseException | None = None
    cleanup_failures: list[str] = []
    try:
        statement = None
        result = None
        try:
            statement = connection.prepareStatement("SELECT pg_advisory_lock(?)")
            statement.setLong(1, lock_key)
            result = statement.executeQuery()
            result.next()
            locked = True
        finally:
            _close_jdbc_resource(result, "lock_result_close", cleanup_failures)
            _close_jdbc_resource(statement, "lock_statement_close", cleanup_failures)
        if cleanup_failures:
            raise BatchLockCleanupError(_cleanup_summary(cleanup_failures)) from None
        yield
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if locked:
            _release_batch_lock(connection, lock_key, cleanup_failures)
        _close_jdbc_resource(connection, "connection_close", cleanup_failures)
        if cleanup_failures:
            summary = _cleanup_summary(cleanup_failures)
            if primary_error is None:
                raise BatchLockCleanupError(summary) from None
            primary_error.add_note(summary)


@contextmanager
def advisory_ownership_lock(
    spark: SparkSession,
    *,
    catalog: str,
    scope: str,
    identity: str,
) -> Iterator[None]:
    """Serialize one catalog-scoped ownership decision with PostgreSQL advisory locking."""
    with _batch_ownership_lock(spark, f"{catalog}.{scope}", identity):
        yield


def output_task_count(input_size_bytes: int) -> int:
    """Calculate a deterministic output task count bounded for local execution."""
    if input_size_bytes < 0:
        raise ValueError("input_size_bytes cannot be negative")
    estimated = max(MIN_OUTPUT_TASKS, math.ceil(input_size_bytes / TARGET_FILE_SIZE_BYTES))
    return min(estimated, MAX_OUTPUT_TASKS)


def _quoted_identifier(identifier: str) -> str:
    parts = identifier.split(".")
    if len(parts) < 2 or any(not _IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError(f"Invalid fully qualified identifier: {identifier}")
    return ".".join(f"`{part}`" for part in parts)


def _quoted_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _partition_expression(transform: str) -> Column:
    match = _TRANSFORM.fullmatch(transform)
    if match:
        function, column = match.groups()
        return F.days(F.col(column)) if function == "days" else F.years(F.col(column))
    bucket_match = _BUCKET_TRANSFORM.fullmatch(transform)
    if bucket_match:
        buckets, column = bucket_match.groups()
        return F.bucket(int(buckets), F.col(column))
    raise ValueError(f"Unsupported Iceberg partition transform: {transform}")


def ensure_namespace(spark: SparkSession, qualified_table: str) -> None:
    """Create the catalog namespace containing a qualified table when absent."""
    parts = qualified_table.split(".")
    _quoted_identifier(qualified_table)
    namespace = ".".join(parts[:-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_quoted_identifier(namespace)}")


def _bounded(df: DataFrame, input_size_bytes: int) -> DataFrame:
    return df.repartition(output_task_count(input_size_bytes))


def _validate_batch(df: DataFrame, batch_id: str) -> None:
    if "_batch_id" not in df.columns:
        raise ValueError("Bronze DataFrame must contain _batch_id column")
    if df.where(F.col("_batch_id").isNull() | (F.col("_batch_id") != batch_id)).limit(1).count():
        raise ValueError("Bronze DataFrame contains rows outside the requested batch")


def create_or_replace(
    spark: SparkSession,
    qualified_table: str,
    df: DataFrame,
    partitions: Sequence[str],
    *,
    input_size_bytes: int,
) -> None:
    """Create or atomically replace an Iceberg v2 table with declared transforms."""
    ensure_namespace(spark, qualified_table)
    writer = (
        _bounded(df, input_size_bytes)
        .writeTo(_quoted_identifier(qualified_table))
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.distribution-mode", "hash")
        .tableProperty("write.target-file-size-bytes", str(TARGET_FILE_SIZE_BYTES))
    )
    if partitions:
        writer = writer.partitionedBy(*(_partition_expression(item) for item in partitions))
    writer.createOrReplace()


def append_batch(
    spark: SparkSession,
    qualified_table: str,
    df: DataFrame,
    *,
    batch_id: str,
    input_size_bytes: int,
    commit_readback: Callable[[], bool] | None = None,
) -> None:
    """Append a new batch and reject any replay before it can duplicate rows."""
    table = _quoted_identifier(qualified_table)
    _validate_batch(df, batch_id)
    with _batch_ownership_lock(spark, qualified_table, batch_id):
        existing = spark.sql(
            f"SELECT 1 FROM {table} WHERE `_batch_id` = {_quoted_literal(batch_id)} LIMIT 1"
        )
        if existing.count():
            raise BatchAlreadyExistsError(
                f"batch {batch_id!r} already exists in {qualified_table}; "
                "use explicit replace_batch"
            )
        try:
            _bounded(df, input_size_bytes).writeTo(table).append()
        except BaseException as exc:
            if commit_readback is not None and commit_readback():
                raise BatchCommitConfirmedAfterError(
                    f"batch {batch_id!r} committed before append response failed"
                ) from exc
            raise


def replace_batch(
    spark: SparkSession,
    qualified_table: str,
    df: DataFrame,
    *,
    batch_id: str,
    input_size_bytes: int,
    ownership_check: Callable[[], None] | None = None,
) -> None:
    """Atomically replace only rows belonging to the requested batch."""
    table = _quoted_identifier(qualified_table)
    _validate_batch(df, batch_id)
    with _batch_ownership_lock(spark, qualified_table, batch_id):
        if ownership_check is not None:
            ownership_check()
        source_view = f"bronze_batch_replacement_{uuid4().hex}"
        _bounded(df, input_size_bytes).createOrReplaceTempView(source_view)
        try:
            spark.sql(
                f"MERGE INTO {table} AS target "
                f"USING `{source_view}` AS source ON FALSE "
                "WHEN NOT MATCHED THEN INSERT * "
                "WHEN NOT MATCHED BY SOURCE AND "
                f"target.`_batch_id` = {_quoted_literal(batch_id)} THEN DELETE"
            )
        finally:
            spark.catalog.dropTempView(source_view)
