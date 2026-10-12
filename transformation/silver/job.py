"""Silver pipeline orchestrator: validates Bronze data and produces Silver and Quarantine Iceberg tables."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import os
import re
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from transformation.common.audit import (
    attempt_run_id,
    ensure_audit_tables,
    finish_pipeline_run,
    finish_transformation_target,
    start_pipeline_run,
    start_transformation_target,
    truncate_error_summary,
)
from transformation.common.config import Settings
from transformation.common.iceberg import create_or_replace
from transformation.common.logging import configure_logging, log_event
from transformation.common.registry import get_table_spec
from transformation.common.spark_session import build_spark_session
from transformation.common.validation import TransformResult
from transformation.silver._common import normalized_zip
from transformation.silver.customers import transform_customers
from transformation.silver.geography import transform_geography
from transformation.silver.order_items import transform_order_items
from transformation.silver.orders import transform_orders
from transformation.silver.payments import transform_payments
from transformation.silver.products import transform_products
from transformation.silver.reviews import transform_reviews
from transformation.silver.sellers import transform_sellers

LOGGER = logging.getLogger("silver_job")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class SilverTableSpec:
    """Declare one Silver target, bronze source, dependencies, grain, keys, and partitioning."""

    name: str
    bronze_source: str
    dependencies: tuple[str, ...]
    grain: str
    key: tuple[str, ...]
    partitions: tuple[str, ...]
    transform: Callable[[Mapping[str, DataFrame]], TransformResult]


@dataclass
class _TargetProgress:
    """In-memory progress tracking for a target transformation."""

    input_count: int
    accepted_count: int | None = None
    rejected_count: int | None = None
    accepted_committed: bool = False
    quarantine_committed: bool = False
    phase: str = "transform"


SILVER_TABLES: dict[str, SilverTableSpec] = {
    "customers": SilverTableSpec(
        name="customers",
        bronze_source="customers",
        dependencies=("customers",),
        grain="one customer",
        key=("customer_id",),
        partitions=(),
        transform=lambda frames: transform_customers(frames["customers"]),
    ),
    "orders": SilverTableSpec(
        name="orders",
        bronze_source="orders",
        dependencies=("orders",),
        grain="one order",
        key=("order_id",),
        partitions=("years(order_purchase_timestamp)",),
        transform=lambda frames: transform_orders(frames["orders"]),
    ),
    "order_items": SilverTableSpec(
        name="order_items",
        bronze_source="order_items",
        dependencies=("order_items", "orders"),
        grain="one line in an order",
        key=("order_id", "order_item_id"),
        partitions=("years(order_purchase_timestamp)",),
        transform=lambda frames: transform_order_items(frames["order_items"], frames["orders"]),
    ),
    "order_payments": SilverTableSpec(
        name="order_payments",
        bronze_source="order_payments",
        dependencies=("order_payments", "orders"),
        grain="one payment component of an order",
        key=("order_id", "payment_sequential"),
        partitions=("years(order_purchase_timestamp)",),
        transform=lambda frames: transform_payments(frames["order_payments"], frames["orders"]),
    ),
    "order_reviews": SilverTableSpec(
        name="order_reviews",
        bronze_source="order_reviews",
        dependencies=("order_reviews",),
        grain="one review linked to one order",
        key=("review_id", "order_id"),
        partitions=("years(review_creation_date)",),
        transform=lambda frames: transform_reviews(frames["order_reviews"]),
    ),
    "products": SilverTableSpec(
        name="products",
        bronze_source="products",
        dependencies=("products", "product_category_translation"),
        grain="one product",
        key=("product_id",),
        partitions=(),
        transform=lambda frames: transform_products(
            frames["products"], frames["product_category_translation"]
        ),
    ),
    "sellers": SilverTableSpec(
        name="sellers",
        bronze_source="sellers",
        dependencies=("sellers",),
        grain="one seller",
        key=("seller_id",),
        partitions=(),
        transform=lambda frames: transform_sellers(frames["sellers"]),
    ),
    "geography": SilverTableSpec(
        name="geography",
        bronze_source="geolocation",
        dependencies=("geolocation",),
        grain="one Brazilian ZIP prefix centroid",
        key=("zip_code_prefix",),
        partitions=(),
        transform=lambda frames: transform_geography(frames["geolocation"]),
    ),
}


def _qualified(catalog: str, namespace: str, table: str) -> str:
    for part in (catalog, namespace, table):
        if not _IDENTIFIER.fullmatch(part):
            raise ValueError(f"Invalid identifier part: {part}")
    return f"{catalog}.{namespace}.{table}"


def _quoted(name: str) -> str:
    parts = name.split(".")
    for part in parts:
        if not _IDENTIFIER.fullmatch(part):
            raise ValueError(f"Invalid identifier: {name}")
    return ".".join(f"`{part}`" for part in parts)


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _selected_specs(table_names: Sequence[str] | None) -> tuple[SilverTableSpec, ...]:
    names = tuple(SILVER_TABLES) if table_names is None else tuple(table_names)
    if not names:
        raise ValueError("Must select at least one Silver table")
    if len(set(names)) != len(names):
        raise ValueError("Selected Silver tables must not contain duplicates")
    unknown = tuple(name for name in names if name not in SILVER_TABLES)
    if unknown:
        raise KeyError(f"Unknown Silver table: {unknown[0]}")
    return tuple(SILVER_TABLES[name] for name in names)


def _schema_errors(source: str, frame: DataFrame) -> tuple[str, ...]:
    expected = tuple(
        (column.name, column.spark_type)
        for column in get_table_spec(source).columns
    )
    actual = tuple(
        (field.name, field.dataType.simpleString())
        for field in frame.schema.fields
    )
    expected_by_name = dict(expected)
    actual_by_name = dict(actual)
    expected_names = tuple(name for name, _ in expected)
    actual_names = tuple(name for name, _ in actual)
    missing = sorted(set(expected_by_name) - set(actual_by_name))
    unexpected = sorted(set(actual_by_name) - set(expected_by_name))
    type_mismatches = sorted(
        f"{name}:{actual_by_name[name]}!={expected_by_name[name]}"
        for name in set(expected_by_name) & set(actual_by_name)
        if actual_by_name[name] != expected_by_name[name]
    )
    if not missing and not unexpected and not type_mismatches:
        return ()
    return (
        f"Schema mismatch for {source} (missing={missing}, unexpected={unexpected}, "
        f"type_mismatches={type_mismatches})",
    )


def _preflight_dependencies(
    spark: SparkSession,
    *,
    catalog: str,
    bronze_namespace: str,
    specs: Sequence[SilverTableSpec],
    mode: str,
    batch_id: str,
) -> dict[str, DataFrame]:
    dependencies = tuple(dict.fromkeys(dep for spec in specs for dep in spec.dependencies))
    frames: dict[str, DataFrame] = {}
    errors: list[str] = []
    for source in dependencies:
        name = _qualified(catalog, bronze_namespace, source)
        try:
            frame = spark.table(name)
        except Exception as exc:
            errors.append(f"{source}: Bronze table not available ({type(exc).__name__})")
            continue
        frames[source] = frame
        schema_errs = _schema_errors(source, frame)
        errors.extend(schema_errs)
        try:
            if frame.limit(1).count() == 0:
                errors.append(f"{source}: Bronze dependency is empty")
        except Exception as exc:
            errors.append(f"{source}: Failed to check Bronze content ({type(exc).__name__})")

    if mode == "incremental":
        main_sources = tuple(dict.fromkeys(spec.bronze_source for spec in specs))
        for source in main_sources:
            batch_frame = frames.get(source)
            if batch_frame is None:
                continue
            if "_batch_id" not in batch_frame.columns:
                errors.append(f"{source}: Missing _batch_id column for incremental mode")
                continue
            try:
                if batch_frame.where(F.col("_batch_id") == batch_id).limit(1).count() == 0:
                    errors.append(f"{source}: Batch {batch_id!r} has no rows")
            except Exception as exc:
                errors.append(f"{source}: Failed to check batch {batch_id!r} ({type(exc).__name__})")

    if errors:
        raise RuntimeError("Silver dependency preflight check failed: " + "; ".join(errors))
    return frames


def _table_exists(spark: SparkSession, name: str) -> bool:
    try:
        spark.table(name).limit(0).collect()
        return True
    except Exception:
        return False


def _set_contract_properties(
    spark: SparkSession,
    table: str,
    *,
    grain: str,
    key: Sequence[str],
) -> None:
    spark.sql(
        f"ALTER TABLE {_quoted(table)} SET TBLPROPERTIES ("
        f"'grain'={_literal(grain)}, 'key'={_literal(','.join(key))})"
    )


def _merge_by_key(
    spark: SparkSession,
    table: str,
    df: DataFrame,
    key: Sequence[str],
) -> None:
    view = f"_silver_merge_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(view)
    condition = " AND ".join(
        f"target.`{column}` <=> source.`{column}`" for column in key
    )
    try:
        spark.sql(
            f"MERGE INTO {_quoted(table)} AS target USING `{view}` AS source ON {condition} "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView(view)


def _write_result(
    spark: SparkSession,
    *,
    spec: SilverTableSpec,
    result: TransformResult,
    mode: str,
    catalog: str,
    silver_namespace: str,
    quarantine_namespace: str,
    accepted_count: int,
    rejected_count: int,
    on_phase_change: Callable[[str], None],
    on_phase_committed: Callable[[str], None],
) -> None:
    target = _qualified(catalog, silver_namespace, spec.name)
    quarantine = _qualified(catalog, quarantine_namespace, spec.name)
    input_size = max(1, (accepted_count + rejected_count) * 512)

    # 1. Commit Accepted Silver Data
    on_phase_change("accepted_data_commit")
    if mode == "full" or not _table_exists(spark, target):
        create_or_replace(
            spark,
            target,
            result.accepted,
            spec.partitions,
            input_size_bytes=input_size,
        )
    else:
        _merge_by_key(spark, target, result.accepted, spec.key)
    on_phase_committed("accepted")

    on_phase_change("accepted_property_application")
    _set_contract_properties(spark, target, grain=spec.grain, key=spec.key)

    # 2. Commit Quarantined Data (if any)
    if rejected_count > 0:
        on_phase_change("quarantine_data_commit")
        if mode == "full" or not _table_exists(spark, quarantine):
            create_or_replace(
                spark,
                quarantine,
                result.rejected,
                (),
                input_size_bytes=input_size,
            )
        else:
            _merge_by_key(spark, quarantine, result.rejected, ("_batch_id", *spec.key))
        on_phase_committed("quarantine")

        on_phase_change("quarantine_property_application")
        _set_contract_properties(
            spark,
            quarantine,
            grain="one rejected Bronze record",
            key=("_batch_id", *spec.key),
        )
    elif mode == "full" and _table_exists(spark, quarantine):
        on_phase_change("quarantine_drop")
        spark.sql(f"DROP TABLE {_quoted(quarantine)} PURGE")


def _transform_spec(
    spec: SilverTableSpec,
    frames: Mapping[str, DataFrame],
    incoming: DataFrame,
    *,
    mode: str,
) -> TransformResult:
    """Transform one target, retaining full history for incremental ZIP aggregates if needed."""
    if spec.name != "geography" or mode == "full":
        selected_frames = dict(frames)
        selected_frames[spec.bronze_source] = incoming
        return spec.transform(selected_frames)

    affected = (
        incoming.select(
            normalized_zip("geolocation_zip_code_prefix").alias("_affected_zip")
        )
        .where(F.col("_affected_zip").rlike(r"^[0-9]{5}$"))
        .distinct()
    )
    historical = (
        frames["geolocation"]
        .withColumn(
            "_affected_zip",
            normalized_zip("geolocation_zip_code_prefix"),
        )
        .join(F.broadcast(affected), "_affected_zip", "inner")
        .drop("_affected_zip")
    )
    historical_result = transform_geography(historical)
    incoming_result = transform_geography(incoming)
    return TransformResult(
        accepted=historical_result.accepted,
        rejected=incoming_result.rejected,
    )


def run_silver_job(
    *,
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    attempt_id: str | None = None,
    batch_id: str,
    mode: str,
    table_names: Sequence[str] | None = None,
    bronze_namespace: str = "bronze",
    silver_namespace: str = "silver",
    quarantine_namespace: str = "quarantine",
    control_namespace: str = "control",
) -> None:
    """Transform selected Bronze tables into Silver and Quarantine layers with durable audit transitions."""
    if mode not in {"full", "incremental"}:
        raise ValueError("mode must be 'full' or 'incremental'")

    specs = _selected_specs(table_names)
    catalog = settings.iceberg_catalog_name
    audit_run_id = run_id if attempt_id is None else attempt_run_id(run_id, attempt_id)
    started_at = datetime.now(timezone.utc)
    total_input = 0
    total_output = 0
    total_rejected = 0
    target_progress: dict[str, _TargetProgress] = {}

    log_event(
        LOGGER,
        "silver_pipeline_started",
        run_id=run_id,
        batch_id=batch_id,
        mode=mode,
        tables=[s.name for s in specs],
    )

    ensure_audit_tables(
        spark,
        namespace=control_namespace,
        include_transformation_targets=True,
    )
    start_pipeline_run(
        spark,
        run_id=audit_run_id,
        logical_run_id=run_id,
        attempt_id="1" if attempt_id is None else attempt_id,
        batch_id=batch_id,
        job_name="silver_transformation",
        layer="silver",
        mode=mode,
        started_at=started_at,
        namespace=control_namespace,
    )

    try:
        bronze_frames = _preflight_dependencies(
            spark,
            catalog=catalog,
            bronze_namespace=bronze_namespace,
            specs=specs,
            mode=mode,
            batch_id=batch_id,
        )

        main_frames: dict[str, DataFrame] = {}
        main_counts: dict[str, int] = {}
        for spec in specs:
            frame = bronze_frames[spec.bronze_source]
            if mode == "incremental":
                frame = frame.where(F.col("_batch_id") == batch_id)
            frame = frame.cache()
            count = frame.count()
            if mode == "incremental" and count == 0:
                raise RuntimeError(
                    f"Bronze batch {batch_id!r} has no rows for {spec.bronze_source}"
                )
            main_frames[spec.name] = frame
            main_counts[spec.name] = count
            total_input += count

        for spec in specs:
            accepted: DataFrame | None = None
            rejected: DataFrame | None = None
            state = _TargetProgress(input_count=main_counts[spec.name])
            start_transformation_target(
                spark,
                run_id=audit_run_id,
                batch_id=batch_id,
                target_name=spec.name,
                mode=mode,
                input_count=main_counts[spec.name],
                started_at=datetime.now(timezone.utc),
                namespace=control_namespace,
            )
            target_progress[spec.name] = state

            def mark_phase(phase: str) -> None:
                state.phase = phase

            def mark_committed(phase: str) -> None:
                nonlocal total_output, total_rejected
                if phase == "accepted":
                    state.accepted_committed = True
                    total_output += state.accepted_count or 0
                elif phase == "quarantine":
                    state.quarantine_committed = True
                    total_rejected += state.rejected_count or 0

            try:
                result = _transform_spec(
                    spec,
                    bronze_frames,
                    main_frames[spec.name],
                    mode=mode,
                )
                accepted = result.accepted.cache()
                rejected = result.rejected.cache()
                accepted_count = accepted.count()
                rejected_count = rejected.count()
                state.accepted_count = accepted_count
                state.rejected_count = rejected_count

                stable_result = TransformResult(accepted, rejected)
                _write_result(
                    spark,
                    spec=spec,
                    result=stable_result,
                    mode=mode,
                    catalog=catalog,
                    silver_namespace=silver_namespace,
                    quarantine_namespace=quarantine_namespace,
                    accepted_count=accepted_count,
                    rejected_count=rejected_count,
                    on_phase_change=mark_phase,
                    on_phase_committed=mark_committed,
                )
                finish_transformation_target(
                    spark,
                    run_id=audit_run_id,
                    target_name=spec.name,
                    accepted_count=accepted_count,
                    rejected_count=rejected_count,
                    accepted_committed=state.accepted_committed,
                    quarantine_committed=state.quarantine_committed,
                    status="SUCCESS",
                    completed_at=datetime.now(timezone.utc),
                    namespace=control_namespace,
                )
                target_progress.pop(spec.name)
                log_event(
                    LOGGER,
                    "silver_table_succeeded",
                    run_id=run_id,
                    batch_id=batch_id,
                    table=spec.name,
                    accepted_count=accepted_count,
                    rejected_count=rejected_count,
                )
            finally:
                if accepted is not None:
                    accepted.unpersist()
                if rejected is not None:
                    rejected.unpersist()

        finish_pipeline_run(
            spark,
            run_id=audit_run_id,
            input_count=total_input,
            output_count=total_output,
            rejected_count=total_rejected,
            status="SUCCESS",
            completed_at=datetime.now(timezone.utc),
            namespace=control_namespace,
        )
        log_event(
            LOGGER,
            "silver_pipeline_completed",
            run_id=run_id,
            batch_id=batch_id,
            total_input=total_input,
            total_output=total_output,
            total_rejected=total_rejected,
        )
    except Exception as exc:
        summary = truncate_error_summary(f"{type(exc).__name__}: {exc}")
        for target_name, state in target_progress.items():
            try:
                finish_transformation_target(
                    spark,
                    run_id=audit_run_id,
                    target_name=target_name,
                    accepted_count=state.accepted_count,
                    rejected_count=state.rejected_count,
                    accepted_committed=state.accepted_committed,
                    quarantine_committed=state.quarantine_committed,
                    status="FAILED",
                    completed_at=datetime.now(timezone.utc),
                    error_summary=summary,
                    namespace=control_namespace,
                )
            except Exception:
                pass
        try:
            finish_pipeline_run(
                spark,
                run_id=audit_run_id,
                input_count=total_input,
                output_count=total_output,
                rejected_count=total_rejected,
                status="FAILED",
                completed_at=datetime.now(timezone.utc),
                error_summary=summary,
                namespace=control_namespace,
            )
        except Exception:
            pass
        raise
    finally:
        for frame in locals().get("main_frames", {}).values():
            frame.unpersist()


def _runtime_settings(env: Mapping[str, str] | None = None) -> Settings:
    values = dict(os.environ if env is None else env)
    values.setdefault("MINIO_ROOT_USER", values.get("AWS_ACCESS_KEY_ID", "olistprocess"))
    values.setdefault("MINIO_ROOT_PASSWORD", values.get("AWS_SECRET_ACCESS_KEY", "admin_process"))
    return Settings.from_env(values)


def _parser() -> argparse.ArgumentParser:
    now_utc = datetime.now(timezone.utc)
    default_run_id = os.environ.get("RUN_ID", f"silver_{now_utc.strftime('%Y%m%d_%H%M%S')}")
    default_batch_id = os.environ.get("BATCH_ID", f"batch_{now_utc.strftime('%Y%m%d')}")

    parser = argparse.ArgumentParser(
        description="Transform Bronze tables into Silver and Quarantine layers (Zero-Config Defaults enabled)."
    )
    parser.add_argument(
        "--run-id",
        default=default_run_id,
        help=f"Pipeline run ID (default: current timestamp '{default_run_id}')",
    )
    parser.add_argument(
        "--attempt-id",
        default="1",
        help="Pipeline attempt ID (default: '1')",
    )
    parser.add_argument(
        "--batch-id",
        default=default_batch_id,
        help=f"Source batch ID (default: today's date '{default_batch_id}')",
    )
    parser.add_argument(
        "--mode",
        choices=("full", "incremental"),
        default="full",
        help="Execution mode (default: 'full')",
    )
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        help="Specific Silver table to process (optional; repeat flag for multiple tables; default: all 8 tables)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Silver orchestration CLI."""
    args = _parser().parse_args(argv)
    configure_logging()
    settings = _runtime_settings()
    spark = build_spark_session(settings, f"olist-silver-{args.run_id}")
    try:
        run_silver_job(
            spark=spark,
            settings=settings,
            run_id=args.run_id,
            attempt_id=args.attempt_id,
            batch_id=args.batch_id,
            mode=args.mode,
            table_names=args.tables,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
