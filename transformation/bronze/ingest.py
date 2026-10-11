"""Spark transformations for source-shaped Bronze records."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ingestion_metadata(
    df: DataFrame,
    batch_id: str,
    source_file: str,
    ingested_at: datetime,
) -> DataFrame:
    """Return source values unchanged with deterministic Bronze lineage columns."""
    utc_time = (
        ingested_at.replace(tzinfo=timezone.utc)
        if ingested_at.tzinfo is None
        else ingested_at.astimezone(timezone.utc)
    )
    timestamp = F.to_timestamp(F.lit(utc_time.isoformat()))

    return (
        df.withColumn("_ingestion_timestamp", timestamp)
        .withColumn("_ingestion_date", F.to_date(F.col("_ingestion_timestamp")))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_file", F.lit(source_file))
    )


# ==============================================================================
# LEGACY / OVER-ENGINEERED CODE (COMMENTED OUT FOR REFERENCE)
#
# Reason for removal:
# The original project calculated a SHA-256 hash for every single row (_row_hash)
# by serializing each row into a JSON string and hashing it.
# In a business e-commerce data lakehouse, this per-row cryptographic hashing adds
# heavy CPU overhead without providing practical value. Standard audit columns
# (_ingestion_timestamp, _ingestion_date, _batch_id, _source_file) are sufficient.
# ==============================================================================
#
# def _legacy_add_ingestion_metadata_with_row_hash(
#     df: DataFrame,
#     batch_id: str,
#     source_file: str,
#     ingested_at: datetime,
# ) -> DataFrame:
#     source_columns = tuple(df.columns)
#     utc_time = (
#         ingested_at.replace(tzinfo=timezone.utc)
#         if ingested_at.tzinfo is None
#         else ingested_at.astimezone(timezone.utc)
#     )
#     timestamp = F.to_timestamp(F.lit(utc_time.isoformat()))
#     canonical_row = F.to_json(
#         F.struct(*(F.col(name).alias(name) for name in source_columns)),
#         options={"ignoreNullFields": "false"},
#     )
#     row_hash = F.sha2(canonical_row, 256)
#
#     return (
#         df.withColumn("_ingestion_timestamp", timestamp)
#         .withColumn("_ingestion_date", F.to_date(F.col("_ingestion_timestamp")))
#         .withColumn("_batch_id", F.lit(batch_id))
#         .withColumn("_source_file", F.lit(source_file))
#         .withColumn("_row_hash", row_hash)
#     )
