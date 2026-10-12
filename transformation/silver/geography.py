"""Silver ZIP-level geography validation and deterministic aggregation."""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformation.common.validation import TransformResult, ValidationRule, split_valid_records
from transformation.silver._common import blank, normalized_zip


COORDINATE_TYPE = "decimal(10,7)"
BRAZIL_LATITUDE_MIN = Decimal("-34.0000000")
BRAZIL_LATITUDE_MAX = Decimal("6.0000000")
BRAZIL_LONGITUDE_MIN = Decimal("-74.0000000")
BRAZIL_LONGITUDE_MAX = Decimal("-34.0000000")


def transform_geography(df: DataFrame) -> TransformResult:
    """Return one canonical coordinate record per valid Brazilian ZIP prefix."""
    normalized = (
        df.withColumn(
            "zip_code_prefix",
            normalized_zip("geolocation_zip_code_prefix"),
        )
        .withColumn("latitude", F.trim("geolocation_lat").cast(COORDINATE_TYPE))
        .withColumn("longitude", F.trim("geolocation_lng").cast(COORDINATE_TYPE))
        .withColumn("city", F.lower(F.trim("geolocation_city")))
        .withColumn("state", F.upper(F.trim("geolocation_state")))
        .drop(
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        )
    )
    rules = (
        ValidationRule(
            "INVALID_GEO_ZIP",
            blank("zip_code_prefix") | ~F.col("zip_code_prefix").rlike(r"^[0-9]{5}$"),
        ),
        ValidationRule(
            "INVALID_GEO_LATITUDE",
            F.col("latitude").isNull()
            | ~F.col("latitude").between(BRAZIL_LATITUDE_MIN, BRAZIL_LATITUDE_MAX),
        ),
        ValidationRule(
            "INVALID_GEO_LONGITUDE",
            F.col("longitude").isNull()
            | ~F.col("longitude").between(BRAZIL_LONGITUDE_MIN, BRAZIL_LONGITUDE_MAX),
        ),
        ValidationRule("MISSING_GEO_CITY", blank("city")),
        ValidationRule(
            "INVALID_GEO_STATE",
            blank("state") | ~F.col("state").rlike(r"^[A-Z]{2}$"),
        ),
    )
    observations = split_valid_records(normalized, rules)
    valid = observations.accepted

    # 1. Aggregate coordinates per ZIP prefix (centroid average)
    coordinates = valid.groupBy("zip_code_prefix").agg(
        F.avg("latitude").cast(COORDINATE_TYPE).alias("latitude"),
        F.avg("longitude").cast(COORDINATE_TYPE).alias("longitude"),
        F.count(F.lit(1)).cast("long").alias("source_observation_count"),
        F.max("_ingestion_timestamp").alias("_ingestion_timestamp"),
        F.max("_ingestion_date").alias("_ingestion_date"),
        F.max("_batch_id").alias("_batch_id"),
        F.min("_source_file").alias("_source_file"),
    )

    # 2. Find most frequent canonical city/state name per ZIP prefix
    frequency = valid.groupBy("zip_code_prefix", "city", "state").agg(
        F.count(F.lit(1)).alias("_location_frequency")
    )
    canonical_window = Window.partitionBy("zip_code_prefix").orderBy(
        F.col("_location_frequency").desc(),
        F.col("city").asc(),
        F.col("state").asc(),
    )
    canonical = (
        frequency.withColumn("_canonical_rank", F.row_number().over(canonical_window))
        .where(F.col("_canonical_rank") == 1)
        .select("zip_code_prefix", "city", "state")
    )

    accepted = coordinates.join(canonical, "zip_code_prefix", "inner")
    return TransformResult(accepted=accepted, rejected=observations.rejected)


# ==============================================================================
# LEGACY / OVER-ENGINEERED CODE (COMMENTED OUT FOR REFERENCE)
#
# Reason for removal:
# The original code computed a synthetic SHA-256 _row_hash for the aggregated
# geography coordinates using concat_ws("\u001f", ...).
# Consistent with removing per-row hash computations across the lakehouse,
# this extra hash step was removed to save compute cycles.
# ==============================================================================
#
# def _legacy_hash_geography(accepted: DataFrame) -> DataFrame:
#     return accepted.withColumn(
#         "_row_hash",
#         F.sha2(
#             F.concat_ws(
#                 "\u001f",
#                 "zip_code_prefix",
#                 F.col("latitude").cast("string"),
#                 F.col("longitude").cast("string"),
#                 "city",
#                 "state",
#             ),
#             256,
#         ),
#     )
