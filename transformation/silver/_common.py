"""Internal Spark-expression helpers for Silver transformations."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformation.common.validation import TransformResult, ValidationRule, split_valid_records


LINEAGE_COLUMNS = (
    "_ingestion_timestamp",
    "_ingestion_date",
    "_batch_id",
    "_source_file",
)


def blank(column: str):
    """Return an invalidity expression for a required trimmed string."""
    return F.col(column).isNull() | (F.trim(F.col(column)) == "")


def normalized_zip(column: str):
    """Normalize a one-to-five digit Brazilian ZIP prefix to five characters."""
    value = F.trim(F.col(column))
    return F.when(value.rlike(r"^[0-9]{1,5}$"), F.lpad(value, 5, "0")).otherwise(value)


def deterministic_result(
    df: DataFrame,
    rules: Sequence[ValidationRule],
    *,
    key: Sequence[str],
    duplicate_code: str,
) -> TransformResult:
    """Apply rules and retain exactly one deterministic winner per valid key."""
    business_errors = F.array(
        *(F.when(rule.invalid_when, F.lit(rule.error_code)) for rule in rules)
    )
    business_error_count = F.size(F.filter(business_errors, lambda value: value.isNotNull()))
    ordering = [
        business_error_count.asc(),
        F.col("_ingestion_timestamp").desc_nulls_last(),
        F.col("_batch_id").desc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
    ]
    ranked = df.withColumn(
        "_silver_dedupe_rank",
        F.row_number().over(Window.partitionBy(*(F.col(name) for name in key)).orderBy(*ordering)),
    )
    result = split_valid_records(
        ranked,
        (*rules, ValidationRule(duplicate_code, F.col("_silver_dedupe_rank") > 1)),
    )
    return TransformResult(
        accepted=result.accepted.drop("_silver_dedupe_rank"),
        rejected=result.rejected.drop("_silver_dedupe_rank"),
    )


# ==============================================================================
# LEGACY / OVER-ENGINEERED CODE (COMMENTED OUT FOR REFERENCE)
#
# Reason for removal:
# The original project included _row_hash in LINEAGE_COLUMNS and used
# F.col("_row_hash").asc_nulls_last() as the final tie-breaker in Window ranking.
# Since per-row SHA-256 hashing was eliminated to save CPU and RAM, tie-breaking
# is performed deterministically using _source_file and ingestion metadata.
# ==============================================================================
#
# LEGACY_LINEAGE_COLUMNS = (
#     "_ingestion_timestamp",
#     "_ingestion_date",
#     "_batch_id",
#     "_source_file",
#     "_row_hash",
# )
