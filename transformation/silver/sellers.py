"""Silver seller normalization and validation."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result, normalized_zip


def transform_sellers(df: DataFrame) -> TransformResult:
    """Return normalized unique sellers and quarantined invalid source rows."""
    normalized = (
        df.withColumn("seller_id", F.trim("seller_id"))
        .withColumn("seller_zip_code_prefix", normalized_zip("seller_zip_code_prefix"))
        .withColumn("seller_city", F.lower(F.trim("seller_city")))
        .withColumn("seller_state", F.upper(F.trim("seller_state")))
    )
    rules = (
        ValidationRule("MISSING_SELLER_ID", blank("seller_id")),
        ValidationRule(
            "INVALID_SELLER_ZIP",
            blank("seller_zip_code_prefix")
            | ~F.col("seller_zip_code_prefix").rlike(r"^[0-9]{5}$"),
        ),
        ValidationRule("MISSING_SELLER_CITY", blank("seller_city")),
        ValidationRule(
            "INVALID_SELLER_STATE",
            blank("seller_state") | ~F.col("seller_state").rlike(r"^[A-Z]{2}$"),
        ),
    )
    return deterministic_result(
        normalized,
        rules,
        key=("seller_id",),
        duplicate_code="DUPLICATE_SELLER_ID",
    )
