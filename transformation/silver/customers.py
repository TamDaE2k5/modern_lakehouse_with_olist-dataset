"""Silver customer normalization and validation."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result, normalized_zip


def transform_customers(df: DataFrame) -> TransformResult:
    """Return normalized unique customers and quarantined invalid source rows."""
    normalized = (
        df.withColumn("customer_id", F.trim("customer_id"))
        .withColumn("customer_unique_id", F.trim("customer_unique_id"))
        .withColumn("customer_zip_code_prefix", normalized_zip("customer_zip_code_prefix"))
        .withColumn("customer_city", F.lower(F.trim("customer_city")))
        .withColumn("customer_state", F.upper(F.trim("customer_state")))
    )
    rules = (
        ValidationRule("MISSING_CUSTOMER_ID", blank("customer_id")),
        ValidationRule("MISSING_CUSTOMER_UNIQUE_ID", blank("customer_unique_id")),
        ValidationRule(
            "INVALID_CUSTOMER_ZIP",
            blank("customer_zip_code_prefix")
            | ~F.col("customer_zip_code_prefix").rlike(r"^[0-9]{5}$"),
        ),
        ValidationRule("MISSING_CUSTOMER_CITY", blank("customer_city")),
        ValidationRule(
            "INVALID_CUSTOMER_STATE",
            blank("customer_state") | ~F.col("customer_state").rlike(r"^[A-Z]{2}$"),
        ),
    )
    return deterministic_result(
        normalized,
        rules,
        key=("customer_id",),
        duplicate_code="DUPLICATE_CUSTOMER_ID",
    )
