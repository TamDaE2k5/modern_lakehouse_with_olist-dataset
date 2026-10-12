"""Silver review typing, validation, and comment indicators."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result
from transformation.silver.orders import TIMESTAMP_FORMAT


def transform_reviews(df: DataFrame) -> TransformResult:
    """Return one typed review-order relation per key and quarantine invalid rows."""
    normalized = (
        df.withColumn("review_id", F.trim("review_id"))
        .withColumn("order_id", F.trim("order_id"))
        .withColumn("review_score", F.trim("review_score").cast("int"))
        .withColumn("review_comment_title", F.trim("review_comment_title"))
        .withColumn("review_comment_message", F.trim("review_comment_message"))
        .withColumn(
            "review_creation_date",
            F.to_timestamp(F.trim("review_creation_date"), TIMESTAMP_FORMAT),
        )
        .withColumn(
            "review_answer_timestamp",
            F.to_timestamp(F.trim("review_answer_timestamp"), TIMESTAMP_FORMAT),
        )
        .withColumn(
            "has_title",
            F.coalesce(F.length(F.trim("review_comment_title")) > 0, F.lit(False)),
        )
        .withColumn(
            "has_message",
            F.coalesce(F.length(F.trim("review_comment_message")) > 0, F.lit(False)),
        )
    )
    rules = (
        ValidationRule("MISSING_REVIEW_ID", blank("review_id")),
        ValidationRule("MISSING_REVIEW_ORDER_ID", blank("order_id")),
        ValidationRule(
            "INVALID_REVIEW_SCORE",
            F.col("review_score").isNull() | ~F.col("review_score").between(1, 5),
        ),
        ValidationRule(
            "INVALID_REVIEW_CREATION_DATE",
            F.col("review_creation_date").isNull(),
        ),
        ValidationRule(
            "INVALID_REVIEW_ANSWER_TIMESTAMP",
            F.col("review_answer_timestamp").isNull(),
        ),
        ValidationRule(
            "REVIEW_ANSWER_BEFORE_CREATION",
            F.col("review_creation_date").isNotNull()
            & F.col("review_answer_timestamp").isNotNull()
            & (F.col("review_answer_timestamp") < F.col("review_creation_date")),
        ),
    )
    return deterministic_result(
        normalized,
        rules,
        key=("review_id", "order_id"),
        duplicate_code="DUPLICATE_REVIEW_KEY",
    )
