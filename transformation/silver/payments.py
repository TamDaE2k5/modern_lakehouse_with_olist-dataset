"""Silver payment typing, validation, and order timestamp enrichment."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result
from transformation.silver.order_items import MONEY_TYPE, _order_purchase_lookup


def transform_payments(payments: DataFrame, orders: DataFrame) -> TransformResult:
    """Return typed payment components enriched with their order purchase timestamp."""
    normalized = (
        payments.withColumn("order_id", F.trim("order_id"))
        .withColumn("payment_sequential", F.trim("payment_sequential").cast("int"))
        .withColumn("payment_type", F.lower(F.trim("payment_type")))
        .withColumn("payment_installments", F.trim("payment_installments").cast("int"))
        .withColumn("payment_value", F.trim("payment_value").cast(MONEY_TYPE))
        .join(
            F.broadcast(_order_purchase_lookup(orders)),
            F.col("order_id") == F.col("_lookup_order_id"),
            "left",
        )
        .drop("_lookup_order_id")
    )

    rules = (
        ValidationRule("MISSING_PAYMENT_ORDER_ID", blank("order_id")),
        ValidationRule(
            "INVALID_PAYMENT_SEQUENCE",
            F.col("payment_sequential").isNull() | (F.col("payment_sequential") <= 0),
        ),
        ValidationRule("MISSING_PAYMENT_TYPE", blank("payment_type")),
        ValidationRule(
            "INVALID_PAYMENT_INSTALLMENTS",
            F.col("payment_installments").isNull(),
        ),
        ValidationRule(
            "NEGATIVE_PAYMENT_INSTALLMENTS",
            F.col("payment_installments") < 0,
        ),
        ValidationRule("INVALID_PAYMENT_VALUE", F.col("payment_value").isNull()),
        ValidationRule("NEGATIVE_PAYMENT_VALUE", F.col("payment_value") < 0),
        ValidationRule("MISSING_ORDER_REFERENCE", F.col("order_purchase_timestamp").isNull()),
    )

    return deterministic_result(
        normalized,
        rules,
        key=("order_id", "payment_sequential"),
        duplicate_code="DUPLICATE_PAYMENT_KEY",
    )
