"""Silver order-item typing, validation, and order timestamp enrichment."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result
from transformation.silver.orders import TIMESTAMP_FORMAT

MONEY_TYPE = "decimal(18,2)"


def _order_purchase_lookup(orders: DataFrame) -> DataFrame:
    """Return latest distinct purchase timestamp per order_id for broadcast enrichment."""
    ordering = Window.partitionBy(F.trim("order_id")).orderBy(
        F.col("_ingestion_timestamp").desc_nulls_last(),
        F.col("_batch_id").desc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
    )
    return (
        orders.withColumn("_lookup_rank", F.row_number().over(ordering))
        .where(F.col("_lookup_rank") == 1)
        .select(
            F.trim("order_id").alias("_lookup_order_id"),
            F.to_timestamp(F.trim("order_purchase_timestamp"), TIMESTAMP_FORMAT).alias(
                "order_purchase_timestamp"
            ),
        )
    )


def transform_order_items(items: DataFrame, orders: DataFrame) -> TransformResult:
    """Return typed unique order line items enriched with their order purchase timestamp."""
    normalized = (
        items.withColumn("order_id", F.trim("order_id"))
        .withColumn("order_item_id", F.trim("order_item_id").cast("int"))
        .withColumn("product_id", F.trim("product_id"))
        .withColumn("seller_id", F.trim("seller_id"))
        .withColumn(
            "shipping_limit_date",
            F.to_timestamp(F.trim("shipping_limit_date"), TIMESTAMP_FORMAT),
        )
        .withColumn("price", F.trim("price").cast(MONEY_TYPE))
        .withColumn("freight_value", F.trim("freight_value").cast(MONEY_TYPE))
        .join(
            F.broadcast(_order_purchase_lookup(orders)),
            F.col("order_id") == F.col("_lookup_order_id"),
            "left",
        )
        .drop("_lookup_order_id")
    )

    rules = (
        ValidationRule("MISSING_ORDER_ITEM_ORDER_ID", blank("order_id")),
        ValidationRule(
            "INVALID_ORDER_ITEM_ID",
            F.col("order_item_id").isNull() | (F.col("order_item_id") <= 0),
        ),
        ValidationRule("MISSING_ORDER_ITEM_PRODUCT_ID", blank("product_id")),
        ValidationRule("MISSING_ORDER_ITEM_SELLER_ID", blank("seller_id")),
        ValidationRule("INVALID_SHIPPING_LIMIT_DATE", F.col("shipping_limit_date").isNull()),
        ValidationRule("INVALID_PRICE", F.col("price").isNull()),
        ValidationRule("NEGATIVE_PRICE", F.col("price") < 0),
        ValidationRule("INVALID_FREIGHT_VALUE", F.col("freight_value").isNull()),
        ValidationRule("NEGATIVE_FREIGHT_VALUE", F.col("freight_value") < 0),
        ValidationRule("MISSING_ORDER_REFERENCE", F.col("order_purchase_timestamp").isNull()),
    )

    return deterministic_result(
        normalized,
        rules,
        key=("order_id", "order_item_id"),
        duplicate_code="DUPLICATE_ORDER_ITEM_KEY",
    )
