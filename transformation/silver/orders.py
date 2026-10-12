"""Silver order lifecycle typing, validation, and delivery metrics."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result


TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
TIMESTAMP_COLUMNS = (
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
)
ALLOWED_ORDER_STATUSES = (
    "approved",
    "canceled",
    "created",
    "delivered",
    "invoiced",
    "processing",
    "shipped",
    "unavailable",
)


def transform_orders(df: DataFrame) -> TransformResult:
    """Return one typed order per key and quarantine invalid lifecycle records."""
    normalized = (
        df.withColumn("order_id", F.trim("order_id"))
        .withColumn("customer_id", F.trim("customer_id"))
        .withColumn("order_status", F.lower(F.trim("order_status")))
    )
    for name in TIMESTAMP_COLUMNS:
        normalized = normalized.withColumn(f"_raw_{name}", F.col(name)).withColumn(
            name, F.to_timestamp(F.trim(F.col(name)), TIMESTAMP_FORMAT)
        )

    # Calculate delivery duration and late delivery flag
    normalized = (
        normalized.withColumn(
            "delivery_duration_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
            ).cast("int"),
        )
        .withColumn(
            "carrier_delivery_duration_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull()
                & F.col("order_delivered_carrier_date").isNotNull(),
                F.datediff("order_delivered_customer_date", "order_delivered_carrier_date"),
            ).cast("int"),
        )
        .withColumn(
            "is_late_delivery",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.col("order_delivered_customer_date")
                > F.col("order_estimated_delivery_date"),
            ).otherwise(F.lit(False)),
        )
    )

    optional_timestamp_rules = tuple(
        ValidationRule(
            f"INVALID_{name.upper()}",
            (~blank(f"_raw_{name}")) & F.col(name).isNull(),
        )
        for name in (
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
        )
    )
    purchase = F.col("order_purchase_timestamp")
    delivered = F.col("order_delivered_customer_date")
    carrier = F.col("order_delivered_carrier_date")
    rules = (
        ValidationRule("MISSING_ORDER_ID", blank("order_id")),
        ValidationRule("MISSING_ORDER_CUSTOMER_ID", blank("customer_id")),
        ValidationRule(
            "INVALID_ORDER_STATUS",
            F.col("order_status").isNull()
            | ~F.col("order_status").isin(*ALLOWED_ORDER_STATUSES),
        ),
        ValidationRule("INVALID_ORDER_PURCHASE_TIMESTAMP", purchase.isNull()),
        *optional_timestamp_rules,
        ValidationRule(
            "INVALID_ORDER_ESTIMATED_DELIVERY_DATE",
            F.col("order_estimated_delivery_date").isNull(),
        ),
        # Lifecycle chronology checks
        ValidationRule(
            "APPROVAL_BEFORE_PURCHASE",
            F.col("order_approved_at").isNotNull()
            & (purchase.isNull() | (F.col("order_approved_at") < purchase)),
        ),
        ValidationRule(
            "CARRIER_BEFORE_PURCHASE",
            carrier.isNotNull() & (purchase.isNull() | (carrier < purchase)),
        ),
        ValidationRule(
            "DELIVERY_BEFORE_PURCHASE",
            delivered.isNotNull() & (purchase.isNull() | (delivered < purchase)),
        ),
        ValidationRule(
            "DELIVERY_BEFORE_CARRIER",
            delivered.isNotNull() & carrier.isNotNull() & (delivered < carrier),
        ),
        ValidationRule(
            "DELIVERED_ORDER_MISSING_DELIVERY_DATE",
            (F.col("order_status") == "delivered") & delivered.isNull(),
        ),
    )
    result = deterministic_result(
        normalized,
        rules,
        key=("order_id",),
        duplicate_code="DUPLICATE_ORDER_ID",
    )
    helper_columns = tuple(f"_raw_{name}" for name in TIMESTAMP_COLUMNS)
    return TransformResult(
        accepted=result.accepted.drop(*helper_columns),
        rejected=result.rejected.drop(*helper_columns),
    )
