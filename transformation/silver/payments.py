"""Chuẩn hóa dữ liệu Thanh toán (Order Payments) và kiểm định chất lượng ở Tầng Silver.

Nghiệp vụ:
1. Ép kiểu: payment_value sang decimal(18,2), số thứ tự thanh toán và số kỳ trả góp sang int.
2. Chuẩn hóa hình thức thanh toán payment_type (credit_card, boleto, voucher, debit_card) về chữ thường.
3. Broadcast Join với orders để bổ sung trường order_purchase_timestamp.
4. Kiểm định chất lượng:
   - Số tiền thanh toán payment_value không được âm (< 0).
   - Số kỳ trả góp payment_installments >= 0.
   - Kiểm tra toàn vẹn tham chiếu với bảng orders (MISSING_ORDER_REFERENCE).
5. Khử trùng lặp theo cặp khóa phức hợp (order_id, payment_sequential).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result
from transformation.silver.order_items import MONEY_TYPE, _order_purchase_lookup


def transform_payments(payments: DataFrame, orders: DataFrame) -> TransformResult:
    """Biến đổi bảng Bronze order_payments thành dữ liệu Silver sạch và Vùng cách ly Quarantine."""
    # Bước 1: Chuẩn hóa dữ liệu, ép kiểu và Broadcast Join lấy order_purchase_timestamp
    normalized = (
        payments.withColumn("order_id", F.trim("order_id"))
        .withColumn("payment_sequential", F.trim("payment_sequential").cast("int"))
        .withColumn("payment_type", F.lower(F.trim("payment_type")))
        .withColumn("payment_installments", F.trim("payment_installments").cast("int"))
        .withColumn("payment_value", F.trim("payment_value").cast(MONEY_TYPE))
        # Nối với bảng orders để lấy ngày mua hàng
        .join(
            F.broadcast(_order_purchase_lookup(orders)),
            F.col("order_id") == F.col("_lookup_order_id"),
            "left",
        )
        .drop("_lookup_order_id")
    )

    # Bước 2: Bộ luật kiểm định chất lượng
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
        # Toàn vẹn tham chiếu: Mã order_id phải tồn tại trong bảng orders
        ValidationRule("MISSING_ORDER_REFERENCE", F.col("order_purchase_timestamp").isNull()),
    )

    # Bước 3: Khử trùng lặp xác định theo cặp khóa (order_id, payment_sequential)
    return deterministic_result(
        normalized,
        rules,
        key=("order_id", "payment_sequential"),
        duplicate_code="DUPLICATE_PAYMENT_KEY",
    )
