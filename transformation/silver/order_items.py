"""Chuẩn hóa chi tiết món hàng trong đơn (Order Items) và làm giàu thời gian mua hàng ở Tầng Silver.

Nghiệp vụ:
1. Ép kiểu dữ liệu tài chính: price và freight_value sang decimal(18,2), thứ tự món hàng order_item_id sang int.
2. Broadcast Join với orders:
   - Làm giàu trường `order_purchase_timestamp` trực tiếp vào từng món hàng.
   - Ý nghĩa: Giúp bảng Fact bán hàng sau này có thể phân tích theo ngày/tháng mà không cần JOIN ngược lại orders.
3. Kiểm tra toàn vẹn tham chiếu (Referential Integrity):
   - Nếu mã order_id không tồn tại trong bảng orders -> lỗi MISSING_ORDER_REFERENCE -> đưa vào Quarantine.
4. Kiểm tra giá bán, cước phí không được âm (< 0).
5. Khử trùng lặp theo cặp khóa phức hợp (order_id, order_item_id).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result
from transformation.silver.orders import TIMESTAMP_FORMAT

# Định dạng tiền tệ chuẩn: 18 chữ số, 2 số thập phân
MONEY_TYPE = "decimal(18,2)"


def _order_purchase_lookup(orders: DataFrame) -> DataFrame:
    """Tạo bảng tra cứu thời điểm đặt mua (order_purchase_timestamp) theo từng order_id."""
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
    """Biến đổi bảng Bronze order_items thành dữ liệu Silver sạch và Vùng cách ly Quarantine."""
    # Bước 1: Chuẩn hóa dữ liệu, ép kiểu tiền tệ và Broadcast Join lấy order_purchase_timestamp
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
        # Toàn vẹn tham chiếu: Mã order_id phải tồn tại trong bảng orders
        ValidationRule("MISSING_ORDER_REFERENCE", F.col("order_purchase_timestamp").isNull()),
    )

    # Bước 3: Khử trùng lặp xác định theo cặp khóa (order_id, order_item_id)
    return deterministic_result(
        normalized,
        rules,
        key=("order_id", "order_item_id"),
        duplicate_code="DUPLICATE_ORDER_ITEM_KEY",
    )
