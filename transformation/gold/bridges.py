"""Bảng cầu nối (Bridge Tables) giữa Đánh giá (Reviews) và các Chiều (Products, Sellers).

Tại sao cần Bảng Cầu Nối (Bridge Table) trong Mô hình Star Schema Kimball?
---------------------------------------------------------------------------
1. Bài toán Quan hệ Nhiều-Nhiều (Many-to-Many Relationship):
   - Một Đơn hàng (Order) có thể gồm 3 sản phẩm khác nhau đến từ 2 nhà bán lẻ (Sellers) khác nhau.
   - Khách hàng để lại đánh giá (Review) cho CẢ ĐƠN HÀNG đó (ở mức grain: review_id + order_id).
2. Nguy cơ khi cố nhét khóa ngoại trực tiếp vào bảng Sự kiện (Fact):
   - Nếu đưa `product_key` hoặc `seller_key` trực tiếp vào `fct_reviews`:
     Một review cho đơn hàng 3 sản phẩm sẽ phải nhân lên làm 3 dòng!
     Hậu quả: Làm nhân ba số lượng review thực tế, sai lệch điểm đánh giá trung bình (average review score),
     và phá vỡ quy ước hạt (grain) 1 review = 1 dòng.
3. Giải pháp chuẩn mực Kimball:
   - Giữ `fct_reviews` ở đúng hạt nguyên tử: 1 dòng cho mỗi `(review_id, order_id)`.
   - Tạo 2 Bảng Cầu Nối riêng biệt:
     + `bridge_review_products`: ánh xạ `(review_id, order_id)` -> `product_key`
     + `bridge_review_sellers`: ánh xạ `(review_id, order_id)` -> `seller_key`
   - Khi cần phân tích: "Điểm review trung bình của Sản phẩm X hoặc Người bán Y là bao nhiêu?",
     chỉ cần JOIN: `fct_reviews` <-> `bridge_review_products` <-> `dim_product`.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from transformation.gold.keys import prepare_dimension_lookup


# Schema cấu trúc của bảng cầu nối Review - Sản phẩm
BRIDGE_REVIEW_PRODUCTS_SCHEMA = StructType(
    [
        StructField("review_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_key", LongType(), False),
    ]
)

# Schema cấu trúc của bảng cầu nối Review - Nhà bán hàng
BRIDGE_REVIEW_SELLERS_SCHEMA = StructType(
    [
        StructField("review_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("seller_key", LongType(), False),
    ]
)


def build_review_product_bridge(
    reviews: DataFrame,
    order_items: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """Xây dựng bảng cầu nối giữa Đánh giá và Sản phẩm (bridge_review_products).

    Cơ chế hoạt động:
    1. Trích xuất cặp (review_id, order_id) hợp lệ từ bảng silver.reviews.
    2. Trích xuất cặp (order_id, product_id) từ bảng silver.order_items.
    3. JOIN inner qua order_id để liên kết: Review này thuộc Đơn hàng nào và đơn hàng đó có Sản phẩm nào.
    4. Tra cứu product_key từ dim_product thông qua prepare_dimension_lookup (Safety Gate).
    5. Đảm bảo tính duy nhất (distinct) để mỗi sản phẩm trong 1 review chỉ xuất hiện đúng 1 lần.
    """

    # 1. Lọc tập đánh giá hợp lệ
    review_grain = (
        reviews.select(
            F.trim("review_id").alias("review_id"),
            F.trim("order_id").alias("order_id"),
        )
        .where(
            F.col("review_id").isNotNull()
            & (F.length("review_id") > 0)
            & F.col("order_id").isNotNull()
            & (F.length("order_id") > 0)
        )
        .distinct()
    )

    # 2. Lọc các sản phẩm có trong đơn hàng
    product_relations = (
        order_items.select(
            F.trim("order_id").alias("order_id"),
            F.trim("product_id").alias("product_id"),
        )
        .where(F.col("order_id").isNotNull() & (F.length("order_id") > 0))
        .distinct()
    )

    # 3. Chuẩn bị bảng tra cứu dim_product có kiểm định an toàn (tránh Cartesian product)
    product_lookup = prepare_dimension_lookup(
        dim_product,
        "product_id",
        "product_key",
        "product",
    ).select(
        F.col("product_id").alias("_dimension_product_id"),
        F.col("product_key").cast("long"),
    )

    # 4. Ghép nối và sinh khóa ngoại product_key
    return (
        review_grain.join(product_relations, "order_id", "inner")
        .join(
            product_lookup,
            F.col("product_id") == F.col("_dimension_product_id"),
            "left",
        )
        .select(
            F.col("review_id").cast("string"),
            F.col("order_id").cast("string"),
            # Nếu sản phẩm không tìm thấy trong dim_product, gán về khóa 0 (Unknown)
            F.coalesce(F.col("product_key"), F.lit(0)).cast("long").alias("product_key"),
        )
        .distinct()
    )


def build_review_seller_bridge(
    reviews: DataFrame,
    order_items: DataFrame,
    dim_seller: DataFrame,
) -> DataFrame:
    """Xây dựng bảng cầu nối giữa Đánh giá và Nhà bán hàng (bridge_review_sellers).

    Cơ chế hoạt động tương tự bridge_review_products:
    Liên kết (review_id, order_id) với các seller_id xuất hiện trong đơn hàng đó,
    sau đó tra cứu sang dim_seller để lấy seller_key đại diện.
    """

    # 1. Lọc tập đánh giá hợp lệ
    review_grain = (
        reviews.select(
            F.trim("review_id").alias("review_id"),
            F.trim("order_id").alias("order_id"),
        )
        .where(
            F.col("review_id").isNotNull()
            & (F.length("review_id") > 0)
            & F.col("order_id").isNotNull()
            & (F.length("order_id") > 0)
        )
        .distinct()
    )

    # 2. Lọc các nhà bán hàng có mặt trong đơn hàng
    seller_relations = (
        order_items.select(
            F.trim("order_id").alias("order_id"),
            F.trim("seller_id").alias("seller_id"),
        )
        .where(F.col("order_id").isNotNull() & (F.length("order_id") > 0))
        .distinct()
    )

    # 3. Chuẩn bị bảng tra cứu dim_seller có kiểm định an toàn
    seller_lookup = prepare_dimension_lookup(
        dim_seller,
        "seller_id",
        "seller_key",
        "seller",
    ).select(
        F.col("seller_id").alias("_dimension_seller_id"),
        F.col("seller_key").cast("long"),
    )

    # 4. Ghép nối và sinh khóa ngoại seller_key
    return (
        review_grain.join(seller_relations, "order_id", "inner")
        .join(
            seller_lookup,
            F.col("seller_id") == F.col("_dimension_seller_id"),
            "left",
        )
        .select(
            F.col("review_id").cast("string"),
            F.col("order_id").cast("string"),
            # Nếu seller không tìm thấy trong dim_seller, gán về khóa 0 (Unknown)
            F.coalesce(F.col("seller_key"), F.lit(0)).cast("long").alias("seller_key"),
        )
        .distinct()
    )
