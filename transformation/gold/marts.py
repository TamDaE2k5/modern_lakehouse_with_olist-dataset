"""Các bảng tổng hợp nghiệp vụ cấp cao (Analytical Data Marts) ở tầng Gold.

Kiến trúc và Vai trò của Tầng Data Marts trong Mô hình Kimball:
---------------------------------------------------------------
1. Định nghĩa và Mục đích:
   - Trong kiến trúc Kimball Data Warehouse / Lakehouse, Data Marts là các bảng dữ liệu tổng hợp
     (Aggregated / Dimensional Marts) phục vụ trực tiếp cho các phòng ban, bảng điều khiển BI
     (PowerBI, Tableau) hoặc các báo cáo quản trị cấp cao.
   - Người dùng cuối (Business Analyst, Giám đốc) thường không muốn tự viết các câu SQL phức tạp
     GROUP BY hàng triệu dòng từ các bảng Fact nguyên tử. Data Marts giải quyết bài toán này bằng cách
     tính toán sẵn (pre-computed / aggregated) theo các chu kỳ và lát cắt kinh doanh quan trọng.

2. 3 Data Marts trọng điểm của Olist Lakehouse:
   - `mart_daily_revenue`:
     + Hạt (Grain): 1 dòng cho mỗi bộ `(revenue_date, customer_state, product_category)`.
     + Đo lường: Doanh thu bán hàng (`item_revenue`), cước vận chuyển (`freight_total`), số lượng sản phẩm
       bán ra (`item_count`), số đơn hàng duy nhất (`order_count`), và số khách hàng duy nhất (`customer_count`).
     + Ứng dụng: Báo cáo xu hướng tăng trưởng doanh thu theo ngày, theo từng bang và ngành hàng.

   - `mart_seller_performance`:
     + Hạt (Grain): 1 dòng cho mỗi bộ `(month_start, seller_key)`.
     + Đo lường toàn diện hiệu suất của Người bán theo tháng: Doanh thu, số lượng đơn, thời gian giao hàng
       trung bình (`average_delivery_days`), tỷ lệ giao đúng hạn (`on_time_delivery_rate`), số lượng đánh giá
       và điểm review trung bình (`average_review_score`).
     + Kỹ thuật Drill-Across: Tổng hợp độc lập 3 nhánh (Doanh thu từ fact_order_items, Vận chuyển từ fact_orders,
       và Đánh giá từ fact_reviews qua bridge_review_sellers) rồi ghép lại trên khóa (tháng, seller_key).
       Triệt tiêu 100% rủi ro nhân dòng (Cartesian product).

   - `mart_product_performance`:
     + Hạt (Grain): 1 dòng cho mỗi bộ `(month_start, product_category)`.
     + Đo lường hiệu quả kinh doanh của từng Danh mục ngành hàng theo tháng: Doanh số, sản lượng bán,
       và mức độ hài lòng của khách hàng (điểm review trung bình sau khi duyệt qua bridge_review_products).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from transformation.gold.keys import prepare_dimension_lookup


# Định dạng số thực và tiền tệ chính xác cao
MONEY_TYPE = DecimalType(18, 2)
AVERAGE_TYPE = DecimalType(10, 2)
RATE_TYPE = DecimalType(5, 4)
REVIEW_AVERAGE_TYPE = DecimalType(4, 2)


@dataclass(frozen=True)
class MetricDefinition:
    """Định nghĩa metadata của từng chỉ số đo lường phục vụ quản trị dữ liệu (Data Governance)."""

    name: str
    definition: str


# Từ điển metadata lưu trữ công thức tính toán của các chỉ số trong từng Mart
MART_METRIC_DEFINITIONS: dict[str, tuple[MetricDefinition, ...]] = {
    "mart_daily_revenue": (
        MetricDefinition("item_revenue", "SUM(fact_order_items.price)"),
        MetricDefinition("freight_total", "SUM(fact_order_items.freight_value)"),
        MetricDefinition("item_count", "COUNT(order-line grain)"),
        MetricDefinition("order_count", "COUNT(DISTINCT order_id)"),
        MetricDefinition("customer_count", "COUNT(DISTINCT customer_key)"),
    ),
    "mart_seller_performance": (
        MetricDefinition("item_revenue", "SUM(fact_order_items.price)"),
        MetricDefinition("freight_total", "SUM(fact_order_items.freight_value)"),
        MetricDefinition("item_count", "COUNT(order-line grain)"),
        MetricDefinition("order_count", "COUNT(DISTINCT order_id)"),
        MetricDefinition(
            "average_delivery_days",
            "AVG(fact_orders.delivery_duration_days) trên các quan hệ seller-order duy nhất",
        ),
        MetricDefinition(
            "on_time_delivery_rate",
            "AVG(NOT fact_orders.is_late_delivery) trên các đơn hàng đã giao thành công của người bán",
        ),
        MetricDefinition(
            "review_count",
            "COUNT(DISTINCT (review_id, order_id)) sau khi duyệt qua bảng cầu nối bridge_review_sellers",
        ),
        MetricDefinition(
            "average_review_score",
            "AVG(fact_reviews.review_score) trên các quan hệ review-order duy nhất",
        ),
    ),
    "mart_product_performance": (
        MetricDefinition("item_revenue", "SUM(fact_order_items.price)"),
        MetricDefinition("freight_total", "SUM(fact_order_items.freight_value)"),
        MetricDefinition("item_count", "COUNT(order-line grain)"),
        MetricDefinition("order_count", "COUNT(DISTINCT order_id)"),
        MetricDefinition(
            "review_count",
            "COUNT(DISTINCT (review_id, order_id)) sau khi duyệt qua bảng cầu nối bridge_review_products",
        ),
        MetricDefinition(
            "average_review_score",
            "AVG(fact_reviews.review_score) trên các quan hệ review-order-category duy nhất",
        ),
    ),
}


# ==============================================================================
# SCHEMA CỦA CÁC BẢNG DATA MART
# ==============================================================================

MART_DAILY_REVENUE_SCHEMA = StructType(
    [
        StructField("revenue_date", DateType(), True),
        StructField("customer_state", StringType(), False),
        StructField("product_category", StringType(), False),
        StructField("item_revenue", MONEY_TYPE, True),
        StructField("freight_total", MONEY_TYPE, True),
        StructField("item_count", LongType(), False),
        StructField("order_count", LongType(), False),
        StructField("customer_count", LongType(), False),
    ]
)

MART_SELLER_PERFORMANCE_SCHEMA = StructType(
    [
        StructField("month_start", DateType(), True),
        StructField("seller_key", LongType(), False),
        StructField("seller_id", StringType(), True),
        StructField("seller_state", StringType(), False),
        StructField("item_revenue", MONEY_TYPE, True),
        StructField("freight_total", MONEY_TYPE, True),
        StructField("item_count", LongType(), False),
        StructField("order_count", LongType(), False),
        StructField("average_delivery_days", AVERAGE_TYPE, True),
        StructField("on_time_delivery_rate", RATE_TYPE, True),
        StructField("review_count", LongType(), False),
        StructField("average_review_score", REVIEW_AVERAGE_TYPE, True),
    ]
)

MART_PRODUCT_PERFORMANCE_SCHEMA = StructType(
    [
        StructField("month_start", DateType(), True),
        StructField("product_category", StringType(), False),
        StructField("item_revenue", MONEY_TYPE, True),
        StructField("freight_total", MONEY_TYPE, True),
        StructField("item_count", LongType(), False),
        StructField("order_count", LongType(), False),
        StructField("review_count", LongType(), False),
        StructField("average_review_score", REVIEW_AVERAGE_TYPE, True),
    ]
)


# ==============================================================================
# CÁC HÀM TRỢ GIÚP (HELPERS)
# ==============================================================================

def _month_start(column: str) -> F.Column:
    """Rút gọn một ngày về ngày đầu tiên của tháng tương ứng (ví dụ: 2017-08-15 -> 2017-08-01)."""
    return F.trunc(F.col(column), "month").cast("date")


def _validated_attributes(
    dimension: DataFrame,
    *,
    natural_key: str,
    surrogate_key: str,
    lookup_name: str,
    attributes: tuple[str, ...],
) -> DataFrame:
    """Tra cứu, chuẩn hóa thuộc tính Dimension có kiểm định an toàn và gợi ý Broadcast Join.

    Đầu vào : bảng Chiều + tên khóa tự nhiên/khóa đại diện + danh sách thuộc tính cần lấy.
    Xử lý   : kiểm định ánh xạ 1-1, rồi rút gọn còn khóa đại diện kèm các thuộc tính cần thiết.
    Đầu ra  : DataFrame tra cứu đã bọc ``F.broadcast()``.

    VÌ SAO BỌC BROADCAST? Các Data Mart đều nối bảng Fact lớn với các lát cắt rất nhỏ của bảng
    Chiều (ví dụ chỉ 2 cột ``product_key`` + ``product_category_name_english``). Broadcast giúp
    toàn bộ phép nối diễn ra ngay trong bộ nhớ của executor, loại bỏ shuffle — đặc biệt đáng giá
    với cấu hình Spark Worker khiêm tốn 2 nhân / 4 GiB RAM của cụm hiện tại.
    """
    prepare_dimension_lookup(
        dimension,
        natural_key,
        surrogate_key,
        lookup_name,
    )
    return F.broadcast(
        dimension.select(
            F.col(surrogate_key).cast("long"),
            *(F.col(name) for name in attributes),
        ).distinct()
    )


# ==============================================================================
# CÁC HÀM XÂY DỰNG DATA MART (MART BUILDERS)
# ==============================================================================

def build_mart_daily_revenue(
    fact_order_items: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """Xây dựng Mart Doanh thu theo Ngày (mart_daily_revenue).

    Hạt: 1 dòng cho mỗi bộ (revenue_date, customer_state, product_category).

    Quy trình:
    1. Làm giàu fact_order_items với thông tin bang của khách hàng (customer_state từ dim_customer).
    2. Làm giàu với tên danh mục ngành hàng tiếng Anh (product_category từ dim_product).
    3. Gom nhóm theo (ngày, bang, danh mục) và tính tổng doanh thu, cước, số lượng món,
       số đơn duy nhất và số khách hàng duy nhất.
    """
    # 1. Trích xuất thuộc tính bang của khách hàng
    customers = _validated_attributes(
        dim_customer,
        natural_key="customer_unique_id",
        surrogate_key="customer_key",
        lookup_name="customer",
        attributes=("customer_state",),
    )

    # 2. Trích xuất thuộc tính danh mục ngành hàng tiếng Anh của sản phẩm
    products = _validated_attributes(
        dim_product,
        natural_key="product_id",
        surrogate_key="product_key",
        lookup_name="product",
        attributes=("product_category_name_english",),
    )

    # 3. Ghép thông tin và xử lý giá trị mặc định cho dữ liệu thiếu
    enriched = (
        fact_order_items.join(customers, "customer_key", "left")
        .join(products, "product_key", "left")
        .select(
            F.col("order_date").cast("date").alias("revenue_date"),
            F.coalesce(F.col("customer_state"), F.lit("Unknown")).alias(
                "customer_state"
            ),
            F.coalesce(
                F.col("product_category_name_english"), F.lit("unknown")
            ).alias("product_category"),
            "order_id",
            "customer_key",
            F.col("price").cast(MONEY_TYPE).alias("price"),
            F.col("freight_value").cast(MONEY_TYPE).alias("freight_value"),
        )
    )

    # 4. Gom nhóm tổng hợp chỉ số
    return enriched.groupBy(
        "revenue_date", "customer_state", "product_category"
    ).agg(
        F.sum("price").cast(MONEY_TYPE).alias("item_revenue"),
        F.sum("freight_value").cast(MONEY_TYPE).alias("freight_total"),
        F.count(F.lit(1)).cast("long").alias("item_count"),
        F.countDistinct("order_id").cast("long").alias("order_count"),
        F.countDistinct("customer_key").cast("long").alias("customer_count"),
    )


def build_mart_seller_performance(
    fact_order_items: DataFrame,
    fact_orders: DataFrame,
    fact_reviews: DataFrame,
    bridge_review_sellers: DataFrame,
    dim_seller: DataFrame,
) -> DataFrame:
    """Xây dựng Mart Hiệu suất Người bán theo Tháng (mart_seller_performance).

    Hạt: 1 dòng cho mỗi bộ (month_start, seller_key).

    Áp dụng Kỹ thuật Kimball Drill-Across kinh điển:
    - Nhánh 1 (Doanh thu): Tính từ fact_order_items gom nhóm theo (tháng, seller_key).
    - Nhánh 2 (Vận chuyển): Nối danh sách đơn hàng của người bán với fact_orders để tính
      thời gian giao hàng trung bình và tỷ lệ giao đúng hẹn.
    - Nhánh 3 (Đánh giá): Nối danh sách đơn hàng với bridge_review_sellers và fact_reviews
      để tính số lượng đánh giá và điểm review trung bình của riêng người bán đó.
    - Ghép nối an toàn 3 nhánh độc lập lại trên khóa [month_start, seller_key].
    """
    # Trích xuất thông tin người bán
    sellers = _validated_attributes(
        dim_seller,
        natural_key="seller_id",
        surrogate_key="seller_key",
        lookup_name="seller",
        attributes=("seller_id", "seller_state"),
    )

    # Nhánh 1: Tổng hợp doanh số bán theo tháng và người bán
    revenue = (
        fact_order_items.withColumn("month_start", _month_start("order_date"))
        .groupBy("month_start", "seller_key")
        .agg(
            F.sum(F.col("price").cast(MONEY_TYPE))
            .cast(MONEY_TYPE)
            .alias("item_revenue"),
            F.sum(F.col("freight_value").cast(MONEY_TYPE))
            .cast(MONEY_TYPE)
            .alias("freight_total"),
            F.count(F.lit(1)).cast("long").alias("item_count"),
            F.countDistinct("order_id").cast("long").alias("order_count"),
        )
    )

    # Danh sách đơn hàng duy nhất ứng với từng người bán trong tháng
    seller_orders = (
        fact_order_items.select(
            _month_start("order_date").alias("month_start"),
            F.coalesce(F.col("seller_key"), F.lit(0)).cast("long").alias(
                "seller_key"
            ),
            F.col("order_id").cast("string"),
        ).distinct()
    )

    # Nhánh 2: Tổng hợp hiệu suất vận chuyển từ fact_orders
    delivery = seller_orders.join(
        fact_orders.select(
            "order_id", "delivery_duration_days", "is_late_delivery"
        ),
        "order_id",
        "left",
    ).groupBy("month_start", "seller_key").agg(
        F.avg(F.col("delivery_duration_days").cast(AVERAGE_TYPE))
        .cast(AVERAGE_TYPE)
        .alias("average_delivery_days"),
        F.avg(
            F.when(
                F.col("delivery_duration_days").isNotNull(),
                (~F.coalesce(F.col("is_late_delivery"), F.lit(False))).cast("int"),
            )
        )
        .cast(RATE_TYPE)
        .alias("on_time_delivery_rate"),
    )

    # Nhánh 3: Tổng hợp đánh giá từ fact_reviews thông qua cầu nối bridge_review_sellers
    review_relations = (
        bridge_review_sellers.join(
            fact_reviews.select("review_id", "order_id", "review_score"),
            ["review_id", "order_id"],
            "inner",
        )
        .join(seller_orders, ["order_id", "seller_key"], "inner")
        .select(
            "month_start", "seller_key", "review_id", "order_id", "review_score"
        )
        .distinct()
    )
    review_metrics = review_relations.groupBy("month_start", "seller_key").agg(
        F.countDistinct(F.struct("review_id", "order_id"))
        .cast("long")
        .alias("review_count"),
        F.avg(F.col("review_score").cast(REVIEW_AVERAGE_TYPE))
        .cast(REVIEW_AVERAGE_TYPE)
        .alias("average_review_score"),
    )

    # Ghép 3 nhánh và nối thông tin người bán
    return (
        revenue.join(delivery, ["month_start", "seller_key"], "left")
        .join(review_metrics, ["month_start", "seller_key"], "left")
        .join(sellers, "seller_key", "left")
        .select(
            "month_start",
            F.coalesce(F.col("seller_key"), F.lit(0)).cast("long").alias(
                "seller_key"
            ),
            F.col("seller_id").cast("string"),
            F.coalesce(F.col("seller_state"), F.lit("Unknown")).alias(
                "seller_state"
            ),
            F.col("item_revenue").cast(MONEY_TYPE),
            F.col("freight_total").cast(MONEY_TYPE),
            F.col("item_count").cast("long"),
            F.col("order_count").cast("long"),
            F.col("average_delivery_days").cast(AVERAGE_TYPE),
            F.col("on_time_delivery_rate").cast(RATE_TYPE),
            F.coalesce(F.col("review_count"), F.lit(0)).cast("long").alias(
                "review_count"
            ),
            F.col("average_review_score").cast(REVIEW_AVERAGE_TYPE),
        )
    )


def build_mart_product_performance(
    fact_order_items: DataFrame,
    fact_reviews: DataFrame,
    bridge_review_products: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """Xây dựng Mart Hiệu suất Ngành hàng Sản phẩm theo Tháng (mart_product_performance).

    Hạt: 1 dòng cho mỗi bộ (month_start, product_category).

    Quy trình:
    1. Nhánh Doanh thu: Tính tổng tiền hàng, tiền ship, số lượng món và số đơn duy nhất
       theo từng danh mục sản phẩm trong mỗi tháng.
    2. Nhánh Đánh giá: Duyệt qua bảng cầu nối bridge_review_products để lấy đúng các đánh giá
       cho sản phẩm thuộc danh mục đó mà không làm nhân bản số dòng đánh giá.
    3. Ghép nối kết quả theo [month_start, product_category].
    """
    # Trích xuất danh mục ngành hàng tiếng Anh
    products = _validated_attributes(
        dim_product,
        natural_key="product_id",
        surrogate_key="product_key",
        lookup_name="product",
        attributes=("product_category_name_english",),
    ).withColumn(
        "product_category",
        F.coalesce(F.col("product_category_name_english"), F.lit("unknown")),
    )

    # Làm giàu fact_order_items với product_category
    enriched_items = fact_order_items.join(
        products.select("product_key", "product_category"), "product_key", "left"
    ).withColumn(
        "product_category",
        F.coalesce(F.col("product_category"), F.lit("unknown")),
    )

    # Nhánh 1: Doanh số theo tháng và danh mục
    revenue = (
        enriched_items.withColumn("month_start", _month_start("order_date"))
        .groupBy("month_start", "product_category")
        .agg(
            F.sum(F.col("price").cast(MONEY_TYPE))
            .cast(MONEY_TYPE)
            .alias("item_revenue"),
            F.sum(F.col("freight_value").cast(MONEY_TYPE))
            .cast(MONEY_TYPE)
            .alias("freight_total"),
            F.count(F.lit(1)).cast("long").alias("item_count"),
            F.countDistinct("order_id").cast("long").alias("order_count"),
        )
    )

    # Danh sách các sản phẩm và danh mục xuất hiện trong các đơn hàng
    order_products = enriched_items.select(
        _month_start("order_date").alias("month_start"),
        "order_id",
        "product_key",
        "product_category",
    ).distinct()

    # Nhánh 2: Tổng hợp đánh giá từ fact_reviews qua bridge_review_products
    review_relations = (
        bridge_review_products.join(
            fact_reviews.select("review_id", "order_id", "review_score"),
            ["review_id", "order_id"],
            "inner",
        )
        .join(order_products, ["order_id", "product_key"], "inner")
        .select(
            "month_start",
            "product_category",
            "review_id",
            "order_id",
            "review_score",
        )
        .distinct()
    )
    review_metrics = review_relations.groupBy(
        "month_start", "product_category"
    ).agg(
        F.countDistinct(F.struct("review_id", "order_id"))
        .cast("long")
        .alias("review_count"),
        F.avg(F.col("review_score").cast(REVIEW_AVERAGE_TYPE))
        .cast(REVIEW_AVERAGE_TYPE)
        .alias("average_review_score"),
    )

    # Ghép 2 nhánh lại thành bảng mart hoàn chỉnh
    return revenue.join(
        review_metrics, ["month_start", "product_category"], "left"
    ).select(
        "month_start",
        "product_category",
        F.col("item_revenue").cast(MONEY_TYPE),
        F.col("freight_total").cast(MONEY_TYPE),
        F.col("item_count").cast("long"),
        F.col("order_count").cast("long"),
        F.coalesce(F.col("review_count"), F.lit(0)).cast("long").alias(
            "review_count"
        ),
        F.col("average_review_score").cast(REVIEW_AVERAGE_TYPE),
    )
