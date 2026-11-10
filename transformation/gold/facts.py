"""Các hàm xây dựng Bảng Sự Kiện (Fact Tables) ở tầng Gold trong mô hình Star Schema.

Kiến trúc và Nguyên lý Mô hình hóa Dữ liệu Sự Kiện (Kimball Dimensional Modeling):
-----------------------------------------------------------------------------------
1. Khái niệm Hạt Dữ liệu (Grain):
   - "Hạt" là mức độ chi tiết nhỏ nhất của một dòng dữ liệu trong bảng Fact.
   - Nguyên tắc vàng của Ralph Kimball: "Mỗi bảng Fact phải có một Hạt dữ liệu duy nhất và nhất quán."
   - Không bao giờ được trộn lẫn các mức hạt khác nhau vào cùng một bảng sự kiện vì sẽ gây nhân đôi
     doanh thu (Double-Counting) hoặc tích Đề-các (Cartesian Explosion).

2. 4 Bảng Fact tác nghiệp chính của Olist Lakehouse:
   - `fct_orders`: Hạt = 1 dòng cho mỗi `order_id`.
     Là bảng tổng quan đơn hàng, chứa các chỉ số tổng hợp cấp đơn (tổng tiền hàng, tiền cước,
     tổng thanh toán, thời gian giao hàng, độ trễ và điểm đánh giá trung bình).
     Các bảng con (items, payments, reviews) được tính tổng trước (pre-aggregate) độc lập
     rồi mới JOIN 1-1 vào đơn hàng, triệt tiêu hoàn toàn nguy cơ nhân dòng.

   - `fct_order_items`: Hạt = 1 dòng cho mỗi `(order_id, order_item_id)`.
     Là bảng sự kiện doanh thu hạt nhân (authoritative revenue grain) - nơi đo lường chính xác
     từng món hàng được bán ra, gắn liền với các chiều: Khách hàng, Sản phẩm, Người bán, Ngày đặt hàng, Hạn giao hàng.

   - `fct_payments`: Hạt = 1 dòng cho mỗi `(order_id, payment_sequential)`.
     Bảo toàn từng đợt thanh toán thành phần (split payments - ví dụ: khách trả 1 phần bằng voucher,
     phần còn lại quẹt thẻ tín dụng). Không gộp dòng thanh toán để phục vụ phân tích tài chính/cổng thanh toán.

   - `fct_reviews`: Hạt = 1 dòng cho mỗi `(review_id, order_id)`.
     Lưu trữ điểm số đánh giá, độ dài phản hồi, cờ có tiêu đề/nội dung. Không gắn trực tiếp `product_key`
     hay `seller_key` vào đây để tránh nhân dòng; việc phân tích review theo sản phẩm/người bán được
     ủy quyền cho các Bảng Cầu Nối (Bridge Tables).

3. Kỹ thuật Chiều Thời gian đóng nhiều vai trò (Role-Playing Dimensions):
   - Một đơn hàng trải qua nhiều mốc thời gian: Ngày đặt, Ngày duyệt, Ngày giao cho bưu cục,
     Ngày khách nhận hàng, Ngày hẹn giao.
   - Thay vì tạo 5 bảng ngày riêng biệt, ta dùng DUY NHẤT một bảng `dim_date`, nhưng JOIN nhiều lần
     với các tên khóa ngoại đại diện khác nhau (`purchase_date_key`, `approval_date_key`, ...).

4. Bản ghi cha đến muộn (Late-Arriving Parent / Inferred Stub Orders):
   - Nếu một `order_id` xuất hiện trong bảng items hoặc payments nhưng bị thiếu ở bảng orders chính:
     Hệ thống tự động sinh một dòng "giữ chỗ" (stub record) trong `fct_orders` với trạng thái Unknown,
     đảm bảo không bao giờ bị rơi rụng doanh thu (Lossless Accounting).
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformation.gold.keys import prepare_dimension_lookup


# Định dạng tiền tệ chính xác cao (18 chữ số tổng cộng, 2 số thập phân)
MONEY_TYPE = DecimalType(18, 2)

# Định dạng điểm đánh giá trung bình (4 chữ số tổng cộng, 2 số thập phân, ví dụ: 4.85)
REVIEW_AVERAGE_TYPE = DecimalType(4, 2)


# ==============================================================================
# SCHEMA CỦA CÁC BẢNG SỰ KIỆN (FACT SCHEMAS)
# ==============================================================================

FACT_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_key", LongType(), False),
        StructField("purchase_date_key", IntegerType(), False),
        StructField("approval_date_key", IntegerType(), False),
        StructField("carrier_date_key", IntegerType(), False),
        StructField("delivered_date_key", IntegerType(), False),
        StructField("estimated_delivery_date_key", IntegerType(), False),
        StructField("purchase_timestamp", TimestampType(), True),
        StructField("order_status", StringType(), True),
        StructField("total_items", LongType(), False),
        StructField("item_revenue", MONEY_TYPE, False),
        StructField("freight_total", MONEY_TYPE, False),
        StructField("payment_total", MONEY_TYPE, False),
        StructField("delivery_duration_days", IntegerType(), True),
        StructField("carrier_delivery_duration_days", IntegerType(), True),
        StructField("is_late_delivery", BooleanType(), True),
        StructField("average_review_score", REVIEW_AVERAGE_TYPE, True),
    ]
)

FACT_ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("order_item_id", IntegerType(), True),
        StructField("customer_key", LongType(), False),
        StructField("product_key", LongType(), False),
        StructField("seller_key", LongType(), False),
        StructField("order_date_key", IntegerType(), False),
        StructField("shipping_limit_date_key", IntegerType(), False),
        StructField("order_date", DateType(), True),
        StructField("shipping_limit_date", TimestampType(), True),
        StructField("price", MONEY_TYPE, True),
        StructField("freight_value", MONEY_TYPE, True),
    ]
)

FACT_PAYMENTS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", IntegerType(), True),
        StructField("customer_key", LongType(), False),
        StructField("order_date_key", IntegerType(), False),
        StructField("order_date", DateType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", MONEY_TYPE, True),
    ]
)

FACT_REVIEWS_SCHEMA = StructType(
    [
        StructField("review_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_key", LongType(), False),
        StructField("review_date_key", IntegerType(), False),
        StructField("answer_date_key", IntegerType(), False),
        StructField("review_date", DateType(), True),
        StructField("review_answer_timestamp", TimestampType(), True),
        StructField("review_score", IntegerType(), True),
        StructField("review_comment_title", StringType(), True),
        StructField("review_comment_message", StringType(), True),
        StructField("has_title", BooleanType(), True),
        StructField("has_message", BooleanType(), True),
    ]
)


# ==============================================================================
# CÁC HÀM TRỢ GIÚP TRA CỨU KHÓA ĐẠI DIỆN (LOOKUP HELPERS)
# ==============================================================================

def _normalized_lookup(
    dimension: DataFrame,
    natural_key: str,
    surrogate_key: str,
    lookup_name: str,
) -> DataFrame:
    """Tạo bảng tra cứu chuẩn hóa, kiểm tra toàn vẹn (Safety Gate) và gợi ý Broadcast Join.

    Đầu vào : một bảng Chiều (dim_customer / dim_product / dim_seller).
    Xử lý   : kiểm định ánh xạ 1-1 rồi rút gọn còn đúng 2 cột (khóa tự nhiên, khóa đại diện).
    Đầu ra  : DataFrame tra cứu đã được bọc gợi ý ``F.broadcast()``.

    VÌ SAO BỌC BROADCAST? Bảng Fact rất lớn (``fact_order_items`` có 112.650 dòng và sẽ còn
    phình to theo thời gian), trong khi bảng tra cứu chỉ còn 2 cột của bảng Chiều — nặng nhất
    là dim_customer cũng chỉ khoảng 96.000 dòng (~5 MB). Nếu để Spark tự chọn, nó sẽ dùng
    Sort-Merge Join: băm và chuyển (shuffle) CẢ HAI phía qua mạng, trong đó phía Fact là cực kỳ
    tốn kém. Gợi ý ``broadcast`` yêu cầu Spark gửi nguyên bảng tra cứu nhỏ tới từng executor,
    nhờ đó bảng Fact được nối tại chỗ và **giai đoạn shuffle bị loại bỏ hoàn toàn**.

    Vì sao dùng gợi ý tường minh mà không dựa vào ngưỡng tự động (``autoBroadcastJoinThreshold``
    mặc định 10 MB)? Vì ngưỡng đó dựa trên ước lượng kích thước của Spark, mà với nguồn Iceberg
    ước lượng này thường bị thổi phồng khiến Spark âm thầm quay về Sort-Merge Join. Gợi ý tường
    minh cho kết quả ổn định, không phụ thuộc chất lượng ước lượng thống kê.
    """
    return F.broadcast(
        prepare_dimension_lookup(
            dimension,
            natural_key,
            surrogate_key,
            lookup_name,
        ).select(
            F.col(natural_key).alias(f"_{lookup_name}_natural_key"),
            F.col(surrogate_key).cast("long").alias(f"_{lookup_name}_surrogate_key"),
        )
    )


def _order_customer_context(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """Ánh xạ từ order_id sang customer_unique_id.

    Trong Olist:
    - Bảng orders có `customer_id` (mã phiên giao dịch của khách).
    - Bảng customers ánh xạ `customer_id` sang `customer_unique_id` (mã định danh thực của khách hàng).
    Hàm này tạo bảng tra cứu gọn: order_id -> customer_unique_id để Fact dễ dàng liên kết sang dim_customer.
    """
    # Bọc broadcast: bảng ánh xạ này chỉ gồm 2 cột mã định danh (~99.000 dòng, vài MB),
    # trong khi phía orders là bảng lớn — đúng hình mẫu kinh điển của Broadcast Hash Join.
    customer_relation = F.broadcast(
        customers.select(
            F.trim(F.col("customer_id").cast("string")).alias("_customer_id"),
            F.trim(F.col("customer_unique_id").cast("string")).alias(
                "customer_unique_id"
            ),
        ).distinct()
    )
    return (
        orders.select(
            F.trim(F.col("order_id").cast("string")).alias("order_id"),
            F.trim(F.col("customer_id").cast("string")).alias("_customer_id"),
        )
        .join(customer_relation, "_customer_id", "left")
        .select("order_id", "customer_unique_id")
    )


def _with_customer_key(
    df: DataFrame,
    dim_customer: DataFrame,
    *,
    customer_column: str = "customer_unique_id",
) -> DataFrame:
    """Ghép nối (LEFT JOIN) với dim_customer để lấy khóa đại diện `customer_key`."""
    lookup = _normalized_lookup(
        dim_customer,
        "customer_unique_id",
        "customer_key",
        "customer",
    )
    return (
        df.join(
            lookup,
            F.trim(F.col(customer_column).cast("string"))
            == F.col("_customer_natural_key"),
            "left",
        )
        .drop("_customer_natural_key")
        .withColumn(
            "customer_key",
            # Nếu không tìm thấy khách hàng tương ứng, gán khóa 0 (Unknown Customer)
            F.coalesce(F.col("_customer_surrogate_key"), F.lit(0)).cast("long"),
        )
        .drop("_customer_surrogate_key")
    )


def _with_member_key(
    df: DataFrame,
    dimension: DataFrame,
    *,
    natural_key: str,
    surrogate_key: str,
    lookup_name: str,
) -> DataFrame:
    """Ghép nối tổng quát với một bảng Chiều (dim_product, dim_seller) để lấy khóa đại diện."""
    lookup = _normalized_lookup(dimension, natural_key, surrogate_key, lookup_name)
    return (
        df.join(
            lookup,
            F.trim(F.col(natural_key).cast("string"))
            == F.col(f"_{lookup_name}_natural_key"),
            "left",
        )
        .drop(f"_{lookup_name}_natural_key")
        .withColumn(
            surrogate_key,
            # Nếu không tìm thấy, fallback an toàn về khóa số 0 (Unknown)
            F.coalesce(
                F.col(f"_{lookup_name}_surrogate_key"), F.lit(0)
            ).cast("long"),
        )
        .drop(f"_{lookup_name}_surrogate_key")
    )


def _date_lookup(dim_date: DataFrame) -> DataFrame:
    """Tạo bảng tra cứu ngày: full_date ('yyyy-MM-dd') -> date_key (YYYYMMDD)."""
    # Bọc broadcast: dim_date chỉ có vài nghìn dòng (1 dòng/ngày) nhưng được JOIN tới 5 lần
    # cho 5 vai trò ngày khác nhau (Role-Playing Dimension). Nếu không broadcast, mỗi lần JOIN
    # là một lần shuffle toàn bộ bảng Fact — tức 5 lần shuffle thừa trong cùng một job.
    return F.broadcast(
        prepare_dimension_lookup(
            dim_date,
            "full_date",
            "date_key",
            "date",
        ).select(
            F.col("full_date").alias("_date_natural_key"),
            F.col("date_key").cast("int").alias("_date_surrogate_key"),
        )
    )


def _with_date_roles(
    df: DataFrame,
    dim_date: DataFrame,
    roles: Sequence[tuple[str, str]],
) -> DataFrame:
    """Áp dụng kỹ thuật Role-Playing Dimension: gán date_key cho nhiều cột ngày khác nhau.

    Tham số roles là danh sách các cặp (cột_thời_gian_nguồn, tên_cột_khóa_ngoại_mục_tiêu).
    Ví dụ: [("order_purchase_timestamp", "purchase_date_key"), ...]
    """
    lookup = _date_lookup(dim_date)
    result = df
    for source_column, key_column in roles:
        natural_alias = f"_{key_column}_natural"
        key_alias = f"_{key_column}_resolved"
        role_lookup = lookup.select(
            F.col("_date_natural_key").alias(natural_alias),
            F.col("_date_surrogate_key").alias(key_alias),
        )
        result = (
            result.join(
                role_lookup,
                F.date_format(F.to_date(F.col(source_column)), "yyyy-MM-dd")
                == F.col(natural_alias),
                "left",
            )
            .drop(natural_alias)
            .withColumn(
                key_column,
                # Nếu ngày bị NULL (chưa duyệt, chưa giao), gán về date_key = 0 (Unknown Date)
                F.coalesce(F.col(key_alias), F.lit(0)).cast("int"),
            )
            .drop(key_alias)
        )
    return result


# ==============================================================================
# HÀM XÂY DỰNG CÁC BẢNG SỰ KIỆN (FACT BUILDERS)
# ==============================================================================

def build_fact_orders(
    orders: DataFrame,
    customers: DataFrame,
    order_items: DataFrame,
    payments: DataFrame,
    reviews: DataFrame,
    dim_customer: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    """Xây dựng Bảng Sự Kiện Đơn Hàng (fct_orders) ở mức hạt 1 dòng / 1 order_id.

    Các bước xử lý cẩn trọng:
    1. Lấy dữ liệu cơ sở từ silver.orders.
    2. Quét tìm các đơn hàng mồ côi (chỉ có trong items/payments/reviews nhưng vắng mặt ở orders)
       để sinh các bản ghi cha tạm thời (inferred stub records), đảm bảo không mất doanh thu.
    3. Ánh xạ customer_key qua thông tin khách hàng.
    4. Ánh xạ 5 vai trò ngày (purchase, approval, carrier, delivered, estimated_delivery).
    5. Tính toán các chỉ số tổng hợp cấp đơn (pre-aggregate độc lập ở từng bảng con trước khi JOIN):
       - items: tổng số lượng món, tổng tiền hàng, tổng tiền ship.
       - payments: tổng tiền thanh toán thực tế.
       - reviews: điểm review trung bình.
    6. Ghép nối an toàn 1-1 và định dạng kiểu dữ liệu tiền tệ chính xác (Decimal(18,2)).
    """

    # 1. Trích xuất các trường cơ sở của đơn hàng
    base = orders.select(
        F.trim(F.col("order_id").cast("string")).alias("order_id"),
        F.trim(F.col("customer_id").cast("string")).alias("customer_id"),
        F.col("order_status").cast("string"),
        F.col("order_purchase_timestamp").cast("timestamp"),
        F.col("order_approved_at").cast("timestamp"),
        F.col("order_delivered_carrier_date").cast("timestamp"),
        F.col("order_delivered_customer_date").cast("timestamp"),
        F.col("order_estimated_delivery_date").cast("timestamp"),
        F.col("delivery_duration_days").cast("int"),
        F.col("carrier_delivery_duration_days").cast("int"),
        F.col("is_late_delivery").cast("boolean"),
    )

    # 2. Xử lý các đơn hàng đến muộn / mồ côi từ các bảng con
    child_purchase_dates = order_items.select(
        F.trim(F.col("order_id").cast("string")).alias("order_id"),
        F.col("order_purchase_timestamp").cast("timestamp").alias(
            "_child_purchase_timestamp"
        ),
    ).unionByName(
        payments.select(
            F.trim(F.col("order_id").cast("string")).alias("order_id"),
            F.col("order_purchase_timestamp").cast("timestamp").alias(
                "_child_purchase_timestamp"
            ),
        )
    )
    child_order_spine = (
        child_purchase_dates.unionByName(
            reviews.select(
                F.trim(F.col("order_id").cast("string")).alias("order_id"),
                F.lit(None).cast("timestamp").alias("_child_purchase_timestamp"),
            )
        )
        .where(F.col("order_id").isNotNull() & (F.length("order_id") > 0))
        .groupBy("order_id")
        .agg(F.min("_child_purchase_timestamp").alias("order_purchase_timestamp"))
    )

    # Tìm các order_id có trong con nhưng chưa có trong cha (anti-join)
    child_only_orders = child_order_spine.join(
        base.select("order_id"), "order_id", "left_anti"
    ).select(
        "order_id",
        F.lit(None).cast("string").alias("customer_id"),
        F.lit(None).cast("string").alias("order_status"),
        F.col("order_purchase_timestamp").cast("timestamp"),
        F.lit(None).cast("timestamp").alias("order_approved_at"),
        F.lit(None).cast("timestamp").alias("order_delivered_carrier_date"),
        F.lit(None).cast("timestamp").alias("order_delivered_customer_date"),
        F.lit(None).cast("timestamp").alias("order_estimated_delivery_date"),
        F.lit(None).cast("int").alias("delivery_duration_days"),
        F.lit(None).cast("int").alias("carrier_delivery_duration_days"),
        F.lit(None).cast("boolean").alias("is_late_delivery"),
    )
    base = base.unionByName(child_only_orders)

    # 3. Gắn khóa khách hàng (customer_key)
    customer_context = _order_customer_context(orders, customers)
    base = _with_customer_key(
        base.join(customer_context, "order_id", "left"), dim_customer
    )

    # 4. Gắn các khóa ngày theo vai trò
    base = _with_date_roles(
        base,
        dim_date,
        (
            ("order_purchase_timestamp", "purchase_date_key"),
            ("order_approved_at", "approval_date_key"),
            ("order_delivered_carrier_date", "carrier_date_key"),
            ("order_delivered_customer_date", "delivered_date_key"),
            ("order_estimated_delivery_date", "estimated_delivery_date_key"),
        ),
    )

    # 5. Tổng hợp độc lập từng bảng con trước khi JOIN (tránh tích Đề-các)
    item_totals = order_items.groupBy(
        F.trim(F.col("order_id").cast("string")).alias("order_id")
    ).agg(
        F.count(F.lit(1)).cast("long").alias("total_items"),
        F.sum(F.col("price").cast(MONEY_TYPE)).cast(MONEY_TYPE).alias("item_revenue"),
        F.sum(F.col("freight_value").cast(MONEY_TYPE))
        .cast(MONEY_TYPE)
        .alias("freight_total"),
    )

    payment_totals = payments.groupBy(
        F.trim(F.col("order_id").cast("string")).alias("order_id")
    ).agg(
        F.sum(F.col("payment_value").cast(MONEY_TYPE))
        .cast(MONEY_TYPE)
        .alias("payment_total")
    )

    review_totals = reviews.groupBy(
        F.trim(F.col("order_id").cast("string")).alias("order_id")
    ).agg(
        F.avg(F.col("review_score").cast("decimal(4,2)"))
        .cast(REVIEW_AVERAGE_TYPE)
        .alias("average_review_score")
    )

    # 6. Ghép nối các chỉ số vào bảng đơn hàng và xử lý giá trị mặc định cho tiền tệ
    zero_money = F.lit(Decimal("0.00")).cast(MONEY_TYPE)
    return (
        base.join(item_totals, "order_id", "left")
        .join(payment_totals, "order_id", "left")
        .join(review_totals, "order_id", "left")
        .select(
            F.col("order_id").cast("string"),
            F.col("customer_key").cast("long"),
            F.col("purchase_date_key").cast("int"),
            F.col("approval_date_key").cast("int"),
            F.col("carrier_date_key").cast("int"),
            F.col("delivered_date_key").cast("int"),
            F.col("estimated_delivery_date_key").cast("int"),
            F.col("order_purchase_timestamp").cast("timestamp").alias(
                "purchase_timestamp"
            ),
            F.col("order_status").cast("string"),
            F.coalesce(F.col("total_items"), F.lit(0)).cast("long").alias(
                "total_items"
            ),
            F.coalesce(F.col("item_revenue"), zero_money)
            .cast(MONEY_TYPE)
            .alias("item_revenue"),
            F.coalesce(F.col("freight_total"), zero_money)
            .cast(MONEY_TYPE)
            .alias("freight_total"),
            F.coalesce(F.col("payment_total"), zero_money)
            .cast(MONEY_TYPE)
            .alias("payment_total"),
            F.col("delivery_duration_days").cast("int"),
            F.col("carrier_delivery_duration_days").cast("int"),
            F.col("is_late_delivery").cast("boolean"),
            F.col("average_review_score").cast(REVIEW_AVERAGE_TYPE),
        )
        # V3 Optimization: Sắp xếp cục bộ theo trường phân vùng (purchase_timestamp) và khóa khách hàng
        # trước khi ghi xuống Apache Iceberg để tối ưu hóa nén Parquet và gom cụm thống kê min/max
        .sortWithinPartitions("purchase_timestamp", "customer_key")
    )


def build_fact_order_items(
    order_items: DataFrame,
    orders: DataFrame,
    customers: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
    dim_seller: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    """Xây dựng Bảng Sự Kiện Chi Tiết Món Hàng (fct_order_items) - Hạt 1 dòng / 1 món hàng.

    Đây là bảng sự kiện doanh thu quan trọng nhất của hệ thống:
    - Lưu lại giá tiền (price) và cước phí (freight_value) của từng mặt hàng.
    - Được liên kết đầy đủ với 4 chiều: Khách hàng (customer_key), Sản phẩm (product_key),
      Người bán (seller_key), và Ngày đặt / Hạn giao hàng (order_date_key, shipping_limit_date_key).
    """

    # 1. Trích xuất thông tin khách hàng từ orders -> customers
    context = _order_customer_context(orders, customers)

    # 2. Chuẩn bị bảng items cơ sở
    result = order_items.select(
        F.trim(F.col("order_id").cast("string")).alias("order_id"),
        F.col("order_item_id").cast("int"),
        F.trim(F.col("product_id").cast("string")).alias("product_id"),
        F.trim(F.col("seller_id").cast("string")).alias("seller_id"),
        F.col("shipping_limit_date").cast("timestamp"),
        F.col("price").cast(MONEY_TYPE),
        F.col("freight_value").cast(MONEY_TYPE),
        F.col("order_purchase_timestamp").cast("timestamp"),
    ).join(context, "order_id", "left")

    # 3. Gắn các khóa đại diện từ các Dimension
    result = _with_customer_key(result, dim_customer)
    result = _with_member_key(
        result,
        dim_product,
        natural_key="product_id",
        surrogate_key="product_key",
        lookup_name="product",
    )
    result = _with_member_key(
        result,
        dim_seller,
        natural_key="seller_id",
        surrogate_key="seller_key",
        lookup_name="seller",
    )

    # 4. Gắn các khóa ngày
    result = _with_date_roles(
        result,
        dim_date,
        (
            ("order_purchase_timestamp", "order_date_key"),
            ("shipping_limit_date", "shipping_limit_date_key"),
        ),
    )

    # 5. Chiếu về đúng schema đích của fct_order_items
    return result.select(
        F.col("order_id").cast("string"),
        F.col("order_item_id").cast("int"),
        F.col("customer_key").cast("long"),
        F.col("product_key").cast("long"),
        F.col("seller_key").cast("long"),
        F.col("order_date_key").cast("int"),
        F.col("shipping_limit_date_key").cast("int"),
        F.to_date("order_purchase_timestamp").alias("order_date"),
        F.col("shipping_limit_date").cast("timestamp"),
        F.col("price").cast(MONEY_TYPE),
        F.col("freight_value").cast(MONEY_TYPE),
    )


def build_fact_payments(
    payments: DataFrame,
    orders: DataFrame,
    customers: DataFrame,
    dim_customer: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    """Xây dựng Bảng Sự Kiện Thanh Toán (fct_payments) - Hạt 1 dòng / 1 lượt thanh toán thành phần.

    Đặc điểm:
    - Bảo toàn các đợt trả góp (payment_installments) và phương thức thanh toán (credit_card, boleto, voucher,...).
    - Giữ nguyên số thứ tự thanh toán (payment_sequential) khi một đơn thanh toán bằng nhiều nguồn.
    """

    context = _order_customer_context(orders, customers)
    result = payments.select(
        F.trim(F.col("order_id").cast("string")).alias("order_id"),
        F.col("payment_sequential").cast("int"),
        F.col("payment_type").cast("string"),
        F.col("payment_installments").cast("int"),
        F.col("payment_value").cast(MONEY_TYPE),
        F.col("order_purchase_timestamp").cast("timestamp"),
    ).join(context, "order_id", "left")

    result = _with_customer_key(result, dim_customer)
    result = _with_date_roles(
        result, dim_date, (("order_purchase_timestamp", "order_date_key"),)
    )

    return result.select(
        F.col("order_id").cast("string"),
        F.col("payment_sequential").cast("int"),
        F.col("customer_key").cast("long"),
        F.col("order_date_key").cast("int"),
        F.to_date("order_purchase_timestamp").alias("order_date"),
        F.col("payment_type").cast("string"),
        F.col("payment_installments").cast("int"),
        F.col("payment_value").cast(MONEY_TYPE),
    )


def build_fact_reviews(
    reviews: DataFrame,
    orders: DataFrame,
    customers: DataFrame,
    dim_customer: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    """Xây dựng Bảng Sự Kiện Đánh Giá (fct_reviews) - Hạt 1 dòng / 1 cặp (review_id, order_id).

    Lưu ý quan trọng:
    - Bảng này KHÔNG chứa product_key hay seller_key để tránh phá vỡ hạt (grain) và nhân bản điểm số.
    - Mọi quan hệ giữa Review và Sản phẩm/Người bán được truy vấn thông qua Bảng Cầu Nối (Bridge Tables).
    """

    context = _order_customer_context(orders, customers)
    result = reviews.select(
        F.trim(F.col("review_id").cast("string")).alias("review_id"),
        F.trim(F.col("order_id").cast("string")).alias("order_id"),
        F.col("review_score").cast("int"),
        F.col("review_comment_title").cast("string"),
        F.col("review_comment_message").cast("string"),
        F.col("review_creation_date").cast("timestamp"),
        F.col("review_answer_timestamp").cast("timestamp"),
        F.col("has_title").cast("boolean"),
        F.col("has_message").cast("boolean"),
    ).join(context, "order_id", "left")

    result = _with_customer_key(result, dim_customer)
    result = _with_date_roles(
        result,
        dim_date,
        (
            ("review_creation_date", "review_date_key"),
            ("review_answer_timestamp", "answer_date_key"),
        ),
    )

    return result.select(
        F.col("review_id").cast("string"),
        F.col("order_id").cast("string"),
        F.col("customer_key").cast("long"),
        F.col("review_date_key").cast("int"),
        F.col("answer_date_key").cast("int"),
        F.to_date("review_creation_date").alias("review_date"),
        F.col("review_answer_timestamp").cast("timestamp"),
        F.col("review_score").cast("int"),
        F.col("review_comment_title").cast("string"),
        F.col("review_comment_message").cast("string"),
        F.col("has_title").cast("boolean"),
        F.col("has_message").cast("boolean"),
    )
