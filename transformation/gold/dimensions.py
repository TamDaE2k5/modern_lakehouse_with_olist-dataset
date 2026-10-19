"""Xây dựng 5 bảng Kích thước (Dimensions) chuẩn hóa cho Tầng Gold theo mô hình Kimball Star Schema.

Các bảng Dimension cung cấp ngữ cảnh phân tích (Ai? Ở đâu? Cái gì? Khi nào?):
1. dim_date: Trục thời gian chuẩn hóa theo ngày (yyyyMMdd).
2. dim_geography: Chiều địa lý và 5 vùng kinh tế Brazil.
3. dim_product: Chiều sản phẩm, kích thước vật lý và danh mục tiếng Anh.
4. dim_seller: Chiều người bán và liên kết vùng địa lý.
5. dim_customer: Chiều khách hàng độc nhất trọn đời (customer_unique_id) và cờ khách hàng thân thiết.
"""

from __future__ import annotations

from typing import Sequence

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
)
from pyspark.sql.window import Window

from transformation.gold.keys import assign_dense_surrogate_keys, prepare_dimension_lookup

# Số ngày đệm an toàn trước ngày nhỏ nhất và sau ngày lớn nhất (mặc định 366 ngày = 1 năm nhuận)
DEFAULT_DATE_SAFETY_DAYS = 366

# --- Định nghĩa Schema chuẩn cho từng bảng Dimension ---

DIM_DATE_SCHEMA = StructType(
    [
        StructField("date_key", IntegerType(), True),
        StructField("full_date", DateType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("day_name", StringType(), True),
        StructField("day_of_month", IntegerType(), False),
        StructField("day_of_year", IntegerType(), False),
        StructField("week_of_year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("month_name", StringType(), True),
        StructField("quarter", IntegerType(), False),
        StructField("year", IntegerType(), False),
        StructField("is_weekend", BooleanType(), True),
    ]
)

DIM_GEOGRAPHY_SCHEMA = StructType(
    [
        StructField("geography_key", LongType(), False),
        StructField("zip_code_prefix", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("region", StringType(), False),
        StructField("latitude", DecimalType(10, 7), True),
        StructField("longitude", DecimalType(10, 7), True),
        StructField("source_observation_count", LongType(), False),
    ]
)

DIM_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_key", LongType(), False),
        StructField("customer_unique_id", StringType(), True),
        StructField("representative_customer_id", StringType(), True),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), False),
        StructField("customer_state", StringType(), False),
        StructField("geography_key", LongType(), False),
        StructField("order_count", LongType(), True),
        StructField("repeat_customer", BooleanType(), True),
    ]
)

DIM_PRODUCT_SCHEMA = StructType(
    [
        StructField("product_key", LongType(), False),
        StructField("product_id", StringType(), True),
        StructField("product_category_name", StringType(), False),
        StructField("product_category_name_english", StringType(), False),
        StructField("product_name_length", IntegerType(), True),
        StructField("product_description_length", IntegerType(), True),
        StructField("product_photos_qty", IntegerType(), True),
        StructField("product_weight_g", DecimalType(18, 3), True),
        StructField("product_length_cm", DecimalType(18, 3), True),
        StructField("product_height_cm", DecimalType(18, 3), True),
        StructField("product_width_cm", DecimalType(18, 3), True),
        StructField("volume_cm3", DecimalType(38, 6), True),
    ]
)

DIM_SELLER_SCHEMA = StructType(
    [
        StructField("seller_key", LongType(), False),
        StructField("seller_id", StringType(), True),
        StructField("seller_zip_code_prefix", StringType(), True),
        StructField("seller_city", StringType(), False),
        StructField("seller_state", StringType(), False),
        StructField("geography_key", LongType(), False),
    ]
)


def _typed_unknown(df: DataFrame, schema: StructType, values: tuple[object, ...]) -> DataFrame:
    """Tạo dòng dự phòng số 0 (Unknown row) với đúng schema của Dimension."""
    return df.sparkSession.createDataFrame([values], schema)


def build_dim_date(
    source_dates: DataFrame,
    date_columns: Sequence[str],
    safety_days: int = DEFAULT_DATE_SAFETY_DAYS,
) -> DataFrame:
    """Xây dựng bảng Kích thước Thời gian (dim_date) bao phủ toàn bộ dải ngày quan sát được.

    Cơ chế:
    1. Quét tìm min_date và max_date từ các cột ngày nguồn (ví dụ: ngày đặt mua, ngày giao hàng).
    2. Mở rộng thêm safety_days (366 ngày) về 2 phía để tạo dải ngày liên tục (continuous date spine).
    3. Trích xuất sẵn các thuộc tính thời gian: thứ, ngày, tuần, tháng, quý, năm, cờ cuối tuần.
    4. Thêm dòng đặc biệt date_key = 0 (Unknown) cho các trường ngày bị NULL.
    """
    if safety_days < 0:
        raise ValueError("safety_days không được là số âm")
    columns = tuple(date_columns)
    if not columns:
        raise ValueError("date_columns phải chứa ít nhất một cột")

    # Gom tất cả các ngày từ các cột ngày được chỉ định
    observed = source_dates.select(F.to_date(F.col(columns[0])).alias("full_date"))
    for name in columns[1:]:
        observed = observed.unionByName(
            source_dates.select(F.to_date(F.col(name)).alias("full_date"))
        )

    # Tìm ngày nhỏ nhất và ngày lớn nhất
    limits = observed.where(F.col("full_date").isNotNull()).agg(
        F.min("full_date").alias("minimum_date"),
        F.max("full_date").alias("maximum_date"),
    )
    bounds = limits.first()
    if bounds is None or bounds.minimum_date is None:
        raise ValueError("source_dates không có giá trị ngày hợp lệ khác null")

    # Sinh chuỗi ngày liên tục từ (min - 366 ngày) đến (max + 366 ngày)
    dates = limits.select(
        F.explode(
            F.sequence(
                F.date_sub("minimum_date", safety_days),
                F.date_add("maximum_date", safety_days),
            )
        ).alias("full_date")
    )

    # Tính toán các thuộc tính lịch
    # Thứ trong tuần theo chuẩn ISO: 1 = Thứ 2 ... 7 = Chủ nhật
    iso_day = ((F.dayofweek("full_date") + F.lit(5)) % F.lit(7) + F.lit(1)).cast("int")
    day_names = F.array(
        *(F.lit(name) for name in ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))
    )
    month_names = F.array(
        *(
            F.lit(name)
            for name in (
                "January",
                "February",
                "March",
                "April",
                "May",
                "June",
                "July",
                "August",
                "September",
                "October",
                "November",
                "December",
            )
        )
    )

    known = dates.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("full_date").cast("date"),
        iso_day.alias("day_of_week"),
        F.element_at(day_names, iso_day).alias("day_name"),
        F.dayofmonth("full_date").cast("int").alias("day_of_month"),
        F.dayofyear("full_date").cast("int").alias("day_of_year"),
        F.weekofyear("full_date").cast("int").alias("week_of_year"),
        F.month("full_date").cast("int").alias("month"),
        F.element_at(month_names, F.month("full_date")).alias("month_name"),
        F.quarter("full_date").cast("int").alias("quarter"),
        F.year("full_date").cast("int").alias("year"),
        iso_day.isin(6, 7).alias("is_weekend"),  # Thứ 7 (6) và Chủ nhật (7) là cuối tuần
    )

    # Dòng dự phòng số 0 đại diện cho ngày bị thiếu
    unknown = _typed_unknown(
        source_dates,
        DIM_DATE_SCHEMA,
        (0, None, 0, "Unknown", 0, 0, 0, 0, "Unknown", 0, 0, False),
    )
    return unknown.unionByName(known)


def build_dim_geography(geography: DataFrame) -> DataFrame:
    """Xây dựng bảng Kích thước Địa lý (dim_geography) và ánh xạ 5 Vùng kinh tế Brazil.

    5 Vùng kinh tế chính thức của Brazil:
    - North (Bắc): AC, AP, AM, PA, RO, RR, TO
    - Northeast (Đông Bắc): AL, BA, CE, MA, PB, PE, PI, RN, SE
    - Central-West (Trung Tây): DF, GO, MT, MS
    - Southeast (Đông Nam): ES, MG, RJ, SP (Vùng kinh tế trọng điểm nhất)
    - South (Nam): PR, RS, SC
    """
    valid = (
        geography.withColumn("zip_code_prefix", F.trim("zip_code_prefix"))
        .where(
            F.col("zip_code_prefix").isNotNull()
            & (F.length("zip_code_prefix") > 0)
        )
    )

    # Chọn bản ghi đại diện tốt nhất cho mỗi mã ZIP nếu có nhiều dòng
    canonical_order = Window.partitionBy("zip_code_prefix").orderBy(
        F.col("city").asc_nulls_last(),
        F.col("state").asc_nulls_last(),
        F.col("latitude").asc_nulls_last(),
        F.col("longitude").asc_nulls_last(),
        F.col("source_observation_count").desc_nulls_last(),
    )
    canonical = (
        valid.withColumn("_canonical_rank", F.row_number().over(canonical_order))
        .where(F.col("_canonical_rank") == 1)
        .drop("_canonical_rank")
    )

    # Ánh xạ bang (state) sang vùng kinh tế (region)
    region_by_state = F.create_map(
        *(
            item
            for state, region in {
                "AC": "North",
                "AP": "North",
                "AM": "North",
                "PA": "North",
                "RO": "North",
                "RR": "North",
                "TO": "North",
                "AL": "Northeast",
                "BA": "Northeast",
                "CE": "Northeast",
                "MA": "Northeast",
                "PB": "Northeast",
                "PE": "Northeast",
                "PI": "Northeast",
                "RN": "Northeast",
                "SE": "Northeast",
                "DF": "Central-West",
                "GO": "Central-West",
                "MT": "Central-West",
                "MS": "Central-West",
                "ES": "Southeast",
                "MG": "Southeast",
                "RJ": "Southeast",
                "SP": "Southeast",
                "PR": "South",
                "RS": "South",
                "SC": "South",
            }.items()
            for item in (F.lit(state), F.lit(region))
        )
    )
    enriched = canonical.withColumn(
        "region",
        F.coalesce(region_by_state[F.upper(F.trim("state"))], F.lit("Unknown")),
    )

    # Gán khóa đại diện geography_key (1, 2, 3...)
    known = assign_dense_surrogate_keys(
        enriched, ("zip_code_prefix",), "geography_key"
    ).select(
        F.col("geography_key").cast("long"),
        F.col("zip_code_prefix").cast("string"),
        F.col("city").cast("string"),
        F.col("state").cast("string"),
        F.col("region").cast("string"),
        F.col("latitude").cast("decimal(10,7)"),
        F.col("longitude").cast("decimal(10,7)"),
        F.coalesce(F.col("source_observation_count"), F.lit(0)).cast("long").alias(
            "source_observation_count"
        ),
    )

    # Thêm dòng dự phòng geography_key = 0
    unknown = _typed_unknown(
        geography,
        DIM_GEOGRAPHY_SCHEMA,
        (0, None, "Unknown", "Unknown", "Unknown", None, None, 0),
    )
    return unknown.unionByName(known)


def build_dim_customer(
    customers: DataFrame,
    orders: DataFrame,
    dim_geography: DataFrame,
) -> DataFrame:
    """Xây dựng bảng Kích thước Khách hàng (dim_customer) theo hạt customer_unique_id.

    Phân biệt quan trọng trong nghiệp vụ Olist:
    - customer_id: Mã định danh cho từng đơn hàng (thay đổi mỗi lần mua).
    - customer_unique_id: Mã định danh thực sự của cá nhân khách hàng (bất biến trọn đời).

    Các chỉ số tính toán sẵn:
    - order_count: Tổng số đơn hàng khách đã từng đặt trên sàn.
    - repeat_customer: Cờ boolean (order_count > 1) xác định khách hàng thân thiết.
    - representative_customer_id: Lấy thông tin địa chỉ từ đơn hàng mua gần nhất.
    - geography_key: Khóa ngoại liên kết với dim_geography.
    """
    customer_rows = (
        customers.select(
            F.trim("customer_id").alias("customer_id"),
            F.trim("customer_unique_id").alias("customer_unique_id"),
            F.trim("customer_zip_code_prefix").alias("customer_zip_code_prefix"),
            F.col("customer_city").cast("string").alias("customer_city"),
            F.col("customer_state").cast("string").alias("customer_state"),
        )
        .where(
            F.col("customer_unique_id").isNotNull()
            & (F.length("customer_unique_id") > 0)
            & F.col("customer_id").isNotNull()
            & (F.length("customer_id") > 0)
        )
    )

    # Tính số đơn hàng và ngày mua gần nhất cho từng customer_id
    order_stats = (
        orders.select(
            F.trim("order_id").alias("order_id"),
            F.trim("customer_id").alias("customer_id"),
            F.col("order_purchase_timestamp").cast("timestamp").alias(
                "order_purchase_timestamp"
            ),
        )
        .where(
            F.col("order_id").isNotNull()
            & (F.length("order_id") > 0)
            & F.col("customer_id").isNotNull()
            & (F.length("customer_id") > 0)
        )
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").cast("long").alias("_customer_order_count"),
            F.max("order_purchase_timestamp").alias("_last_order_timestamp"),
        )
    )

    candidates = customer_rows.join(order_stats, "customer_id", "left").withColumn(
        "_customer_order_count",
        F.coalesce(F.col("_customer_order_count"), F.lit(0)).cast("long"),
    )

    # Tính tổng số đơn hàng trọn đời cho mỗi customer_unique_id
    totals = candidates.groupBy("customer_unique_id").agg(
        F.sum("_customer_order_count").cast("long").alias("order_count")
    )

    # Chọn đơn hàng mới nhất làm thông tin địa chỉ đại diện
    representative_order = Window.partitionBy("customer_unique_id").orderBy(
        F.col("_last_order_timestamp").desc_nulls_last(),
        F.col("customer_id").asc(),
        F.col("customer_zip_code_prefix").asc_nulls_last(),
        F.col("customer_city").asc_nulls_last(),
        F.col("customer_state").asc_nulls_last(),
    )
    representatives = (
        candidates.withColumn("_representative_rank", F.row_number().over(representative_order))
        .where(F.col("_representative_rank") == 1)
        .select(
            "customer_unique_id",
            F.col("customer_id").alias("representative_customer_id"),
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        )
    )

    # Tra cứu khóa ngoại geography_key thông qua Safety Gate
    geography_lookup = prepare_dimension_lookup(
        dim_geography,
        "zip_code_prefix",
        "geography_key",
        "geography",
    ).select(
        F.col("zip_code_prefix").alias("_geo_zip_code_prefix"),
        F.col("geography_key").cast("long"),
    )

    conformed = (
        representatives.join(totals, "customer_unique_id", "inner")
        .join(
            geography_lookup,
            F.col("customer_zip_code_prefix") == F.col("_geo_zip_code_prefix"),
            "left",
        )
        .drop("_geo_zip_code_prefix")
        .withColumn(
            "geography_key",
            F.coalesce(F.col("geography_key"), F.lit(0)).cast("long"),
        )
        .withColumn("repeat_customer", F.col("order_count") > 1)
    )

    # Gán khóa đại diện customer_key (1, 2, 3...)
    known = assign_dense_surrogate_keys(
        conformed, ("customer_unique_id",), "customer_key"
    ).select(
        F.col("customer_key").cast("long"),
        F.col("customer_unique_id").cast("string"),
        F.col("representative_customer_id").cast("string"),
        F.col("customer_zip_code_prefix").cast("string"),
        F.coalesce(F.col("customer_city"), F.lit("Unknown")).alias("customer_city"),
        F.coalesce(F.col("customer_state"), F.lit("Unknown")).alias("customer_state"),
        F.col("geography_key").cast("long"),
        F.col("order_count").cast("long"),
        F.col("repeat_customer").cast("boolean"),
    )

    # Thêm dòng dự phòng customer_key = 0
    unknown = _typed_unknown(
        customers,
        DIM_CUSTOMER_SCHEMA,
        (0, None, None, None, "Unknown", "Unknown", 0, 0, False),
    )
    return unknown.unionByName(known)


def build_dim_product(products: DataFrame) -> DataFrame:
    """Xây dựng bảng Kích thước Sản phẩm (dim_product) chứa thuộc tính vật lý và danh mục."""
    valid = products.withColumn("product_id", F.trim("product_id")).where(
        F.col("product_id").isNotNull() & (F.length("product_id") > 0)
    )
    attributes = (
        "product_category_name",
        "product_category_name_english",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "volume_cm3",
    )
    canonical_order = Window.partitionBy("product_id").orderBy(
        *(F.col(name).asc_nulls_last() for name in attributes)
    )
    canonical = (
        valid.withColumn("_canonical_rank", F.row_number().over(canonical_order))
        .where(F.col("_canonical_rank") == 1)
        .drop("_canonical_rank")
    )

    # Gán khóa đại diện product_key
    known = assign_dense_surrogate_keys(canonical, ("product_id",), "product_key").select(
        F.col("product_key").cast("long"),
        F.col("product_id").cast("string"),
        F.coalesce(F.col("product_category_name"), F.lit("unknown")).alias(
            "product_category_name"
        ),
        F.coalesce(F.col("product_category_name_english"), F.lit("unknown")).alias(
            "product_category_name_english"
        ),
        F.col("product_name_length").cast("int"),
        F.col("product_description_length").cast("int"),
        F.col("product_photos_qty").cast("int"),
        F.col("product_weight_g").cast("decimal(18,3)"),
        F.col("product_length_cm").cast("decimal(18,3)"),
        F.col("product_height_cm").cast("decimal(18,3)"),
        F.col("product_width_cm").cast("decimal(18,3)"),
        F.col("volume_cm3").cast("decimal(38,6)"),
    )

    # Thêm dòng dự phòng product_key = 0
    unknown = _typed_unknown(
        products,
        DIM_PRODUCT_SCHEMA,
        (0, None, "unknown", "unknown", None, None, None, None, None, None, None, None),
    )
    return unknown.unionByName(known)


def build_dim_seller(sellers: DataFrame, dim_geography: DataFrame) -> DataFrame:
    """Xây dựng bảng Kích thước Người bán (dim_seller) kèm khóa ngoại geography_key."""
    valid = sellers.select(
        F.trim("seller_id").alias("seller_id"),
        F.trim("seller_zip_code_prefix").alias("seller_zip_code_prefix"),
        F.col("seller_city").cast("string").alias("seller_city"),
        F.col("seller_state").cast("string").alias("seller_state"),
    ).where(F.col("seller_id").isNotNull() & (F.length("seller_id") > 0))

    canonical_order = Window.partitionBy("seller_id").orderBy(
        F.col("seller_zip_code_prefix").asc_nulls_last(),
        F.col("seller_city").asc_nulls_last(),
        F.col("seller_state").asc_nulls_last(),
    )
    canonical = (
        valid.withColumn("_canonical_rank", F.row_number().over(canonical_order))
        .where(F.col("_canonical_rank") == 1)
        .drop("_canonical_rank")
    )

    # Tra cứu khóa ngoại geography_key thông qua Safety Gate
    geography_lookup = prepare_dimension_lookup(
        dim_geography,
        "zip_code_prefix",
        "geography_key",
        "geography",
    ).select(
        F.col("zip_code_prefix").alias("_geo_zip_code_prefix"),
        F.col("geography_key").cast("long"),
    )

    conformed = (
        canonical.join(
            geography_lookup,
            F.col("seller_zip_code_prefix") == F.col("_geo_zip_code_prefix"),
            "left",
        )
        .drop("_geo_zip_code_prefix")
        .withColumn(
            "geography_key",
            F.coalesce(F.col("geography_key"), F.lit(0)).cast("long"),
        )
    )

    # Gán khóa đại diện seller_key
    known = assign_dense_surrogate_keys(conformed, ("seller_id",), "seller_key").select(
        F.col("seller_key").cast("long"),
        F.col("seller_id").cast("string"),
        F.col("seller_zip_code_prefix").cast("string"),
        F.coalesce(F.col("seller_city"), F.lit("Unknown")).alias("seller_city"),
        F.coalesce(F.col("seller_state"), F.lit("Unknown")).alias("seller_state"),
        F.col("geography_key").cast("long"),
    )

    # Thêm dòng dự phòng seller_key = 0
    unknown = _typed_unknown(
        sellers,
        DIM_SELLER_SCHEMA,
        (0, None, None, "Unknown", "Unknown", 0),
    )
    return unknown.unionByName(known)
