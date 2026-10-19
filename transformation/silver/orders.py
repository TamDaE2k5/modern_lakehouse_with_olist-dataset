"""Chuẩn hóa vòng đời Đơn hàng (Orders), tính toán chỉ số vận chuyển và kiểm định thời gian ở Tầng Silver.

Các điểm nghiệp vụ chính:
1. Ép kiểu 5 mốc thời gian sang timestamp (yyyy-MM-dd HH:mm:ss):
   - order_purchase_timestamp: Thời điểm khách đặt mua.
   - order_approved_at: Thời điểm thanh toán được duyệt.
   - order_delivered_carrier_date: Thời điểm bưu cục/vận chuyển nhận hàng.
   - order_delivered_customer_date: Thời điểm khách nhận được hàng.
   - order_estimated_delivery_date: Hạn chót giao hàng cam kết.
2. Feature Engineering:
   - delivery_duration_days: Số ngày giao hàng thực tế (khách nhận - đặt mua).
   - carrier_delivery_duration_days: Số ngày vận chuyển (khách nhận - bàn giao bưu cục).
   - is_late_delivery: Cờ boolean cảnh báo giao hàng trễ so với hạn cam kết.
3. Kiểm tra logic dòng đời đơn hàng (Chronological Validation):
   - Ngăn chặn nghịch lý thời gian: Duyệt trước khi mua, giao trước khi mua, giao trước khi bưu cục nhận.
   - Bắt lỗi đơn hàng báo "delivered" nhưng ngày nhận thực tế lại bị NULL (DELIVERED_ORDER_MISSING_DELIVERY_DATE).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result

# Định dạng thời gian chuẩn của Olist
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

# Danh sách 5 mốc thời gian của vòng đời đơn hàng
TIMESTAMP_COLUMNS = (
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
)

# 8 trạng thái đơn hàng hợp lệ
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
    """Biến đổi bảng Bronze orders thành dữ liệu Silver sạch và Vùng cách ly Quarantine."""
    # Bước 1: Chuẩn hóa chuỗi và ép kiểu 5 mốc thời gian sang timestamp
    normalized = (
        df.withColumn("order_id", F.trim("order_id"))
        .withColumn("customer_id", F.trim("customer_id"))
        .withColumn("order_status", F.lower(F.trim("order_status")))
    )
    for name in TIMESTAMP_COLUMNS:
        normalized = normalized.withColumn(f"_raw_{name}", F.col(name)).withColumn(
            name, F.to_timestamp(F.trim(F.col(name)), TIMESTAMP_FORMAT)
        )

    # Bước 2: Khai phá thuộc tính mới (Feature Engineering)
    normalized = (
        normalized.withColumn(
            # Tổng số ngày từ khi đặt mua đến khi khách nhận hàng
            "delivery_duration_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
            ).cast("int"),
        )
        .withColumn(
            # Số ngày hãng vận chuyển giao hàng
            "carrier_delivery_duration_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull()
                & F.col("order_delivered_carrier_date").isNotNull(),
                F.datediff("order_delivered_customer_date", "order_delivered_carrier_date"),
            ).cast("int"),
        )
        .withColumn(
            # Cờ giao trễ hạn: Ngày giao thực tế > Ngày dự kiến giao
            "is_late_delivery",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.col("order_delivered_customer_date")
                > F.col("order_estimated_delivery_date"),
            ).otherwise(F.lit(False)),
        )
    )

    # Kiểm tra các cột timestamp tùy chọn: nếu chuỗi thô có dữ liệu mà cast sang timestamp ra NULL -> lỗi định dạng
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

    # Bước 3: Bộ luật kiểm định nghiệp vụ và dòng đời thời gian
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
        # Kiểm tra tính logic của các mốc thời gian (Chronological Invariants)
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
        # Kiểm tra ngữ cảnh: Đơn hàng báo 'delivered' nhưng ngày khách nhận lại bị NULL
        ValidationRule(
            "DELIVERED_ORDER_MISSING_DELIVERY_DATE",
            (F.col("order_status") == "delivered") & delivered.isNull(),
        ),
    )

    # Bước 4: Khử trùng lặp xác định theo order_id
    result = deterministic_result(
        normalized,
        rules,
        key=("order_id",),
        duplicate_code="DUPLICATE_ORDER_ID",
    )

    # Dọn dẹp cột tạm _raw_* trước khi trả về
    helper_columns = tuple(f"_raw_{name}" for name in TIMESTAMP_COLUMNS)
    return TransformResult(
        accepted=result.accepted.drop(*helper_columns),
        rejected=result.rejected.drop(*helper_columns),
    )
