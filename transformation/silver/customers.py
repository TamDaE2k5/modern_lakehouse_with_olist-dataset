"""Chuẩn hóa và kiểm định chất lượng dữ liệu Khách hàng (Customers) ở Tầng Silver.

Quy trình:
1. Chuẩn hóa định dạng chuỗi: trim khoảng trắng thừa, viết thường tên thành phố, viết hoa mã bang.
2. Chuẩn hóa mã bưu điện Brazil về đúng 5 chữ số bằng normalized_zip().
3. Kiểm tra 5 luật chất lượng (MISSING_ID, INVALID_ZIP, MISSING_CITY, INVALID_STATE).
4. Khử trùng lặp theo khóa chính customer_id (DUPLICATE_CUSTOMER_ID).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result, normalized_zip


def transform_customers(df: DataFrame) -> TransformResult:
    """Biến đổi bảng Bronze customers thành dữ liệu Silver sạch và Vùng cách ly Quarantine."""
    # Bước 1: Chuẩn hóa dữ liệu (Normalization)
    normalized = (
        df.withColumn("customer_id", F.trim("customer_id"))
        .withColumn("customer_unique_id", F.trim("customer_unique_id"))
        .withColumn("customer_zip_code_prefix", normalized_zip("customer_zip_code_prefix"))
        .withColumn("customer_city", F.lower(F.trim("customer_city")))    # Tên thành phố về chữ thường (sao paulo)
        .withColumn("customer_state", F.upper(F.trim("customer_state")))  # Mã bang về chữ in hoa (SP, RJ)
    )

    # Bước 2: Bộ luật kiểm định chất lượng (Data Quality Rules)
    rules = (
        ValidationRule("MISSING_CUSTOMER_ID", blank("customer_id")),
        ValidationRule("MISSING_CUSTOMER_UNIQUE_ID", blank("customer_unique_id")),
        ValidationRule(
            "INVALID_CUSTOMER_ZIP",
            blank("customer_zip_code_prefix")
            | ~F.col("customer_zip_code_prefix").rlike(r"^[0-9]{5}$"),
        ),
        ValidationRule("MISSING_CUSTOMER_CITY", blank("customer_city")),
        ValidationRule(
            "INVALID_CUSTOMER_STATE",
            blank("customer_state") | ~F.col("customer_state").rlike(r"^[A-Z]{2}$"),
        ),
    )

    # Bước 3: Khử trùng lặp xác định theo customer_id và phân tách kết quả
    return deterministic_result(
        normalized,
        rules,
        key=("customer_id",),
        duplicate_code="DUPLICATE_CUSTOMER_ID",
    )
