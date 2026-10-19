"""Chuẩn hóa và kiểm định chất lượng dữ liệu Người bán (Sellers) ở Tầng Silver.

Quy trình:
1. Chuẩn hóa: trim khoảng trắng, chuẩn hóa zip code về 5 số, chữ thường cho thành phố, chữ hoa cho bang.
2. Kiểm tra chất lượng: bắt lỗi thiếu seller_id, zip code sai định dạng, thiếu tên thành phố, mã bang không đúng 2 chữ cái.
3. Khử trùng lặp theo khóa chính seller_id (DUPLICATE_SELLER_ID).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result, normalized_zip


def transform_sellers(df: DataFrame) -> TransformResult:
    """Biến đổi bảng Bronze sellers thành dữ liệu Silver sạch và Vùng cách ly Quarantine."""
    # Bước 1: Chuẩn hóa dữ liệu
    normalized = (
        df.withColumn("seller_id", F.trim("seller_id"))
        .withColumn("seller_zip_code_prefix", normalized_zip("seller_zip_code_prefix"))
        .withColumn("seller_city", F.lower(F.trim("seller_city")))
        .withColumn("seller_state", F.upper(F.trim("seller_state")))
    )

    # Bước 2: Bộ luật kiểm định chất lượng
    rules = (
        ValidationRule("MISSING_SELLER_ID", blank("seller_id")),
        ValidationRule(
            "INVALID_SELLER_ZIP",
            blank("seller_zip_code_prefix")
            | ~F.col("seller_zip_code_prefix").rlike(r"^[0-9]{5}$"),
        ),
        ValidationRule("MISSING_SELLER_CITY", blank("seller_city")),
        ValidationRule(
            "INVALID_SELLER_STATE",
            blank("seller_state") | ~F.col("seller_state").rlike(r"^[A-Z]{2}$"),
        ),
    )

    # Bước 3: Khử trùng lặp xác định theo seller_id
    return deterministic_result(
        normalized,
        rules,
        key=("seller_id",),
        duplicate_code="DUPLICATE_SELLER_ID",
    )
