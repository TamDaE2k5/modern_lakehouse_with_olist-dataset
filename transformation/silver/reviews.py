"""Chuẩn hóa kiểu dữ liệu, trích xuất cờ đánh giá và kiểm tra chất lượng Đánh giá (Reviews) ở Tầng Silver.

Nghiệp vụ:
1. Ép kiểu: review_score sang integer (1-5 sao), ngày tạo và ngày trả lời sang timestamp.
2. Feature Engineering:
   - has_title: Khách hàng có nhập tiêu đề đánh giá hay không (boolean).
   - has_message: Khách hàng có để lại lời nhắn/nhận xét chi tiết hay không (boolean).
3. Kiểm tra chất lượng:
   - Điểm đánh giá phải hợp lệ từ 1 đến 5 sao (INVALID_REVIEW_SCORE).
   - Ngày trả lời đánh giá không được diễn ra trước ngày tạo đánh giá (REVIEW_ANSWER_BEFORE_CREATION).
4. Khử trùng lặp theo cặp khóa (review_id, order_id).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result
from transformation.silver.orders import TIMESTAMP_FORMAT


def transform_reviews(df: DataFrame) -> TransformResult:
    """Biến đổi bảng Bronze order_reviews thành dữ liệu Silver sạch và Vùng cách ly Quarantine."""
    # Bước 1: Chuẩn hóa chuỗi, ép kiểu số và trích xuất cờ boolean
    normalized = (
        df.withColumn("review_id", F.trim("review_id"))
        .withColumn("order_id", F.trim("order_id"))
        .withColumn("review_score", F.trim("review_score").cast("int"))
        .withColumn("review_comment_title", F.trim("review_comment_title"))
        .withColumn("review_comment_message", F.trim("review_comment_message"))
        .withColumn(
            "review_creation_date",
            F.to_timestamp(F.trim("review_creation_date"), TIMESTAMP_FORMAT),
        )
        .withColumn(
            "review_answer_timestamp",
            F.to_timestamp(F.trim("review_answer_timestamp"), TIMESTAMP_FORMAT),
        )
        # Cờ xác định khách hàng có viết tiêu đề đánh giá không
        .withColumn(
            "has_title",
            F.coalesce(F.length(F.trim("review_comment_title")) > 0, F.lit(False)),
        )
        # Cờ xác định khách hàng có viết nhận xét chi tiết không
        .withColumn(
            "has_message",
            F.coalesce(F.length(F.trim("review_comment_message")) > 0, F.lit(False)),
        )
    )

    # Bước 2: Bộ luật kiểm định chất lượng
    rules = (
        ValidationRule("MISSING_REVIEW_ID", blank("review_id")),
        ValidationRule("MISSING_REVIEW_ORDER_ID", blank("order_id")),
        ValidationRule(
            "INVALID_REVIEW_SCORE",
            F.col("review_score").isNull() | ~F.col("review_score").between(1, 5),
        ),
        ValidationRule(
            "INVALID_REVIEW_CREATION_DATE",
            F.col("review_creation_date").isNull(),
        ),
        ValidationRule(
            "INVALID_REVIEW_ANSWER_TIMESTAMP",
            F.col("review_answer_timestamp").isNull(),
        ),
        # Kiểm tra tính logic thời gian: Ngày trả lời không được trước ngày tạo
        ValidationRule(
            "REVIEW_ANSWER_BEFORE_CREATION",
            F.col("review_creation_date").isNotNull()
            & F.col("review_answer_timestamp").isNotNull()
            & (F.col("review_answer_timestamp") < F.col("review_creation_date")),
        ),
    )

    # Bước 3: Khử trùng lặp xác định theo cặp khóa (review_id, order_id)
    return deterministic_result(
        normalized,
        rules,
        key=("review_id", "order_id"),
        duplicate_code="DUPLICATE_REVIEW_KEY",
    )
