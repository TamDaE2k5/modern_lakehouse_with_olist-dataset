"""Các hàm trợ giúp Spark dùng chung cho toàn bộ các bảng ở Tầng Silver.

Bao gồm:
- blank(): Biểu thức kiểm tra chuỗi bị NULL hoặc toàn khoảng trắng.
- normalized_zip(): Chuẩn hóa mã bưu điện Brazil (CEP) về đúng 5 ký tự.
- deterministic_result(): Thuật toán khử trùng lặp xác định (Deterministic Deduplication),
  chọn bản ghi ít lỗi nhất và nạp mới nhất làm bản ghi chiến thắng, chuyển bản ghi trùng lặp vào Quarantine.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformation.common.validation import TransformResult, ValidationRule, split_valid_records

# 4 cột metadata hệ thống được kế thừa từ tầng Bronze
LINEAGE_COLUMNS = (
    "_ingestion_timestamp",
    "_ingestion_date",
    "_batch_id",
    "_source_file",
)


def blank(column: str):
    """Trả về biểu thức kiểm tra cột bị NULL hoặc chuỗi rỗng sau khi trim (F.col == '')."""
    return F.col(column).isNull() | (F.trim(F.col(column)) == "")


def normalized_zip(column: str):
    """Chuẩn hóa tiền tố mã bưu chính Brazil (CEP) từ 1-5 chữ số thành chuỗi 5 chữ số cố định.

    Ví dụ:
    - "789" -> "00789" (dùng lpad đệm số 0 vào đầu).
    - "12345" -> "12345" (giữ nguyên).
    - Chuỗi không phải số hoặc sai định dạng -> giữ nguyên để chuyển sang bước kiểm định bắt lỗi.
    """
    value = F.trim(F.col(column))
    return F.when(value.rlike(r"^[0-9]{1,5}$"), F.lpad(value, 5, "0")).otherwise(value)


def deterministic_result(
    df: DataFrame,
    rules: Sequence[ValidationRule],
    *,
    key: Sequence[str],
    duplicate_code: str,
) -> TransformResult:
    """Khử trùng lặp bản ghi theo khóa chính (key) và áp dụng các luật kiểm định chất lượng.

    Nguyên tắc chọn bản ghi chiến thắng (Winner Selection):
    Nếu có nhiều bản ghi trùng cùng 1 khóa chính (ví dụ 2 dòng cùng customer_id),
    hệ thống dùng Spark Window Function xếp hạng theo thứ tự ưu tiên:
    1. Bản ghi có ít lỗi nghiệp vụ nhất (business_error_count nhỏ nhất).
    2. Bản ghi có thời gian nạp mới nhất (_ingestion_timestamp giảm dần).
    3. Đợt nạp mới nhất (_batch_id giảm dần).
    4. Tên file nguồn theo thứ tự từ điển (_source_file tăng dần).

    Bản ghi hạng 1 (Rank 1): Bản ghi đại diện chiến thắng -> Đưa vào luồng kiểm tra bình thường.
    Bản ghi hạng > 1: Bản ghi bị trùng lặp -> Gắn mã lỗi duplicate_code và chuyển vào Quarantine.
    """
    # Đếm số lượng luật nghiệp vụ bị vi phạm trên từng dòng
    business_errors = F.array(
        *(F.when(rule.invalid_when, F.lit(rule.error_code)) for rule in rules)
    )
    business_error_count = F.size(F.filter(business_errors, lambda value: value.isNotNull()))

    # Tiêu chí xếp hạng ưu tiên
    ordering = [
        business_error_count.asc(),                     # 1. Ít lỗi nhất
        F.col("_ingestion_timestamp").desc_nulls_last(),# 2. Nạp mới nhất
        F.col("_batch_id").desc_nulls_last(),           # 3. Batch nạp mới nhất
        F.col("_source_file").asc_nulls_last(),          # 4. Tie-break cố định theo file
    ]

    # Đánh số thứ tự (1, 2, 3...) cho từng nhóm có cùng khóa chính
    ranked = df.withColumn(
        "_silver_dedupe_rank",
        F.row_number().over(Window.partitionBy(*(F.col(name) for name in key)).orderBy(*ordering)),
    )

    # Đưa vào hàm tách nhánh: bản ghi có _silver_dedupe_rank > 1 sẽ bị gắn mã lỗi duplicate_code
    result = split_valid_records(
        ranked,
        (*rules, ValidationRule(duplicate_code, F.col("_silver_dedupe_rank") > 1)),
    )

    return TransformResult(
        accepted=result.accepted.drop("_silver_dedupe_rank"),
        rejected=result.rejected.drop("_silver_dedupe_rank"),
    )


# ==============================================================================
# LEGACY / OVER-ENGINEERED CODE (COMMENTED OUT FOR REFERENCE)
#
# Reason for removal:
# The original project included _row_hash in LINEAGE_COLUMNS and used
# F.col("_row_hash").asc_nulls_last() as the final tie-breaker in Window ranking.
# Since per-row SHA-256 hashing was eliminated to save CPU and RAM, tie-breaking
# is performed deterministically using _source_file and ingestion metadata.
# ==============================================================================
#
# LEGACY_LINEAGE_COLUMNS = (
#     "_ingestion_timestamp",
#     "_ingestion_date",
#     "_batch_id",
#     "_source_file",
#     "_row_hash",
# )
