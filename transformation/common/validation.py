"""Các cấu trúc nền tảng phục vụ kiểm định chất lượng dữ liệu (Data Quality) và phân tách Vùng Cách Ly (Quarantine Pattern).

Nguyên tắc cốt lõi:
- Không xóa bỏ (DROP) dữ liệu lỗi làm mất dấu vết.
- Không để job bị sập (CRASH) khi gặp dòng dữ liệu bất thường.
- Đánh giá tất cả các luật cùng lúc bằng biểu thức Spark SQL trên Worker.
- Phân chia nguyên tử thành 2 luồng: `accepted` (dữ liệu sạch -> Silver) và `rejected` (dữ liệu lỗi -> Quarantine).
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Định dạng chuẩn cho mã lỗi: viết hoa, chỉ gồm chữ cái, số và dấu gạch dưới (ví dụ: MISSING_ORDER_ID)
_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]*$")


@dataclass(frozen=True)
class TransformResult:
    """Đối tượng chứa kết quả biến đổi gồm 2 DataFrame:
    - accepted: Các bản ghi đạt 100% tiêu chuẩn chất lượng (đưa vào Silver).
    - rejected: Các bản ghi vi phạm ít nhất 1 luật kiểm định (đưa vào Quarantine).
    """

    accepted: DataFrame
    rejected: DataFrame


@dataclass(frozen=True)
class ValidationRule:
    """Định nghĩa một luật kiểm định chất lượng dữ liệu:
    - error_code: Mã lỗi ngắn gọn, chuẩn hóa (ví dụ: 'NEGATIVE_PRICE', 'BLANK_CITY').
    - invalid_when: Biểu thức Spark Column trả về True khi dòng dữ liệu bị VI PHẠM luật.
    """

    error_code: str
    invalid_when: Column

    def __post_init__(self) -> None:
        if not _ERROR_CODE.fullmatch(self.error_code):
            raise ValueError(f"Mã lỗi kiểm định không hợp lệ (phải viết hoa A-Z, 0-9, _): {self.error_code}")


def split_valid_records(
    df: DataFrame,
    rules: Sequence[ValidationRule],
) -> TransformResult:
    """Đánh giá toàn bộ danh sách luật và phân tách dữ liệu thành 2 nhánh: Accepted vs Rejected.

    Cách hoạt động:
    1. Với mỗi luật trong rules, nếu invalid_when == True thì thu thập error_code vào một mảng lỗi.
    2. Lọc bỏ các phần tử NULL và sắp xếp mảng lỗi -> thu được cột `_error_codes` (mảng string).
    3. Dòng có size(_error_codes) == 0: Không có lỗi -> Đưa vào `accepted` (bỏ cột _error_codes).
    4. Dòng có size(_error_codes) > 0: Có ít nhất 1 lỗi -> Đưa vào `rejected`, gắn thêm timestamp `_rejected_at`.
    """
    if rules:
        # Gom tất cả các mã lỗi thỏa mãn điều kiện vi phạm vào một mảng
        raw_errors = F.array(
            *(F.when(rule.invalid_when, F.lit(rule.error_code)) for rule in rules)
        )
        # Lọc bỏ NULL và sắp xếp thứ tự mã lỗi
        error_codes = F.array_sort(F.filter(raw_errors, lambda value: value.isNotNull()))
    else:
        # Nếu không có luật nào, tạo mảng rỗng
        error_codes = F.array().cast("array<string>")

    classified = df.withColumn("_error_codes", error_codes)

    # Nhánh dữ liệu sạch: Không vi phạm luật nào
    accepted = classified.where(F.size("_error_codes") == 0).drop("_error_codes")

    # Nhánh dữ liệu lỗi: Vi phạm từ 1 luật trở lên, lưu lại thời điểm cách ly
    rejected = classified.where(F.size("_error_codes") > 0).withColumn(
        "_rejected_at", F.current_timestamp()
    )

    return TransformResult(accepted=accepted, rejected=rejected)
