"""Các hàm tiện ích sinh và kiểm tra Khóa đại diện (Surrogate Keys) tầng Gold.

Mục đích:
- Thay thế các chuỗi UUID/mã tự nhiên dài bằng số nguyên 64-bit (bigint) để tăng tốc độ JOIN.
- Áp dụng nguyên tắc Khóa số 0 (UNKNOWN_KEY = 0) để đại diện cho bản ghi bị thiếu/lỗi thay vì dùng NULL.
- Kiểm định tính toàn vẹn 1-1 trước khi thực hiện JOIN vào bảng Fact.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Callable, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Khóa số 0 dành riêng cho các bản ghi bị thiếu/không xác định (Unknown Record)
UNKNOWN_KEY = 0
# Số lượng mẫu vi phạm tối đa hiển thị trong thông báo lỗi để debug
MAX_LOOKUP_DIAGNOSTICS = 5


def date_key(value: date) -> int:
    """Chuyển đổi một ngày (date) thành số nguyên chuẩn yyyyMMdd (ví dụ: 2018-05-15 -> 20180515)."""
    return value.year * 10_000 + value.month * 100 + value.day


def assign_dense_surrogate_keys(
    df: DataFrame,
    natural_keys: Sequence[str],
    surrogate_key: str,
) -> DataFrame:
    """Gán khóa đại diện số nguyên liên tục (1, 2, 3...) theo thứ tự từ điển của các khóa tự nhiên duy nhất.

    Cơ chế hoạt động:
    1. Lọc bỏ các dòng có khóa tự nhiên bị NULL hoặc rỗng.
    2. Rút gọn danh sách các khóa tự nhiên bằng .distinct() để mỗi mã chỉ xuất hiện đúng 1 lần.
    3. Đánh số thứ tự 1, 2, 3... bằng Window Function (F.row_number().over(orderBy(keys))).
    4. LEFT JOIN ngược lại với bảng gốc để bảo toàn 100% số dòng.
    5. Dùng F.coalesce() gán số 0 (UNKNOWN_KEY) cho các dòng bị NULL khóa tự nhiên.
    """
    keys = tuple(natural_keys)
    if not keys:
        raise ValueError("natural_keys phải chứa ít nhất một cột")

    # Bước 1: Tạo biểu thức lọc các bản ghi có khóa tự nhiên hợp lệ (khác NULL và độ dài > 0)
    valid = F.lit(True)
    for name in keys:
        valid = valid & F.col(name).isNotNull() & (F.length(F.trim(F.col(name).cast("string"))) > 0)

    # Bước 2: Sắp xếp theo thứ tự từ điển A-Z để đảm bảo tính tất định (chạy lại n lần vẫn ra cùng 1 mã)
    ordering = Window.orderBy(*(F.col(name).asc() for name in keys))

    # Bước 3: Tạo bảng từ điển ánh xạ (mapping dictionary)
    # Rút gọn các khóa tự nhiên duy nhất và đánh số 1, 2, 3... kiểu số nguyên bigint (long)
    mapping = (
        df.where(valid)
        .select(*keys)
        .distinct()
        .withColumn(surrogate_key, F.row_number().over(ordering).cast("long"))
    )

    # Bước 4: LEFT JOIN với bảng gốc và dùng F.coalesce thế chỗ NULL bằng số 0 (UNKNOWN_KEY)
    return df.join(mapping, list(keys), "left").withColumn(
        surrogate_key,
        F.coalesce(F.col(surrogate_key), F.lit(UNKNOWN_KEY).cast("long")),
    )


def _bounded_values(
    rows: Sequence[Any],
    formatter: Callable[[Any], str],
) -> str:
    """Định dạng chuỗi các giá trị vi phạm mẫu, giới hạn tối đa MAX_LOOKUP_DIAGNOSTICS phần tử."""
    rendered = ", ".join(formatter(row) for row in rows[:MAX_LOOKUP_DIAGNOSTICS])
    if len(rows) > MAX_LOOKUP_DIAGNOSTICS:
        return f"{rendered}, ..."
    return rendered


def prepare_dimension_lookup(
    df: DataFrame,
    natural_key: str,
    surrogate_key: str,
    lookup_name: str,
) -> DataFrame:
    """Chuẩn hóa và kiểm định chất lượng (Safety Gate) bảng tra cứu Dimension trước khi JOIN vào Fact.

    Mục đích sống còn:
    - Đảm bảo quan hệ ánh xạ 1-1 hoàn hảo giữa Khóa tự nhiên và Khóa đại diện.
    - Ngăn chặn triệt để nguy cơ 1 khóa tự nhiên trỏ tới 2 khóa đại diện,
      vì điều đó sẽ làm nhân đôi số dòng (nhân đôi doanh thu) khi JOIN vào bảng Fact.

    5 bài kiểm tra nghiêm ngặt:
    1. Khóa tự nhiên hợp lệ nhưng lại bị gán số 0.
    2. Khóa tự nhiên bị NULL nhưng lại có khóa đại diện khác 0.
    3. Khóa đại diện bị NULL hoặc âm.
    4. Xung đột 1-N: Một khóa tự nhiên ánh xạ tới nhiều khóa đại diện (nguy hiểm nhất!).
    5. Xung đột N-1: Nhiều khóa tự nhiên dùng chung 1 khóa đại diện.
    """
    # Bước 1: Rút gọn chỉ lấy 2 cột (natural_key, surrogate_key) và khử trùng lặp
    normalized = (
        df.select(
            F.trim(F.col(natural_key).cast("string")).alias(natural_key),
            F.col(surrogate_key).cast("long").alias(surrogate_key),
        )
        .withColumn(
            natural_key,
            F.when(F.length(F.col(natural_key)) > 0, F.col(natural_key)),
        )
        .distinct()
    )
    valid_natural = F.col(natural_key).isNotNull()
    diagnostics: list[str] = []

    # Bài test 1: Khóa tự nhiên hợp lệ nhưng lại bị gán số 0 (khóa số 0 chỉ dành riêng cho Unknown)
    ordinary_unknown = (
        normalized.where(valid_natural & (F.col(surrogate_key) == UNKNOWN_KEY))
        .select(natural_key)
        .orderBy(F.col(natural_key).asc())
        .limit(MAX_LOOKUP_DIAGNOSTICS + 1)
        .collect()
    )
    if ordinary_unknown:
        values = [row[natural_key] for row in ordinary_unknown]
        diagnostics.append(
            f"Khóa tự nhiên thông thường ánh xạ tới khóa 0 dành riêng: [{_bounded_values(values, repr)}]"
        )

    # Bài test 2: Khóa đại diện khác 0 nhưng khóa tự nhiên lại bị NULL/rỗng
    missing_natural = (
        normalized.where((~valid_natural) & (F.col(surrogate_key) != UNKNOWN_KEY))
        .select(surrogate_key)
        .orderBy(F.col(surrogate_key).asc_nulls_first())
        .limit(MAX_LOOKUP_DIAGNOSTICS + 1)
        .collect()
    )
    if missing_natural:
        values = [row[surrogate_key] for row in missing_natural]
        diagnostics.append(
            f"Khóa đại diện khác 0 nhưng khóa tự nhiên bị rỗng: [{_bounded_values(values, repr)}]"
        )

    # Bài test 3: Khóa đại diện bị NULL hoặc giá trị âm (< 0)
    invalid_surrogate = (
        normalized.where(
            F.col(surrogate_key).isNull() | (F.col(surrogate_key) < UNKNOWN_KEY)
        )
        .select(natural_key, surrogate_key)
        .orderBy(
            F.col(natural_key).asc_nulls_first(),
            F.col(surrogate_key).asc_nulls_first(),
        )
        .limit(MAX_LOOKUP_DIAGNOSTICS + 1)
        .collect()
    )
    if invalid_surrogate:
        diagnostics.append(
            "Khóa đại diện không hợp lệ (bị NULL hoặc âm): "
            + _bounded_values(
                invalid_surrogate,
                lambda row: f"{row[natural_key]!r}->{row[surrogate_key]!r}",
            )
        )

    # Bài test 4: Một khóa tự nhiên lại ánh xạ tới NHIỀU khóa đại diện (Nguy cơ nhân đôi dòng khi JOIN)
    natural_conflicts = (
        normalized.where(valid_natural)
        .groupBy(natural_key)
        .agg(F.sort_array(F.collect_set(surrogate_key)).alias("_surrogate_keys"))
        .where(F.size("_surrogate_keys") > 1)
        .orderBy(F.col(natural_key).asc())
        .limit(MAX_LOOKUP_DIAGNOSTICS + 1)
        .collect()
    )
    if natural_conflicts:
        diagnostics.append(
            "Một khóa tự nhiên ánh xạ tới nhiều khóa đại diện: "
            + _bounded_values(
                natural_conflicts,
                lambda row: f"{row[natural_key]}->{row['_surrogate_keys']}",
            )
        )

    # Bài test 5: Một khóa đại diện lại cấp cho NHIỀU khóa tự nhiên khác nhau
    surrogate_conflicts = (
        normalized.where(valid_natural & (F.col(surrogate_key) > UNKNOWN_KEY))
        .groupBy(surrogate_key)
        .agg(F.sort_array(F.collect_set(natural_key)).alias("_natural_keys"))
        .where(F.size("_natural_keys") > 1)
        .orderBy(F.col(surrogate_key).asc())
        .limit(MAX_LOOKUP_DIAGNOSTICS + 1)
        .collect()
    )
    if surrogate_conflicts:
        diagnostics.append(
            "Một khóa đại diện ánh xạ tới nhiều khóa tự nhiên: "
            + _bounded_values(
                surrogate_conflicts,
                lambda row: f"{row[surrogate_key]}->{row['_natural_keys']}",
            )
        )

    # Nếu phát hiện bất kỳ lỗi nào trong 5 bài test trên, dừng ngay pipeline để tránh làm hỏng tầng Fact
    if diagnostics:
        raise ValueError(f"Bảng tra cứu {lookup_name} không hợp lệ: {'; '.join(diagnostics)}")

    # Trả về bảng tra cứu sạch 1-1 chỉ chứa các khóa hợp lệ (> 0), sẵn sàng để JOIN
    return normalized.where(
        valid_natural & (F.col(surrogate_key) > UNKNOWN_KEY)
    )
