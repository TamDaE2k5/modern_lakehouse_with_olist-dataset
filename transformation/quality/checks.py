"""Bộ hàm kiểm định chất lượng dữ liệu thuần Spark (Deterministic Spark-native Quality Checks).

Mô-đun này cung cấp các phép kiểm tra chất lượng dữ liệu độc lập, phân tán và có tính tất định cao:
1. Kiểm tra số lượng dòng (Row count positive): Đảm bảo bảng không rỗng.
2. Kiểm tra tính duy nhất (Uniqueness): Đếm chính xác số bản ghi tham gia vào khóa trùng lặp.
3. Kiểm tra tính không null (Nullability): Đếm tổng số ô null trên tập cột bắt buộc.
4. Kiểm tra miền giá trị (Domain check): Đếm các bản ghi có giá trị nằm ngoài danh mục cho phép.
5. Kiểm tra giá trị không âm (Non-negative check): Đảm bảo các chỉ số tài chính/đo lường >= 0.
6. Kiểm tra thứ tự thời gian (Date ordering check): Đảm bảo tiến trình vòng đời đúng logic (ngày tạo <= ngày giao).
7. Kiểm tra khóa ngoại (Foreign key check): Sử dụng Left-Anti Join để đếm các bản ghi mồ côi (orphans).
8. Kiểm tra khóa ngoại có phân loại (Classified FK check): Phân tách bản ghi mồ côi thành 2 nhóm:
   - Nhóm có thể giải thích được: Khóa cha đã bị cách ly ở bảng Quarantine (mức cảnh báo WARN).
   - Nhóm không thể giải thích: Bản ghi mồ côi thực sự do lỗi hệ thống (mức lỗi nghiêm trọng ERROR).
9. Kiểm tra khóa không xác định (Unknown Key 0 check): Đảm bảo mỗi bảng Dimension có đúng 1 bản ghi khóa 0.
10. Đối soát tài chính (Reconciliation check): So sánh tổng cộng dồn bằng số học Decimal chính xác tuyệt đối.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Các mức độ nghiêm trọng chuẩn của hệ thống chất lượng dữ liệu
SEVERITIES = ("INFO", "WARN", "ERROR")

# Dung sai cho phép khi đối soát tiền tệ (0.01 BRL do làm tròn số thập phân)
MONEY_TOLERANCE = Decimal("0.01")


@dataclass(frozen=True)
class QualityResult:
    """Đại diện cho kết quả của một bài kiểm tra chất lượng trên một bảng dữ liệu."""

    layer: str            # Tầng dữ liệu: 'bronze', 'silver', 'gold'
    table_name: str       # Tên bảng: ví dụ 'orders', 'fact_order_items'
    rule_name: str        # Tên quy tắc: ví dụ 'row_count_positive', 'unique_order_id'
    severity: str         # Mức độ nghiêm trọng: 'INFO', 'WARN', 'ERROR'
    observed_value: str   # Giá trị quan sát thực tế (dưới dạng chuỗi để lưu trữ bền vững)
    threshold: str        # Ngưỡng kỳ vọng: ví dụ '> 0 rows', '0 duplicate rows'
    status: str           # Trạng thái cuối cùng: 'PASS', 'WARN', hoặc 'ERROR'
    run_id: str           # Mã phiên chạy kiểm định (dùng để truy vết audit)
    checked_at: datetime  # Thời điểm thực hiện bài kiểm tra (UTC)


def _columns(frame: DataFrame, columns: Sequence[str]) -> tuple[str, ...]:
    """Xác thực và trả về danh sách các cột được yêu cầu, báo lỗi nếu cột không tồn tại trong DataFrame."""
    selected = tuple(columns)
    if not selected:
        raise ValueError("Cần ít nhất một column để kiểm tra")
    missing = tuple(column for column in selected if column not in frame.columns)
    if missing:
        raise ValueError(f"Không tìm thấy column trong bảng: {', '.join(missing)}")
    return selected


def _result(
    *,
    layer: str,
    table_name: str,
    rule_name: str,
    severity: str,
    observed_value: str,
    threshold: str,
    passed: bool,
    run_id: str,
    checked_at: datetime | None,
) -> QualityResult:
    """Tạo đối tượng QualityResult chuẩn hóa, chuẩn hóa severity và gán trạng thái PASS/WARN/ERROR."""
    normalized_severity = severity.upper()
    if normalized_severity not in SEVERITIES:
        raise ValueError(f"Severity chất lượng không được hỗ trợ: {severity}")
    return QualityResult(
        layer=layer,
        table_name=table_name,
        rule_name=rule_name,
        severity=normalized_severity,
        observed_value=observed_value,
        threshold=threshold,
        status="PASS" if passed else normalized_severity,
        run_id=run_id,
        checked_at=checked_at or datetime.now(timezone.utc),
    )


def unavailable_result(
    *, layer: str, table_name: str, detail: str, run_id: str
) -> QualityResult:
    """Tạo bản ghi chất lượng báo lỗi nghiêm trọng khi không thể đọc được bảng dữ liệu khai báo."""
    return _result(
        layer=layer,
        table_name=table_name,
        rule_name="table_available",
        severity="ERROR",
        observed_value=detail,
        threshold="table is readable",
        passed=False,
        run_id=run_id,
        checked_at=None,
    )


def check_row_count(
    frame: DataFrame,
    *,
    layer: str,
    table_name: str,
    severity: str,
    run_id: str,
    checked_at: datetime | None = None,
) -> QualityResult:
    """Kiểm tra số lượng dòng: Đảm bảo bảng không rỗng (row count > 0)."""
    observed = frame.count()
    return _result(
        layer=layer,
        table_name=table_name,
        rule_name="row_count_positive",
        severity=severity,
        observed_value=str(observed),
        threshold="> 0 rows",
        passed=observed > 0,
        run_id=run_id,
        checked_at=checked_at,
    )


def check_uniqueness(
    frame: DataFrame,
    *,
    columns: Sequence[str],
    layer: str,
    table_name: str,
    severity: str,
    run_id: str,
    checked_at: datetime | None = None,
) -> QualityResult:
    """Kiểm tra tính duy nhất: Đếm tổng số dòng tham gia vào các khóa bị trùng lặp.
    
    Lý thuyết thực thi Spark:
    Nhóm theo các cột khóa (groupBy), lọc ra các nhóm có count > 1, sau đó tính SUM(count)
    để biết tổng số bản ghi bị ảnh hưởng bởi trùng lặp (chứ không chỉ đếm số khóa khác nhau).
    """
    selected = _columns(frame, columns)
    duplicate_row = (
        frame.groupBy(*selected)
        .count()
        .where(F.col("count") > 1)
        .agg(F.coalesce(F.sum("count"), F.lit(0)).cast("long").alias("value"))
        .first()
    )
    if duplicate_row is None:
        raise RuntimeError("Phép tổng hợp khóa trùng lặp không trả về kết quả")
    duplicate_rows = duplicate_row.value
    return _result(
        layer=layer,
        table_name=table_name,
        rule_name="unique_" + "_".join(selected),
        severity=severity,
        observed_value=str(duplicate_rows),
        threshold="0 duplicate rows",
        passed=duplicate_rows == 0,
        run_id=run_id,
        checked_at=checked_at,
    )


def check_nullability(
    frame: DataFrame,
    *,
    columns: Sequence[str],
    layer: str,
    table_name: str,
    severity: str,
    run_id: str,
    checked_at: datetime | None = None,
) -> QualityResult:
    """Kiểm tra tính không null: Đếm tổng số ô null trên danh sách các cột bắt buộc.
    
    Lý thuyết thực thi Spark:
    Dùng biểu thức F.col(column).isNull().cast('long') rồi SUM gộp trên một lượt quét DataFrame (single scan),
    tránh việc phải quét bảng nhiều lần cho từng cột.
    """
    selected = _columns(frame, columns)
    null_cells = frame.select(
        *[F.col(column).isNull().cast("long").alias(column) for column in selected]
    ).agg(*[F.sum(column).alias(column) for column in selected]).first()
    if null_cells is None:
        raise RuntimeError("Phép tổng hợp nullability không trả về kết quả")
    observed = sum((int(null_cells[column] or 0) for column in selected), start=0)
    return _result(
        layer=layer,
        table_name=table_name,
        rule_name="not_null_" + "_".join(selected),
        severity=severity,
        observed_value=str(observed),
        threshold="0 null values",
        passed=observed == 0,
        run_id=run_id,
        checked_at=checked_at,
    )


def check_domain(
    frame: DataFrame,
    *,
    column: str,
    allowed_values: Sequence[object],
    layer: str,
    table_name: str,
    severity: str,
    run_id: str,
    checked_at: datetime | None = None,
) -> QualityResult:
    """Kiểm tra miền giá trị: Đếm số lượng bản ghi có giá trị khác null nhưng không nằm trong tập cho phép."""
    _columns(frame, (column,))
    allowed = tuple(allowed_values)
    if not allowed:
        raise ValueError("allowed_values không được rỗng")
    observed = frame.where(
        F.col(column).isNotNull() & ~F.col(column).isin(*allowed)
    ).count()
    rendered = ", ".join(str(value) for value in allowed)
    return _result(
        layer=layer,
        table_name=table_name,
        rule_name=f"domain_{column}",
        severity=severity,
        observed_value=str(observed),
        threshold=f"values in [{rendered}]",
        passed=observed == 0,
        run_id=run_id,
        checked_at=checked_at,
    )


def check_non_negative(
    frame: DataFrame,
    *,
    column: str,
    layer: str,
    table_name: str,
    severity: str,
    run_id: str,
    checked_at: datetime | None = None,
) -> QualityResult:
    """Kiểm tra giá trị không âm: Đếm số lượng bản ghi có giá trị số < 0 (áp dụng cho giá bán, phí ship, v.v.)."""
    _columns(frame, (column,))
    observed = frame.where(F.col(column).isNotNull() & (F.col(column) < 0)).count()
    return _result(
        layer=layer,
        table_name=table_name,
        rule_name=f"non_negative_{column}",
        severity=severity,
        observed_value=str(observed),
        threshold=">= 0",
        passed=observed == 0,
        run_id=run_id,
        checked_at=checked_at,
    )


def check_date_ordering(
    frame: DataFrame,
    *,
    earlier_column: str,
    later_column: str,
    layer: str,
    table_name: str,
    severity: str,
    run_id: str,
    checked_at: datetime | None = None,
) -> QualityResult:
    """Kiểm tra thứ tự vòng đời thời gian: Đếm số bản ghi mà mốc thời gian sau lại nhỏ hơn mốc thời gian trước."""
    _columns(frame, (earlier_column, later_column))
    observed = frame.where(
        F.col(earlier_column).isNotNull()
        & F.col(later_column).isNotNull()
        & (F.col(later_column) < F.col(earlier_column))
    ).count()
    return _result(
        layer=layer,
        table_name=table_name,
        rule_name=f"{earlier_column}_before_or_equal_{later_column}",
        severity=severity,
        observed_value=str(observed),
        threshold="0 reversed rows",
        passed=observed == 0,
        run_id=run_id,
        checked_at=checked_at,
    )


def check_foreign_key(
    child: DataFrame,
    parent: DataFrame,
    *,
    child_columns: Sequence[str],
    parent_columns: Sequence[str],
    layer: str,
    table_name: str,
    severity: str,
    run_id: str,
    checked_at: datetime | None = None,
) -> QualityResult:
    """Kiểm tra toàn vẹn khóa ngoại: Đếm số bản ghi con bị mồ côi (không tìm thấy bản ghi cha tương ứng).
    
    Lý thuyết thực thi Spark:
    Sử dụng phép 'Left-Anti Join'. Phép toán này chỉ giữ lại những dòng ở bảng con (child)
    mà KHÔNG có dòng nào khớp ở bảng cha (parent). Nếu count() == 0 nghĩa là 100% bản ghi con đều có cha.
    """
    children = _columns(child, child_columns)
    parents = _columns(parent, parent_columns)
    if len(children) != len(parents):
        raise ValueError("Số lượng cột khóa ngoại của bảng con và bảng cha phải bằng nhau")
    child_keys = child.select(*children)
    parent_keys = parent.select(
        *[F.col(column).alias(f"_parent_{index}") for index, column in enumerate(parents)]
    ).dropDuplicates()
    condition = F.lit(True)
    for index, column in enumerate(children):
        condition = condition & (child_keys[column] == parent_keys[f"_parent_{index}"])
    observed = child_keys.join(parent_keys, condition, "left_anti").count()
    return _result(
        layer=layer,
        table_name=table_name,
        rule_name=(
            "foreign_key_"
            + "_".join(children)
            + "_to_"
            + "_".join(parents)
        ),
        severity=severity,
        observed_value=str(observed),
        threshold="0 orphan rows",
        passed=observed == 0,
        run_id=run_id,
        checked_at=checked_at,
    )


def check_classified_foreign_key(
    child: DataFrame,
    parent: DataFrame,
    quarantine_parent: DataFrame,
    *,
    child_column: str,
    parent_column: str,
    quarantine_column: str,
    layer: str,
    table_name: str,
    run_id: str,
    checked_at: datetime | None = None,
) -> tuple[QualityResult, QualityResult]:
    """Phân loại bản ghi mồ côi thành 2 nhóm: Nhóm đã cách ly (Quarantined) và Nhóm lỗi bất thường (Unexplained).
    
    Lý thuyết nghiệp vụ:
    Trong thực tế, khi bảng cha (ví dụ orders) bị cách ly một số bản ghi không hợp lệ vào quarantine.orders,
    các bản ghi con (order_items, order_payments) trỏ vào các đơn hàng đó sẽ tự nhiên trở thành mồ côi ở tầng Silver.
    Hàm này phân tách:
    1. expected_count (WARN): Bản ghi mồ côi nhưng tìm thấy order_id trong quarantine.orders (biết lý do).
    2. unexplained_count (ERROR): Bản ghi mồ côi mà KHÔNG hề có trong cả silver.orders lẫn quarantine.orders (lỗi nghiêm trọng).
    """
    _columns(child, (child_column,))
    _columns(parent, (parent_column,))
    _columns(quarantine_parent, (quarantine_column,))
    child_keys = child.select(F.col(child_column).alias("_child_key"))
    parent_keys = parent.select(F.col(parent_column).alias("_parent_key")).dropDuplicates()
    orphan_rows = child_keys.join(
        parent_keys,
        child_keys._child_key == parent_keys._parent_key,
        "left_anti",
    )
    quarantine_keys = quarantine_parent.select(
        F.col(quarantine_column).alias("_quarantine_key")
    ).dropDuplicates()
    expected_count = orphan_rows.join(
        quarantine_keys,
        orphan_rows._child_key == quarantine_keys._quarantine_key,
        "inner",
    ).count()
    unexplained_count = orphan_rows.join(
        quarantine_keys,
        orphan_rows._child_key == quarantine_keys._quarantine_key,
        "left_anti",
    ).count()
    prefix = f"foreign_key_{child_column}_to_{parent_column}"
    expected = _result(
        layer=layer,
        table_name=table_name,
        rule_name=f"{prefix}_quarantined_parent",
        severity="WARN",
        observed_value=str(expected_count),
        threshold="0 retained rows with quarantined parent",
        passed=expected_count == 0,
        run_id=run_id,
        checked_at=checked_at,
    )
    unexplained = _result(
        layer=layer,
        table_name=table_name,
        rule_name=f"{prefix}_unexplained",
        severity="ERROR",
        observed_value=str(unexplained_count),
        threshold="0 unexplained orphan rows",
        passed=unexplained_count == 0,
        run_id=run_id,
        checked_at=checked_at,
    )
    return expected, unexplained


def check_unknown_members(
    frame: DataFrame,
    *,
    column: str,
    layer: str,
    table_name: str,
    run_id: str,
    checked_at: datetime | None = None,
    expected_count: int = 0,
    severity: str = "WARN",
) -> QualityResult:
    """Kiểm tra số lượng bản ghi Khóa 0 (Unknown Member):
    - Đối với bảng Dimension: Mong đợi đúng 1 dòng có surrogate key = 0 (Unknown Record).
    - Đối với bảng Fact/Bridge: Báo cáo số lượng bản ghi trỏ về Khóa 0 mà không coi đó là lỗi mồ côi.
    """
    _columns(frame, (column,))
    if expected_count < 0:
        raise ValueError("Số lượng thành viên không xác định dự kiến không được là số âm")
    observed = frame.where(F.col(column) == 0).count()
    return _result(
        layer=layer,
        table_name=table_name,
        rule_name=f"unknown_member_{column}",
        severity=severity,
        observed_value=str(observed),
        threshold=f"{expected_count} key-0 rows",
        passed=observed == expected_count,
        run_id=run_id,
        checked_at=checked_at,
    )


def check_reconciliation(
    left: DataFrame,
    right: DataFrame,
    *,
    left_column: str,
    right_column: str,
    rule_name: str,
    layer: str,
    table_name: str,
    severity: str,
    run_id: str,
    checked_at: datetime | None = None,
    tolerance: Decimal = MONEY_TOLERANCE,
) -> QualityResult:
    """Kiểm tra đối soát tài chính (Reconciliation): So sánh tổng cộng dồn bằng số học Decimal chuẩn xác.
    
    Lý thuyết thực thi:
    Chuyển đổi tổng sum sang decimal(38, 2) trên Spark, sau đó dùng Decimal của Python để tính chênh lệch tuyệt đối:
    abs(left_total - right_total) <= tolerance (0.01 BRL).
    """
    _columns(left, (left_column,))
    _columns(right, (right_column,))
    if tolerance < 0:
        raise ValueError("Dung sai đối soát không được là số âm")
    left_total_row = left.agg(
        F.coalesce(F.sum(left_column), F.lit(0)).cast("decimal(38,2)").alias("total")
    ).first()
    right_total_row = right.agg(
        F.coalesce(F.sum(right_column), F.lit(0)).cast("decimal(38,2)").alias("total")
    ).first()
    if left_total_row is None or right_total_row is None:
        raise RuntimeError("Phép tổng hợp đối soát không trả về kết quả")
    left_total = left_total_row.total
    right_total = right_total_row.total
    difference = abs(Decimal(left_total) - Decimal(right_total)).quantize(Decimal("0.01"))
    return _result(
        layer=layer,
        table_name=table_name,
        rule_name=rule_name,
        severity=severity,
        observed_value=f"{difference:.2f} BRL",
        threshold=f"<= {tolerance:.2f} BRL",
        passed=difference <= tolerance,
        run_id=run_id,
        checked_at=checked_at,
    )


def aggregate_status(results: Sequence[QualityResult]) -> str:
    """Tính toán trạng thái tổng thể từ danh sách các kết quả kiểm định:
    - Nếu có ít nhất 1 quy tắc bị ERROR -> Trả về 'ERROR' (chặn pipeline).
    - Nếu không có ERROR nhưng có ít nhất 1 WARN -> Trả về 'WARN' (cảnh báo nhưng cho phép đi tiếp).
    - Nếu tất cả đều PASS -> Trả về 'PASS'.
    """
    statuses = {result.status for result in results}
    if "ERROR" in statuses:
        return "ERROR"
    if "WARN" in statuses:
        return "WARN"
    return "PASS"
