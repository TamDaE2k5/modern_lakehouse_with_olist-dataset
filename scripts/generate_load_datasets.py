"""Sinh 3 bộ dữ liệu kiểm thử TẢI & XỬ LÝ TĂNG DẦN từ dữ liệu Olist thật (Task 2).

TẠI SAO KHÔNG DÙNG DỮ LIỆU NGẪU NHIÊN?
--------------------------------------
Dữ liệu sinh ngẫu nhiên hoàn toàn (ví dụ ``faker``) không tái hiện được các đặc tính
thống kê thật sự gây khó cho hệ thống: phân bố lệch của sản phẩm bán chạy, tỷ lệ đơn bị
hủy, độ dài đuôi của thời gian giao hàng, mật độ mã bưu chính theo bang. Vì vậy cả 3 bộ
dữ liệu dưới đây đều được **cắt ra từ chính 99.441 đơn hàng Olist thật** rồi chỉ biến tấu
đúng chiều thời gian và khối lượng — giữ nguyên mọi quan hệ nghiệp vụ gốc.

BA KỊCH BẢN TẢI ĐƯỢC MÔ PHỎNG
-----------------------------
    Dữ liệu Olist gốc  ┌──► Bộ 1: Giờ thấp điểm   (~500 đơn, 02:00-05:00)
    (99.441 đơn hàng) ─┼──► Bộ 2: Flash-Sale      (~40.000 đơn dồn vào 12:00-13:59)
                       └──► Bộ 3: Dồn tích bù     (trọn 1 tháng, trạng thái hỗn hợp)

ĐẦU VÀO : ``data/raw/`` (9 file CSV Olist gốc).
ĐẦU RA  : ``data/synthetic/01_offpeak/``, ``02_flash_sale/``, ``03_catchup/``
          — mỗi thư mục chứa đủ 9 file CSV đúng chuẩn, nạp thẳng được vào Bronze.

CÁCH CHẠY:
    python scripts/generate_load_datasets.py
    python scripts/generate_load_datasets.py --base-date 2026-11-10 --flash-sale-orders 50000
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta
from pathlib import Path
import sys

import pandas as pd

# Cho phép chạy trực tiếp `python scripts/generate_load_datasets.py` từ thư mục gốc dự án.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from olist_dataset_common import (  # noqa: E402
    OlistSource,
    enable_utf8_output,
    TIMESTAMP_FORMAT,
    print_summary,
    shift_order_timestamps,
    slice_by_orders,
    write_dataset,
)


# Hạt giống ngẫu nhiên cố định: đảm bảo chạy lại script luôn cho ra bộ dữ liệu y hệt,
# nhờ vậy các con số benchmark đo được ở những lần chạy khác nhau mới so sánh được với nhau.
RANDOM_SEED = 20261106


def _spread_within_window(
    count: int,
    day: date,
    start_hour: int,
    end_hour: int,
    rng: pd.Series | None = None,
) -> pd.Series:
    """Rải đều ``count`` mốc thời gian vào khung giờ [start_hour, end_hour) của một ngày.

    Đầu vào : số lượng mốc cần sinh, ngày đích và khung giờ (theo giờ, 0-23).
    Xử lý   : chia đều khoảng thời gian thành ``count`` điểm cách nhau bằng nhau.
    Đầu ra  : Series các ``datetime`` đã sắp xếp tăng dần.

    Vì sao rải đều thay vì ngẫu nhiên? Rải đều cho ra mật độ giao dịch ổn định, giúp số đo
    thông lượng (dòng/giây) của benchmark phản ánh đúng năng lực hệ thống thay vì bị nhiễu
    bởi các cụm ngẫu nhiên dồn cục.
    """
    window_start = datetime.combine(day, time(hour=start_hour))
    window_end = datetime.combine(day, time(hour=end_hour - 1, minute=59, second=59))
    total_seconds = int((window_end - window_start).total_seconds())
    step = max(total_seconds / max(count, 1), 0.001)
    return pd.Series(
        [window_start + timedelta(seconds=int(index * step)) for index in range(count)]
    )


def build_offpeak(source: OlistSource, day: date, order_count: int) -> dict[str, pd.DataFrame]:
    """BỘ 1 — Giờ thấp điểm: lô dữ liệu siêu nhỏ chạy lúc 02:00-05:00 sáng.

    Ý nghĩa thực tế : khung giờ đêm muộn, giao dịch thưa thớt, mỗi micro-batch chỉ vài trăm đơn.
    Mục tiêu kiểm thử:
      1. Đo chi phí cố định (overhead) của Spark khi xử lý lô rất nhỏ — liệu 500 dòng có
         sinh ra hàng trăm tệp Parquet tí hon (vấn đề small-file) hay không.
      2. Xác minh ``mode=incremental`` nạp thêm mà KHÔNG nhân đôi dữ liệu đã có.
    """
    sample = source.orders.sample(n=order_count, random_state=RANDOM_SEED)
    targets = _spread_within_window(len(sample), day, start_hour=2, end_hour=5)
    shifted = shift_order_timestamps(sample.reset_index(drop=True), targets)
    return slice_by_orders(
        OlistSource(**{**source.__dict__, "orders": shifted}),
        shifted["order_id"],
    )


def build_flash_sale(
    source: OlistSource,
    day: date,
    order_count: int,
    hot_category_count: int,
) -> dict[str, pd.DataFrame]:
    """BỘ 2 — Flash-Sale: hàng chục nghìn đơn dồn vào đúng 2 tiếng vàng 12:00-13:59.

    Ý nghĩa thực tế : các đợt siêu sale 11/11, 12/12 — lưu lượng tăng vọt 10-50 lần.
    Mục tiêu kiểm thử:
      1. Spark Worker (2 nhân, 4 GiB RAM) có bị tràn bộ nhớ (OOM) hay mất nhịp tim
         (heartbeat timeout) khi shuffle khối dữ liệu dồn nén hay không.
      2. Airflow Pool ``lakehouse_writes`` có giữ vững trần 8 slot, không để nhiều job Spark
         cùng ghi gây xung đột commit trên Iceberg catalog hay không.

    Cách tạo độ lệch (skew) giống thật: ưu tiên lấy các đơn có chứa sản phẩm thuộc nhóm
    danh mục bán chạy nhất, rồi NHÂN BẢN thêm dòng hàng hot trong chính các đơn đó. Độ lệch
    phân bố khóa này mới là thứ thực sự làm Spark shuffle bị nghẽn — đúng điều cần đo.
    """
    # 1. Xác định nhóm danh mục bán chạy nhất theo số dòng hàng thực tế đã bán.
    items_with_category = source.order_items.merge(
        source.products[["product_id", "product_category_name"]], on="product_id", how="left"
    )
    hot_categories = (
        items_with_category["product_category_name"].value_counts().head(hot_category_count).index
    )
    hot_products = set(
        source.products.loc[
            source.products["product_category_name"].isin(hot_categories), "product_id"
        ]
    )
    hot_orders = set(
        source.order_items.loc[source.order_items["product_id"].isin(hot_products), "order_id"]
    )

    # 2. Lấy mẫu lệch: 70% đơn có hàng hot, 30% đơn thường (giữ tính đa dạng của dữ liệu).
    hot_pool = source.orders[source.orders["order_id"].isin(hot_orders)]
    cold_pool = source.orders[~source.orders["order_id"].isin(hot_orders)]
    hot_take = min(int(order_count * 0.7), len(hot_pool))
    cold_take = min(order_count - hot_take, len(cold_pool))
    sample = pd.concat(
        [
            hot_pool.sample(n=hot_take, random_state=RANDOM_SEED),
            cold_pool.sample(n=cold_take, random_state=RANDOM_SEED),
        ]
    ).reset_index(drop=True)

    # 3. Dồn toàn bộ thời điểm mua hàng vào đúng khung giờ vàng 12:00 - 13:59.
    targets = _spread_within_window(len(sample), day, start_hour=12, end_hour=14)
    shifted = shift_order_timestamps(sample, targets)
    frames = slice_by_orders(
        OlistSource(**{**source.__dict__, "orders": shifted}), shifted["order_id"]
    )

    # 4. Nhân bản dòng hàng hot để đẩy mạnh độ lệch phân bố product_id.
    frames["order_items"], frames["order_payments"] = _duplicate_hot_items(
        frames["order_items"], frames["order_payments"], hot_products
    )
    return frames


def _duplicate_hot_items(
    items: pd.DataFrame,
    payments: pd.DataFrame,
    hot_products: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Nhân đôi các dòng hàng thuộc sản phẩm hot và cộng bù tiền vào dòng thanh toán đầu tiên.

    Đầu vào : bảng order_items, order_payments đã cắt và tập product_id thuộc nhóm hot.
    Xử lý   : với mỗi đơn có hàng hot, thêm một dòng hàng sao chép mang ``order_item_id`` kế tiếp,
              đồng thời cộng đúng số tiền phát sinh vào ``payment_value`` của dòng thanh toán
              có ``payment_sequential`` nhỏ nhất của đơn đó.
    Đầu ra  : cặp (order_items, order_payments) đã nhất quán về mặt số tiền.

    Vì sao phải cộng bù tiền? Nếu chỉ thêm dòng hàng mà quên cộng tiền, bảng ``fact_orders``
    tầng Gold sẽ ghi nhận ``item_revenue`` lớn hơn hẳn ``payment_total`` cho riêng lô này,
    làm sai lệch mọi phép đối soát doanh thu về sau — một cái bẫy rất dễ mắc.
    """
    hot_rows = items[items["product_id"].isin(hot_products)].copy()
    if hot_rows.empty:
        return items, payments

    # Mỗi đơn chỉ nhân bản tối đa 1 dòng hàng hot để khối lượng tăng có kiểm soát.
    hot_rows = hot_rows.drop_duplicates(subset=["order_id"], keep="first")
    next_item_id = (
        items.assign(_n=items["order_item_id"].astype(int))
        .groupby("order_id")["_n"]
        .max()
        .add(1)
    )
    hot_rows["order_item_id"] = hot_rows["order_id"].map(next_item_id).astype(str)
    combined_items = pd.concat([items, hot_rows], ignore_index=True)

    # Cộng bù tiền hàng + cước vào dòng thanh toán đầu tiên của từng đơn.
    extra = (
        hot_rows.assign(
            _amount=hot_rows["price"].astype(float) + hot_rows["freight_value"].astype(float)
        )
        .groupby("order_id")["_amount"]
        .sum()
    )
    updated = payments.copy()
    first_payment = updated["payment_sequential"].astype(int).groupby(updated["order_id"]).transform("min")
    is_first = updated["payment_sequential"].astype(int) == first_payment
    bonus = updated["order_id"].map(extra).fillna(0.0)
    updated.loc[is_first, "payment_value"] = (
        updated.loc[is_first, "payment_value"].astype(float) + bonus[is_first]
    ).round(2).astype(str)
    return combined_items, updated


def build_catchup(source: OlistSource, month: str) -> dict[str, pd.DataFrame]:
    """BỘ 3 — Dồn tích bù sau sự cố: nạp bù trọn vẹn 1 tháng với trạng thái hỗn hợp.

    Ý nghĩa thực tế : hệ thống mất kết nối 3-5 ngày, sau đó toàn bộ dữ liệu ứ đọng được
    nạp bù một lần. Trong đống dữ liệu đó có cả đơn mới tạo lẫn đơn cũ vừa đổi trạng thái.

    Mục tiêu kiểm thử: đây là bài kiểm tra cơ chế **Upsert (MERGE INTO)** của Apache Iceberg.
    Toàn bộ ``order_id`` trong bộ này ĐÃ TỒN TẠI trong tầng Silver với trạng thái cũ. Sau khi
    nạp bù, mỗi ``order_id`` phải chỉ còn ĐÚNG MỘT dòng mang trạng thái MỚI nhất — không được
    sinh thêm bản ghi trùng, cũng không được giữ lại trạng thái cũ đã lạc hậu.

    Cách tạo 3 nhóm trạng thái (chia đều, xoay vòng theo chỉ số dòng):
      - Nhóm 0 -> ``created``   : xóa trắng 3 mốc duyệt/giao (đơn vừa tạo, chưa xử lý).
      - Nhóm 1 -> ``invoiced``  : giữ mốc duyệt, xóa 2 mốc giao hàng (đã xuất hóa đơn).
      - Nhóm 2 -> ``delivered`` : giữ nguyên toàn bộ mốc thời gian (đã giao thành công).
    """
    purchase = pd.to_datetime(source.orders["order_purchase_timestamp"], errors="coerce")
    selected = source.orders[purchase.dt.strftime("%Y-%m") == month].copy().reset_index(drop=True)
    if selected.empty:
        raise ValueError(f"Không có đơn hàng nào trong tháng {month}")

    group = selected.index % 3
    # Nhóm 0: đơn vừa được tạo, chưa duyệt chưa giao.
    selected.loc[group == 0, "order_status"] = "created"
    selected.loc[
        group == 0,
        ["order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date"],
    ] = ""
    # Nhóm 1: đã xuất hóa đơn, chưa bàn giao vận chuyển.
    selected.loc[group == 1, "order_status"] = "invoiced"
    selected.loc[
        group == 1, ["order_delivered_carrier_date", "order_delivered_customer_date"]
    ] = ""
    # Nhóm 2: đã giao thành công — chỉ nhận trạng thái delivered nếu thực sự có ngày khách nhận,
    # nếu không sẽ vi phạm luật DELIVERED_ORDER_MISSING_DELIVERY_DATE và bị cách ly.
    has_delivery = selected["order_delivered_customer_date"].astype(str).str.strip() != ""
    selected.loc[(group == 2) & has_delivery, "order_status"] = "delivered"

    return slice_by_orders(
        OlistSource(**{**source.__dict__, "orders": selected}), selected["order_id"]
    )


def _busiest_month(orders: pd.DataFrame) -> str:
    """Tìm tháng có nhiều đơn hàng nhất để bộ dữ liệu dồn tích sát với kịch bản xấu nhất."""
    purchase = pd.to_datetime(orders["order_purchase_timestamp"], errors="coerce")
    return str(purchase.dt.strftime("%Y-%m").value_counts().idxmax())


def main(argv: list[str] | None = None) -> int:
    """Điểm khởi nhập CLI: nạp dữ liệu gốc, dựng 3 bộ dữ liệu và in báo cáo tổng kết."""
    parser = argparse.ArgumentParser(
        description="Sinh 3 bộ dữ liệu kiểm thử tải và xử lý tăng dần từ dữ liệu Olist thật."
    )
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"), help="Thư mục CSV gốc.")
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/synthetic"), help="Thư mục chứa kết quả."
    )
    parser.add_argument(
        "--base-date",
        type=lambda value: datetime.strptime(value, "%Y-%m-%d").date(),
        default=date.today(),
        help="Ngày đích để dời các mốc mua hàng về (mặc định: hôm nay).",
    )
    parser.add_argument(
        "--offpeak-orders", type=int, default=500, help="Số đơn của bộ giờ thấp điểm."
    )
    parser.add_argument(
        "--flash-sale-orders", type=int, default=40_000, help="Số đơn của bộ Flash-Sale."
    )
    parser.add_argument(
        "--hot-categories", type=int, default=5, help="Số danh mục được coi là hàng hot."
    )
    parser.add_argument(
        "--catchup-month", default=None, help="Tháng dồn tích dạng YYYY-MM (mặc định: tháng đông đơn nhất)."
    )
    args = parser.parse_args(argv)
    enable_utf8_output()

    print(f"Đang nạp dữ liệu Olist gốc từ {args.raw_dir} ...")
    source = OlistSource.load(args.raw_dir)
    print(f"Đã nạp {len(source.orders):,} đơn hàng và {len(source.order_items):,} dòng hàng.")

    # --- Bộ 1: Giờ thấp điểm ------------------------------------------------------------
    destination = args.output_dir / "01_offpeak"
    counts = write_dataset(build_offpeak(source, args.base_date, args.offpeak_orders), destination)
    print_summary(
        f"BỘ 1 — GIỜ THẤP ĐIỂM ({args.base_date} 02:00-05:00)",
        destination,
        counts,
        "Overhead lô nhỏ (small-file) + nạp tăng dần không nhân đôi dữ liệu",
    )

    # --- Bộ 2: Flash-Sale ---------------------------------------------------------------
    destination = args.output_dir / "02_flash_sale"
    counts = write_dataset(
        build_flash_sale(source, args.base_date, args.flash_sale_orders, args.hot_categories),
        destination,
    )
    print_summary(
        f"BỘ 2 — FLASH-SALE ({args.base_date} 12:00-13:59)",
        destination,
        counts,
        "Chịu tải đột biến: OOM Spark Worker + điều tiết Airflow pool lakehouse_writes",
    )

    # --- Bộ 3: Dồn tích bù --------------------------------------------------------------
    month = args.catchup_month or _busiest_month(source.orders)
    destination = args.output_dir / "03_catchup"
    counts = write_dataset(build_catchup(source, month), destination)
    print_summary(
        f"BỘ 3 — DỒN TÍCH BÙ (trọn tháng {month}, trạng thái hỗn hợp)",
        destination,
        counts,
        "Cơ chế Upsert MERGE INTO của Iceberg: cập nhật trạng thái không sinh bản ghi thừa",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
