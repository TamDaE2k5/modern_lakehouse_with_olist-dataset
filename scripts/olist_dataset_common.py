"""Thư viện dùng chung để cắt lát và xuất các bộ dữ liệu thử nghiệm từ dữ liệu Olist thật.

VÌ SAO CẦN MODULE NÀY?
----------------------
Hai script sinh dữ liệu (`generate_load_datasets.py` cho test tải và
`generate_chaos_datasets.py` cho test chịu lỗi) đều phải làm chung một việc nền:

    "Cho trước một tập ``order_id``, hãy cắt ra đủ 9 file CSV Olist nhất quán tham
     chiếu lẫn nhau, đúng tên file và đúng thứ tự cột mà Lakehouse yêu cầu."

Đây là ràng buộc BẮT BUỘC, vì cả hai cổng kiểm tra đầu vào của hệ thống đều rất nghiêm:

  * ``ingestion/validate_dataset.py`` yêu cầu thư mục nguồn chứa **đúng 9 file**,
    không thiếu cũng không thừa.
  * ``transformation/bronze/job.py::validate_staged_files`` đọc header của từng đối tượng
    trên MinIO và so khớp **chính xác từng cột theo đúng thứ tự** với registry.

Gom phần nền này vào một chỗ giúp hai script kia chỉ còn phải lo đúng phần nghiệp vụ
riêng của mình (biến tấu thời gian, nhân bản hàng hot, tiêm lỗi...), đồng thời tránh
nhân bản 200 dòng code giống hệt nhau ở hai nơi.

ĐẦU VÀO : thư mục ``data/raw/`` chứa 9 file CSV gốc tải từ Kaggle.
ĐẦU RA  : một thư mục con trong ``data/synthetic/<tên-bộ>/`` chứa đủ 9 file CSV.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import sys

import pandas as pd


def enable_utf8_output() -> None:
    """Ép luồng stdout/stderr sang UTF-8 để in được tiếng Việt trên console Windows.

    Vì sao cần hàm này? Console Windows mặc định dùng bảng mã cũ theo ngôn ngữ hệ thống
    (cp1258, cp932...). Khi script in chuỗi tiếng Việt có dấu, Python ném ngay
    ``UnicodeEncodeError`` và làm sập script giữa chừng dù phần xử lý dữ liệu hoàn toàn đúng.
    Gọi hàm này ở đầu mỗi CLI là đủ để mọi thông báo tiếng Việt hiển thị chuẩn trên cả
    Windows (PowerShell/CMD) lẫn Linux container.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")


# Tên 9 file CSV nguồn, ánh xạ theo khóa ngắn gọn dùng trong code.
# Thứ tự và tên file phải khớp tuyệt đối với `transformation/common/registry.py`.
CSV_FILES: dict[str, str] = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv",
}

# 5 cột mốc thời gian trong bảng orders, theo đúng thứ tự vòng đời đơn hàng.
# Khi dịch chuyển thời gian BẮT BUỘC phải dịch cả 5 cột cùng một lượng delta,
# nếu không Silver sẽ bắt lỗi nghịch lý thời gian (APPROVAL_BEFORE_PURCHASE,
# DELIVERY_BEFORE_CARRIER...) và đẩy toàn bộ đơn hàng vào vùng cách ly.
ORDER_TIMESTAMP_COLUMNS = (
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
)

# Định dạng thời gian duy nhất mà tầng Silver chấp nhận (xem silver/orders.py).
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# Giới hạn số dòng geolocation mang theo mỗi bộ dữ liệu.
# Vì sao cần giới hạn? File gốc nặng 61MB với hơn 1 triệu dòng tọa độ; nếu bộ nào cũng
# chép nguyên thì 6 bộ dữ liệu sẽ ngốn ~370MB đĩa một cách vô ích. Tầng Silver chỉ cần
# đủ tọa độ cho các mã bưu chính thực sự xuất hiện trong bộ dữ liệu đó.
MAX_GEOLOCATION_ROWS = 50_000


@dataclass(frozen=True)
class OlistSource:
    """Toàn bộ 9 bảng Olist gốc đã nạp sẵn vào bộ nhớ dưới dạng DataFrame pandas.

    Đầu vào : thư mục ``data/raw``.
    Xử lý   : đọc mọi cột dưới dạng chuỗi (``dtype=str``) để bảo toàn 100% ký tự gốc.
    Đầu ra  : một đối tượng chứa 9 DataFrame, dùng lại cho cả 6 bộ dữ liệu.

    Vì sao đọc tất cả dưới dạng chuỗi? Nếu để pandas tự suy luận kiểu, nó sẽ biến
    ``order_item_id`` thành số nguyên rồi ghi lại thành "1.0", hoặc biến ô trống thành
    chuỗi "nan" — cả hai đều làm hỏng dữ liệu so với bản gốc. Tầng Bronze cũng đọc mọi
    cột dưới dạng ``string``, nên đọc-ghi dạng chuỗi là cách giữ dữ liệu trung thực nhất.
    """

    customers: pd.DataFrame
    orders: pd.DataFrame
    order_items: pd.DataFrame
    order_payments: pd.DataFrame
    order_reviews: pd.DataFrame
    products: pd.DataFrame
    sellers: pd.DataFrame
    geolocation: pd.DataFrame
    product_category_translation: pd.DataFrame

    @classmethod
    def load(cls, raw_dir: Path) -> OlistSource:
        """Nạp đủ 9 file CSV gốc từ thư mục ``raw_dir`` vào bộ nhớ."""
        if not raw_dir.is_dir():
            raise FileNotFoundError(f"Không tìm thấy thư mục dữ liệu gốc: {raw_dir}")
        frames = {}
        for key, filename in CSV_FILES.items():
            path = raw_dir / filename
            if not path.is_file():
                raise FileNotFoundError(f"Thiếu file CSV nguồn: {path}")
            frames[key] = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
        return cls(**frames)


def slice_by_orders(source: OlistSource, order_ids: pd.Series) -> dict[str, pd.DataFrame]:
    """Cắt ra một bộ 9 bảng nhất quán, chỉ chứa dữ liệu liên quan tới tập ``order_ids``.

    Đầu vào : bộ dữ liệu gốc + danh sách mã đơn hàng cần giữ lại.
    Xử lý   : lọc theo đúng chuỗi khóa ngoại của mô hình Olist
              ``orders -> order_items/order_payments/order_reviews -> products/sellers``
              và ``orders -> customers -> geolocation``.
    Đầu ra  : dict gồm 9 DataFrame đã lọc, sẵn sàng ghi ra đĩa.

    Lưu ý nghiệp vụ: các bảng chiều (customers, products, sellers) được cắt theo đúng
    những khóa thực sự được tham chiếu. Nhờ vậy bộ dữ liệu vừa nhỏ gọn vừa KHÔNG sinh ra
    khóa ngoại mồ côi ngoài ý muốn — điều rất quan trọng, vì bộ Dataset 5 cần khóa mồ côi
    là lỗi được tiêm vào CÓ CHỦ ĐÍCH, chứ không phải tai nạn của script sinh dữ liệu.
    """
    keep = set(order_ids)
    orders = source.orders[source.orders["order_id"].isin(keep)].copy()
    items = source.order_items[source.order_items["order_id"].isin(keep)].copy()
    payments = source.order_payments[source.order_payments["order_id"].isin(keep)].copy()
    reviews = source.order_reviews[source.order_reviews["order_id"].isin(keep)].copy()

    customer_ids = set(orders["customer_id"])
    customers = source.customers[source.customers["customer_id"].isin(customer_ids)].copy()
    products = source.products[source.products["product_id"].isin(set(items["product_id"]))].copy()
    sellers = source.sellers[source.sellers["seller_id"].isin(set(items["seller_id"]))].copy()

    # Geolocation nối với khách hàng và người bán qua tiền tố mã bưu chính (CEP).
    zips = set(customers["customer_zip_code_prefix"]) | set(sellers["seller_zip_code_prefix"])
    geolocation = source.geolocation[
        source.geolocation["geolocation_zip_code_prefix"].isin(zips)
    ].head(MAX_GEOLOCATION_ROWS).copy()

    return {
        "customers": customers,
        "orders": orders,
        "order_items": items,
        "order_payments": payments,
        "order_reviews": reviews,
        "products": products,
        "sellers": sellers,
        "geolocation": geolocation,
        # Bảng dịch danh mục chỉ có ~71 dòng nên luôn mang theo nguyên vẹn.
        "product_category_translation": source.product_category_translation.copy(),
    }


def shift_order_timestamps(orders: pd.DataFrame, target_purchase: pd.Series) -> pd.DataFrame:
    """Dời toàn bộ vòng đời thời gian của đơn hàng sang mốc mua hàng mới, giữ nguyên thứ tự.

    Đầu vào : bảng orders + chuỗi thời điểm mua hàng MỚI mong muốn cho từng dòng.
    Xử lý   : tính ``delta = mốc_mới - mốc_cũ`` của riêng ``order_purchase_timestamp``,
              rồi cộng CHÍNH delta đó vào cả 5 cột thời gian.
    Đầu ra  : bảng orders mới với thời gian đã dời, các ô vốn trống vẫn giữ trống.

    Vì sao phải cộng cùng một delta cho cả 5 cột? Vì tầng Silver kiểm tra nghiêm ngặt tính
    hợp lý của dòng đời đơn hàng (duyệt sau khi mua, giao sau khi bưu cục nhận...). Nếu chỉ
    dời riêng cột mua hàng, mọi đơn hàng sẽ vi phạm ``APPROVAL_BEFORE_PURCHASE`` và bị cách
    ly sạch — bộ dữ liệu test tải sẽ trở nên vô nghĩa vì không còn dòng hợp lệ nào.
    """
    result = orders.copy()
    original_purchase = pd.to_datetime(result["order_purchase_timestamp"], errors="coerce")
    delta = pd.to_datetime(target_purchase) - original_purchase

    for column in ORDER_TIMESTAMP_COLUMNS:
        values = pd.to_datetime(result[column], errors="coerce")
        shifted = values + delta
        # Ô nào vốn trống/không parse được thì giữ nguyên chuỗi rỗng, không bịa dữ liệu.
        result[column] = shifted.dt.strftime(TIMESTAMP_FORMAT).fillna("")
    return result


def write_dataset(frames: dict[str, pd.DataFrame], destination: Path) -> dict[str, int]:
    """Ghi đủ 9 file CSV ra thư mục đích theo đúng tên file mà Lakehouse yêu cầu.

    Đầu vào : dict 9 DataFrame + thư mục đích.
    Xử lý   : xóa sạch thư mục cũ (đảm bảo chạy lại cho kết quả giống hệt - idempotent),
              rồi ghi từng file với ``index=False`` để không sinh thêm cột thứ tự thừa.
    Đầu ra  : dict đếm số dòng của từng bảng, dùng để in báo cáo và đưa vào tài liệu.
    """
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True, exist_ok=True)

    counts: dict[str, int] = {}
    for key, filename in CSV_FILES.items():
        frame = frames[key]
        if frame.empty:
            raise ValueError(
                f"Bảng '{key}' rỗng — tầng Bronze sẽ từ chối với lỗi "
                "'staged source contains zero data rows'"
            )
        frame.to_csv(destination / filename, index=False, encoding="utf-8")
        counts[key] = len(frame)
    return counts


def print_summary(name: str, destination: Path, counts: dict[str, int], purpose: str) -> None:
    """In bảng tóm tắt kết quả sinh dữ liệu ra màn hình cho người vận hành đọc."""
    total_mb = sum(path.stat().st_size for path in destination.glob("*.csv")) / 1024 / 1024
    print(f"\n{'=' * 78}")
    print(f"  {name}")
    print(f"  Mục tiêu kiểm thử : {purpose}")
    print(f"  Thư mục đầu ra    : {destination}")
    print(f"  Tổng dung lượng   : {total_mb:.1f} MB")
    print(f"{'-' * 78}")
    for key, count in counts.items():
        print(f"    {key:<32} {count:>10,} dòng")
    print(f"{'=' * 78}")
