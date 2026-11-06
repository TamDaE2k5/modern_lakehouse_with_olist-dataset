"""Sinh 3 bộ dữ liệu kiểm thử KHẢ NĂNG CHỊU LỖI (Chaos Testing) từ dữ liệu Olist thật (Task 3).

MỤC ĐÍCH
--------
Một Data Lakehouse chạy thật sẽ liên tục nhận phải dữ liệu bẩn: hệ thống nguồn đổi schema
mà không báo, ứng dụng ghi sai định dạng ngày, khóa ngoại trỏ vào hư không, message queue
gửi lặp bản ghi. Ba bộ dữ liệu dưới đây **tiêm lỗi có chủ đích** vào dữ liệu thật để chứng
minh hệ thống phòng vệ đúng như thiết kế, thay vì chỉ tin vào lời hứa trong tài liệu.

    Dữ liệu bẩn ──► Spark Silver Transformation ──┬──► Hợp lệ ──► lakehouse.silver.*
                                                  └──► Lỗi    ──► lakehouse.quarantine.*

BA KỊCH BẢN CHAOS
-----------------
  Bộ 4 — Metadata bẩn & trôi lược đồ : ngày sai định dạng, chữ trong cột tiền, cột lạ.
  Bộ 5 — Vi phạm toàn vẹn khóa       : khóa chính rỗng, khóa ngoại mồ côi, số tiền âm.
  Bộ 6 — Trùng lặp & xung đột trạng thái: dòng trùng khít và trùng khóa chính khác nội dung.

NGUYÊN TẮC THIẾT KẾ QUAN TRỌNG
------------------------------
Mỗi lỗi tiêm vào đều được **đếm chính xác** và ghi ra ``_injection_manifest.json`` ngay cạnh
bộ dữ liệu. Nhờ vậy khi chạy pipeline xong, ta đối chiếu được số bản ghi trong
``lakehouse.quarantine.*`` với số lỗi đã tiêm — nếu khớp thì cơ chế cách ly hoạt động đúng,
nếu lệch thì biết ngay có lỗi lọt lưới. Không có bản kê này thì bài test chaos chỉ là cảm tính.

ĐẦU VÀO : ``data/raw/``.
ĐẦU RA  : ``data/synthetic/04_dirty_metadata/``, ``05_integrity_violations/``, ``06_duplicates/``

CÁCH CHẠY:
    python scripts/generate_chaos_datasets.py
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from olist_dataset_common import (  # noqa: E402
    CSV_FILES,
    OlistSource,
    enable_utf8_output,
    print_summary,
    slice_by_orders,
    write_dataset,
)


RANDOM_SEED = 20261106

# Các giá trị ngày tháng hỏng được tiêm vào. Mỗi giá trị đại diện cho một kiểu lỗi thật
# thường gặp khi tích hợp dữ liệu từ nhiều hệ thống nguồn khác nhau:
BROKEN_TIMESTAMPS = (
    "23/10/26 10h15m",      # Định dạng ngày kiểu Brazil trộn ký tự giờ-phút tự chế
    "2026-10-23T10:15:00",  # ISO-8601 có chữ T — Silver chỉ nhận 'yyyy-MM-dd HH:mm:ss'
    "invalid_date",         # Chuỗi rác do ứng dụng nguồn ghi nhầm
    "N/A",                  # Giá trị khuyết được mã hóa thành chuỗi thay vì để trống
    "0000-00-00 00:00:00",  # "Ngày số không" đặc trưng của MySQL
)

# Các giá trị tiền tệ hỏng. Lưu ý "1.250,50R$" dùng dấu phân cách kiểu Brazil
# (chấm cho hàng nghìn, phẩy cho thập phân) kèm ký hiệu tiền tệ — rất hay gặp thực tế.
BROKEN_MONEY = ("free_shipping", "1.250,50R$", "N/A", "-", "null")

# Hai cột lạ dùng cho bài test trôi lược đồ (schema drift).
DRIFT_COLUMNS = ("promotional_code_internal", "hack_tracking_id")


def _every_nth(frame: pd.DataFrame, step: int) -> pd.Index:
    """Chọn ra các dòng ở vị trí 0, step, 2*step... để lỗi rải đều thay vì dồn một cụm."""
    return frame.index[:: max(step, 1)]


def build_dirty_metadata(base: dict[str, pd.DataFrame]) -> tuple[dict[str, pd.DataFrame], dict]:
    """BỘ 4 — Metadata bẩn & Trôi lược đồ (Schema Drift).

    Các lỗi được tiêm:
      1. Ngày tháng sai định dạng ở 2 cột thời gian TÙY CHỌN (``order_approved_at`` và
         ``order_delivered_carrier_date``). Silver có luật ``INVALID_ORDER_APPROVED_AT`` /
         ``INVALID_ORDER_DELIVERED_CARRIER_DATE`` bắt đúng trường hợp "chuỗi thô có dữ liệu
         nhưng ép kiểu timestamp ra NULL".
      2. Chuỗi văn bản trong cột tiền ``price`` và ``freight_value`` của order_items.
         Spark ép kiểu ``decimal(18,2)`` sẽ cho NULL -> luật ``INVALID_PRICE`` bắt được.
      3. (Ghi riêng ra thư mục con ``schema_drift/``) Bảng orders có thêm 2 cột lạ.

    Mục tiêu kiểm thử: pipeline KHÔNG được sập. Dữ liệu hỏng phải được ép NULL có kiểm soát
    rồi bóc tách sang vùng cách ly, phần dữ liệu lành lặn còn lại vẫn chảy bình thường về Silver.

    Vì sao chỉ tiêm vào cột thời gian TÙY CHỌN? Nếu phá ``order_purchase_timestamp`` (cột bắt
    buộc) thì bảng ``order_items`` tham chiếu tới nó cũng hỏng dây chuyền, làm lẫn lộn nguyên
    nhân gốc giữa "ngày hỏng" và "khóa ngoại mồ côi" — ta sẽ không còn phân tách được số liệu.
    """
    frames = {key: value.copy().reset_index(drop=True) for key, value in base.items()}
    counts: dict[str, int] = {}

    orders = frames["orders"]
    for column, step in (("order_approved_at", 7), ("order_delivered_carrier_date", 11)):
        targets = _every_nth(orders, step)
        orders.loc[targets, column] = [
            BROKEN_TIMESTAMPS[index % len(BROKEN_TIMESTAMPS)] for index in range(len(targets))
        ]
        counts[f"orders.{column}_sai_dinh_dang"] = len(targets)

    items = frames["order_items"]
    for column, step in (("price", 9), ("freight_value", 13)):
        targets = _every_nth(items, step)
        items.loc[targets, column] = [
            BROKEN_MONEY[index % len(BROKEN_MONEY)] for index in range(len(targets))
        ]
        counts[f"order_items.{column}_chuoi_van_ban"] = len(targets)

    return frames, counts


def build_integrity_violations(
    base: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], dict]:
    """BỘ 5 — Vi phạm toàn vẹn khóa & Dữ liệu phải bị cách ly (Quarantine).

    Các lỗi được tiêm và luật Silver tương ứng sẽ bắt được chúng:
      1. ``order_id`` rỗng trong orders            -> MISSING_ORDER_ID
      2. ``customer_id`` rỗng trong orders         -> MISSING_ORDER_CUSTOMER_ID
      3. ``order_id`` mồ côi trong order_items     -> MISSING_ORDER_REFERENCE
         (trỏ tới mã đơn 'chaos-orphan-xxxx' không hề tồn tại trong bảng orders)
      4. ``price`` âm trong order_items            -> NEGATIVE_PRICE
      5. ``payment_installments`` âm               -> INVALID_PAYMENT_INSTALLMENTS
      6. ``payment_value`` = 0 cho đơn 'delivered' -> bất thường nghiệp vụ cần theo dõi

    Mục tiêu kiểm thử: chứng minh Medallion Quarantine Pattern hoạt động nghiêm ngặt —
    100% các bản ghi sai phạm phải nằm trong ``lakehouse.quarantine.*``, tuyệt đối không
    một dòng nào được phép lọt vào tầng Silver hay Gold.
    """
    frames = {key: value.copy().reset_index(drop=True) for key, value in base.items()}
    counts: dict[str, int] = {}

    # 1 & 2. Khóa chính / khóa ngoại bắt buộc bị bỏ trống.
    orders = frames["orders"]
    blank_order_ids = _every_nth(orders, 29)
    orders.loc[blank_order_ids, "order_id"] = ""
    counts["orders.order_id_rong"] = len(blank_order_ids)

    blank_customers = orders.index[3::31]
    orders.loc[blank_customers, "customer_id"] = ""
    counts["orders.customer_id_rong"] = len(blank_customers)

    # 3. Khóa ngoại mồ côi: thêm hẳn các dòng hàng trỏ tới đơn hàng không tồn tại.
    items = frames["order_items"]
    orphans = items.head(200).copy()
    orphans["order_id"] = [f"chaos-orphan-{index:06d}" for index in range(len(orphans))]
    counts["order_items.khoa_ngoai_mo_coi"] = len(orphans)

    # 4. Đơn giá âm — giá trị phi lý về mặt nghiệp vụ.
    negative_prices = items.index[5::17]
    items.loc[negative_prices, "price"] = "-50.00"
    counts["order_items.don_gia_am"] = len(negative_prices)
    frames["order_items"] = pd.concat([items, orphans], ignore_index=True)

    # 5 & 6. Bất thường trong bảng thanh toán.
    payments = frames["order_payments"]
    negative_installments = payments.index[2::23]
    payments.loc[negative_installments, "payment_installments"] = "-2"
    counts["order_payments.so_ky_tra_gop_am"] = len(negative_installments)

    delivered_orders = set(orders.loc[orders["order_status"] == "delivered", "order_id"])
    zero_mask = payments["order_id"].isin(delivered_orders) & (payments.index % 37 == 0)
    payments.loc[zero_mask, "payment_value"] = "0.00"
    counts["order_payments.thanh_toan_0_du_da_giao"] = int(zero_mask.sum())

    return frames, counts


def build_duplicates(base: dict[str, pd.DataFrame]) -> tuple[dict[str, pd.DataFrame], dict]:
    """BỘ 6 — Bản ghi trùng lặp & Xung đột trạng thái.

    Các lỗi được tiêm:
      1. Trùng khít hoàn toàn (Exact Duplicate): nhân đôi nguyên văn mọi trường của
         một số dòng orders — mô phỏng message queue gửi lại do mất xác nhận (at-least-once).
      2. Trùng khóa chính nhưng khác nội dung (Primary Key Collision): cùng ``order_id``
         nhưng một dòng ghi ``shipped``, dòng kia ghi ``canceled``, thời điểm mua hàng lệch
         nhau đúng 5 giây — mô phỏng hai hệ thống nguồn cùng báo cáo về một đơn.

    Mục tiêu kiểm thử: thuật toán khử trùng lặp ở tầng Silver (xem
    ``transformation/silver/_common.py::deterministic_result``) phải giữ lại ĐÚNG MỘT bản ghi
    cho mỗi ``order_id``, và bản được giữ phải là bản ghi MỚI NHẤT theo nghiệp vụ.

    Lưu ý kỹ thuật quan trọng: các dòng xung đột được cố ý tạo sao cho CẢ HAI đều hợp lệ
    (0 lỗi nghiệp vụ). Đây là phép thử khắc nghiệt nhất cho tiêu chí phân định thắng-thua của
    thuật toán khử trùng, vì khi đó nó buộc phải dựa vào mốc thời gian nghiệp vụ để quyết định.
    """
    frames = {key: value.copy().reset_index(drop=True) for key, value in base.items()}
    counts: dict[str, int] = {}
    orders = frames["orders"]

    # 1. Trùng khít hoàn toàn.
    exact = orders.head(300).copy()
    counts["orders.trung_khit_hoan_toan"] = len(exact)

    # 2. Trùng khóa chính, khác trạng thái và lệch 5 giây.
    collisions = orders.iloc[300:600].copy()
    purchase = pd.to_datetime(collisions["order_purchase_timestamp"], errors="coerce")
    collisions["order_purchase_timestamp"] = (
        purchase + pd.Timedelta(seconds=5)
    ).dt.strftime("%Y-%m-%d %H:%M:%S")
    collisions["order_status"] = "canceled"
    # Đơn đã hủy thì không thể có ngày giao hàng — xóa để bản ghi vẫn hợp lệ về nghiệp vụ.
    collisions[
        ["order_delivered_carrier_date", "order_delivered_customer_date"]
    ] = ""
    counts["orders.trung_khoa_chinh_khac_trang_thai"] = len(collisions)

    frames["orders"] = pd.concat([orders, exact, collisions], ignore_index=True)
    return frames, counts


def _write_schema_drift_variant(base: dict[str, pd.DataFrame], destination: Path) -> int:
    """Ghi riêng biến thể orders CÓ THÊM CỘT LẠ ra thư mục con ``schema_drift/``.

    Vì sao phải tách riêng thay vì để chung trong bộ 9 file? Vì cả hai cổng kiểm tra đầu vào
    (``validate_dataset`` và ``validate_staged_files``) đều so khớp header TUYỆT ĐỐI. Nếu nhét
    cột lạ vào bộ chính thì toàn bộ lô dữ liệu bị chặn ngay từ cổng, và ta sẽ không quan sát
    được hành vi xử lý giá trị bẩn ở các tầng sau nữa.

    Tách ra hai bài test riêng cho hai hành vi phòng vệ khác nhau:
      - Bộ chính (9 file đúng header): kiểm tra **độ khoan dung** — giá trị bẩn bị ép NULL
        và cách ly, pipeline vẫn chạy tiếp.
      - Biến thể này: kiểm tra **độ nghiêm ngặt** — cột lạ bị cổng Bronze chặn đứng kèm
        thông báo lỗi chỉ rõ header nhận được khác header mong đợi ở chỗ nào.
    """
    drift_dir = destination / "schema_drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
    orders = base["orders"].head(500).copy()
    orders[DRIFT_COLUMNS[0]] = "PROMO-INTERNAL-2026"
    orders[DRIFT_COLUMNS[1]] = "trk-" + orders.index.astype(str)
    orders.to_csv(drift_dir / CSV_FILES["orders"], index=False, encoding="utf-8")
    (drift_dir / "README.txt").write_text(
        "Bien the kiem thu TROI LUOC DO (schema drift).\n"
        f"File orders o day co THEM 2 cot la: {', '.join(DRIFT_COLUMNS)}.\n\n"
        "Cach dung: day rieng file nay len MinIO raw/olist/ roi chay DAG olist_transformation.\n"
        "KY VONG: task bronze_ingestion FAIL co kiem soat voi thong bao\n"
        "  'expected header (...), got (...)'\n"
        "va ghi nhan ban ghi FAILED trong lakehouse.control.ingestion_batches.\n"
        "Day la hanh vi DUNG: cong Bronze chan truoc khi du lieu la kip chay vao Lakehouse.\n",
        encoding="utf-8",
    )
    return len(orders)


def main(argv: list[str] | None = None) -> int:
    """Điểm khởi nhập CLI: dựng 3 bộ chaos kèm bản kê số lỗi đã tiêm."""
    parser = argparse.ArgumentParser(
        description="Sinh 3 bộ dữ liệu kiểm thử khả năng chịu lỗi (chaos testing)."
    )
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/synthetic"))
    parser.add_argument(
        "--base-orders",
        type=int,
        default=2_000,
        help="Số đơn hàng gốc làm nền để tiêm lỗi (mặc định 2.000 — đủ lớn để thống kê có ý nghĩa "
        "mà vẫn chạy nhanh, giúp vòng lặp kiểm thử chaos ngắn).",
    )
    args = parser.parse_args(argv)
    enable_utf8_output()

    print(f"Đang nạp dữ liệu Olist gốc từ {args.raw_dir} ...")
    source = OlistSource.load(args.raw_dir)
    sample = source.orders.sample(n=args.base_orders, random_state=RANDOM_SEED)
    base = slice_by_orders(source, sample["order_id"])
    print(f"Đã cắt nền {len(base['orders']):,} đơn hàng sạch để tiêm lỗi.")

    builders = (
        (
            "04_dirty_metadata",
            "BỘ 4 — METADATA BẨN & TRÔI LƯỢC ĐỒ",
            "Pipeline không sập: giá trị hỏng bị ép NULL có kiểm soát rồi cách ly",
            build_dirty_metadata,
        ),
        (
            "05_integrity_violations",
            "BỘ 5 — VI PHẠM TOÀN VẸN KHÓA & CÁCH LY",
            "Medallion Quarantine Pattern: 100% bản ghi sai phạm vào lakehouse.quarantine.*",
            build_integrity_violations,
        ),
        (
            "06_duplicates",
            "BỘ 6 — TRÙNG LẶP & XUNG ĐỘT TRẠNG THÁI",
            "Khử trùng lặp Silver: mỗi order_id chỉ còn đúng 1 bản ghi mới nhất",
            build_duplicates,
        ),
    )

    for folder, title, purpose, builder in builders:
        destination = args.output_dir / folder
        frames, injected = builder(base)
        counts = write_dataset(frames, destination)

        if folder == "04_dirty_metadata":
            injected["schema_drift.orders_them_cot_la"] = _write_schema_drift_variant(
                base, destination
            )

        # Bản kê số lỗi đã tiêm — dùng để đối soát với số bản ghi trong vùng cách ly.
        manifest = {
            "dataset": folder,
            "purpose": purpose,
            "base_orders": args.base_orders,
            "row_counts": counts,
            "injected_faults": injected,
            "total_injected_faults": sum(injected.values()),
        }
        (destination / "_injection_manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        print_summary(title, destination, counts, purpose)
        print("  LỖI ĐÃ TIÊM CÓ CHỦ ĐÍCH (đối soát với lakehouse.quarantine.*):")
        for name, value in injected.items():
            print(f"    {name:<48} {value:>8,}")
        print(f"    {'TỔNG SỐ LỖI TIÊM VÀO':<48} {sum(injected.values()):>8,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
