"""Đẩy một bộ dữ liệu CSV lên vùng raw của MinIO để pipeline có thể nạp vào Bronze.

VAI TRÒ TRONG QUY TRÌNH KIỂM THỬ
--------------------------------
Tầng Bronze luôn đọc dữ liệu từ đúng một vị trí cố định trên MinIO: ``s3://raw/olist/``.
Vì vậy, để kiểm thử bất kỳ bộ dữ liệu nào trong 6 bộ đã sinh ra, ta phải THAY THẾ nội dung
ở vị trí đó rồi mới kích hoạt DAG.

    data/synthetic/<tên bộ>/*.csv  ──[script này]──►  s3://raw/olist/*.csv  ──► DAG Bronze

ĐẦU VÀO : đường dẫn thư mục chứa các file CSV (thường là một bộ trong ``data/synthetic/``).
XỬ LÝ   : chạy một container ``mc`` (MinIO Client) tạm thời, gắn vào mạng nội bộ của cụm,
          xóa sạch tiền tố raw/olist/ cũ rồi chép toàn bộ CSV mới lên.
ĐẦU RA  : in danh sách đối tượng đã nạp kèm dung lượng; mã thoát khác 0 nếu thất bại.

VÌ SAO DÙNG CONTAINER ``mc`` MÀ KHÔNG DÙNG boto3 TRÊN MÁY HOST?
Vì endpoint ``http://minio:9000`` chỉ phân giải được bên trong mạng Docker. Chạy ``mc``
trong mạng đó vừa tránh phải cài thêm thư viện Python trên máy host, vừa dùng đúng đường
mạng mà các job Spark thật sự sử dụng — nên kết quả kiểm thử sát thực tế hơn.

VÌ SAO PHẢI XÓA SẠCH TRƯỚC KHI CHÉP (``mc rm --recursive``)?
Nếu chỉ chép đè, các file thừa của bộ dữ liệu trước vẫn nằm lại và cổng kiểm tra
``validate_staged_files`` sẽ thấy "thừa file CSV". Xóa trước đảm bảo mỗi lần kiểm thử bắt
đầu từ trạng thái sạch, cho kết quả lặp lại được (reproducible).

CÁCH CHẠY:
    python scripts/stage_dataset.py data/synthetic/01_offpeak
    python scripts/stage_dataset.py data/raw            # khôi phục dữ liệu gốc
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


# Tên mạng và image khớp với cấu hình docker-compose của dự án.
PROJECT_NETWORK = "olist-lakehouse_lakehouse-internal"
MC_IMAGE = "olist-lakehouse-mc:RELEASE.2025-08-13T08-35-41Z"
RAW_BUCKET = "raw"
RAW_PREFIX = "olist"


def _mc_script(clear: bool) -> str:
    """Sinh đoạn shell chạy bên trong container ``mc``.

    Đầu vào : cờ ``clear`` cho biết có xóa sạch tiền tố đích trước khi chép hay không.
    Xử lý   : đăng ký alias tới MinIO rồi thực hiện xóa (tùy chọn) và chép đệ quy.
    Đầu ra  : chuỗi lệnh shell hoàn chỉnh.
    """
    remove = (
        f'mc rm --recursive --force "local/{RAW_BUCKET}/{RAW_PREFIX}/" >/dev/null 2>&1 || true\n'
        if clear
        else ""
    )
    return (
        'set -eu\n'
        'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null\n'
        f'{remove}'
        f'mc cp --recursive /stage/ "local/{RAW_BUCKET}/{RAW_PREFIX}/" >/dev/null\n'
        'echo "--- Noi dung vung raw sau khi nap ---"\n'
        f'mc ls "local/{RAW_BUCKET}/{RAW_PREFIX}/"\n'
    )


def stage(source: Path, *, clear: bool = True) -> int:
    """Đẩy toàn bộ file CSV trong thư mục ``source`` lên ``s3://raw/olist/``."""
    source = source.resolve()
    csv_files = sorted(source.glob("*.csv"))
    if not csv_files:
        print(f"LỖI: không tìm thấy file CSV nào trong {source}", file=sys.stderr)
        return 1

    total_mb = sum(path.stat().st_size for path in csv_files) / 1024 / 1024
    print(f"Đang nạp {len(csv_files)} file CSV ({total_mb:.1f} MB) từ {source}")
    print(f"  tới  s3://{RAW_BUCKET}/{RAW_PREFIX}/  (xóa sạch trước = {clear})")

    command = [
        "docker", "run", "--rm",
        "--network", PROJECT_NETWORK,
        "-e", f"MINIO_ROOT_USER={os.environ.get('MINIO_ROOT_USER', 'root')}",
        "-e", f"MINIO_ROOT_PASSWORD={os.environ.get('MINIO_ROOT_PASSWORD', 'admin_root')}",
        "-v", f"{source}:/stage:ro",
        "--entrypoint", "/bin/sh",
        MC_IMAGE, "-c", _mc_script(clear),
    ]
    result = subprocess.run(command, text=True, capture_output=True)
    print(result.stdout.strip())
    if result.returncode != 0:
        print(result.stderr.strip(), file=sys.stderr)
    return result.returncode


def _load_env(path: Path) -> None:
    """Nạp các biến môi trường từ file ``.env`` nếu chúng chưa được đặt sẵn trong shell."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def main(argv: list[str] | None = None) -> int:
    """Điểm khởi nhập CLI."""
    parser = argparse.ArgumentParser(
        description="Đẩy một bộ dữ liệu CSV lên vùng raw của MinIO cho pipeline nạp vào Bronze."
    )
    parser.add_argument("source", type=Path, help="Thư mục chứa 9 file CSV cần nạp.")
    parser.add_argument(
        "--keep-existing",
        action="store_true",
        help="Giữ lại các đối tượng cũ thay vì xóa sạch tiền tố đích trước khi chép.",
    )
    args = parser.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    _load_env(Path(".env"))
    return stage(args.source, clear=not args.keep_existing)


if __name__ == "__main__":
    raise SystemExit(main())
