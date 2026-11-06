"""Tổng hợp toàn bộ file JSON benchmark thành các bảng Markdown để dán vào báo cáo nghiệm thu.

ĐẦU VÀO : các file ``docs/reports/benchmarks/*.json`` do ``run_benchmark.py`` sinh ra.
XỬ LÝ   : đọc, sắp xếp theo tên bài rồi dựng 4 bảng Markdown trình bày theo góc nhìn khác nhau.
ĐẦU RA  : in ra màn hình, đồng thời ghi vào ``docs/reports/benchmarks/TONG_HOP.md``.

VÌ SAO TÁCH RIÊNG KHỎI ``run_benchmark.py``? Vì hai việc này có nhịp độ khác hẳn nhau: đo đạc
là việc chạy một lần, tốn nhiều phút và có thể thất bại giữa chừng; còn tổng hợp là việc đọc
lại kết quả đã có, chạy trong một giây và sẽ phải lặp đi lặp lại nhiều lần trong lúc soạn báo
cáo. Tách ra giúp sửa cách trình bày bảng mà không phải chạy lại toàn bộ chuỗi benchmark.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


def _mb(value: int) -> str:
    """Quy đổi byte sang MB, có dấu cộng/trừ để thấy rõ chiều thay đổi."""
    return f"{value / 1024 / 1024:+,.1f}"


def build_tables(records: list[dict]) -> str:
    """Dựng toàn bộ phần Markdown tổng hợp kết quả benchmark."""
    lines: list[str] = []

    # ---- Bảng 1: Tổng quan từng bài đo -------------------------------------------------
    lines += [
        "### Bảng 1 — Tổng quan các bài đo",
        "",
        "| Bài đo | Chế độ | Kết quả | Tổng thời gian (giây) | Dòng ghi thêm | Thông lượng (dòng/giây) | Task chậm nhất |",
        "|:---|:---:|:---:|---:|---:|---:|:---|",
    ]
    for item in records:
        lines.append(
            f"| `{item['label']}` | {item['mode']} | **{item['dag_result']}** | "
            f"{item['wall_clock_seconds']:,.1f} | {item['rows_written_total']:,} | "
            f"{item['throughput_rows_per_second']:,.1f} | `{item['slowest_task'] or '—'}` |"
        )

    # ---- Bảng 2: Độ trễ từng chặng của pipeline ----------------------------------------
    # Gộp 8 task Silver chạy song song thành một dòng duy nhất: điều có ý nghĩa vận hành là
    # thời gian của chặng Silver (bị quyết định bởi task chậm nhất), không phải từng task lẻ.
    stages = {
        "Nạp Bronze": ("ingest_bronze_layer",),
        "Silver (8 task song song)": tuple(
            f"silver_{name}" for name in (
                "customers", "sellers", "products", "geography",
                "orders", "order_items", "order_payments", "order_reviews",
            )
        ),
        "Dựng Gold": ("build_gold_closure",),
        "Kiểm định chất lượng": ("run_quality_checks",),
    }
    lines += [
        "",
        "### Bảng 2 — Độ trễ từng chặng (giây)",
        "",
        "> Chặng Silver gồm 8 task chạy song song; con số ghi nhận là **task lâu nhất**, vì đó mới",
        "> là thời gian thực tế mà chặng này chiếm trên đường găng của pipeline.",
        "",
        "| Bài đo | " + " | ".join(stages) + " |",
        "|:---|" + "---:|" * len(stages),
    ]
    for item in records:
        durations = item["task_durations_seconds"]
        cells = []
        for names in stages.values():
            values = [durations.get(name) for name in names]
            values = [value for value in values if value is not None]
            cells.append(f"{max(values):,.1f}" if values else "—")
        lines.append(f"| `{item['label']}` | " + " | ".join(cells) + " |")

    # ---- Bảng 3: Tài nguyên đỉnh --------------------------------------------------------
    lines += [
        "",
        "### Bảng 3 — Tài nguyên đỉnh của Spark Worker và Trino",
        "",
        "| Bài đo | Spark Worker CPU đỉnh | Spark Worker RAM đỉnh | Trino RAM đỉnh | Số mẫu đo |",
        "|:---|---:|---:|---:|---:|",
    ]
    for item in records:
        worker = item["resources"].get("olist-lakehouse-spark-worker-1", {})
        trino = item["resources"].get("olist-lakehouse-trino-1", {})
        lines.append(
            f"| `{item['label']}` | {worker.get('cpu_dinh_phan_tram', 0):,.1f}% | "
            f"{worker.get('ram_dinh_mib', 0):,.0f} MiB | {trino.get('ram_dinh_mib', 0):,.0f} MiB | "
            f"{worker.get('so_mau', 0)} |"
        )

    # ---- Bảng 4: Tác động lên dữ liệu và số tệp Parquet --------------------------------
    lines += [
        "",
        "### Bảng 4 — Tác động lên dữ liệu và số tệp Parquet",
        "",
        "| Bài đo | silver.orders (dòng) | quarantine.orders (dòng) | gold.fact_orders (dòng) | Tổng tệp Parquet sau | Thay đổi dung lượng |",
        "|:---|---:|---:|---:|---:|---:|",
    ]
    for item in records:
        after, delta = item["tables_after"], item["tables_delta"]
        total_files = sum(table["files"] for table in after.values())
        total_bytes = sum(table["bytes"] for table in delta.values())
        lines.append(
            f"| `{item['label']}` | {after['silver.orders']['rows']:,} | "
            f"{after['quarantine.orders']['rows']:,} | {after['gold.fact_orders']['rows']:,} | "
            f"{total_files} | {_mb(total_bytes)} MB |"
        )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    """Điểm khởi nhập CLI."""
    parser = argparse.ArgumentParser(description="Tổng hợp kết quả benchmark thành bảng Markdown.")
    parser.add_argument("--input-dir", type=Path, default=Path("docs/reports/benchmarks"))
    args = parser.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    files = sorted(args.input_dir.glob("*.json"))
    if not files:
        print(f"Không tìm thấy file benchmark nào trong {args.input_dir}", file=sys.stderr)
        return 1
    records = [json.loads(path.read_text(encoding="utf-8")) for path in files]
    markdown = build_tables(records)

    destination = args.input_dir / "TONG_HOP.md"
    destination.write_text(
        "# Tổng hợp số liệu Benchmark (sinh tự động)\n\n"
        "> Tệp này do `scripts/summarize_benchmarks.py` kết xuất từ các file JSON đo được.\n\n"
        + markdown,
        encoding="utf-8",
    )
    print(markdown)
    print(f"Đã ghi: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
