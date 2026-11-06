"""Chạy một bài benchmark hoàn chỉnh trên cụm Lakehouse và ghi lại số liệu đo được (Giai đoạn 2).

MỘT BÀI BENCHMARK GỒM NHỮNG BƯỚC NÀO?
-------------------------------------
    1. Đo trạng thái TRƯỚC   : số dòng và số tệp Parquet của các bảng trọng yếu (qua Trino).
    2. Nạp bộ dữ liệu        : đẩy CSV lên s3://raw/olist/ (gọi lại scripts/stage_dataset.py).
    3. Bật bộ lấy mẫu tài nguyên: nền chạy `docker stats` định kỳ để bắt đỉnh CPU/RAM.
    4. Kích hoạt DAG & theo dõi : ghi mốc bắt đầu/kết thúc của TỪNG task trong DAG.
    5. Đo trạng thái SAU     : lặp lại bước 1 để tính phần chênh lệch.
    6. Xuất kết quả          : in bảng ra màn hình và lưu JSON vào docs/reports/benchmarks/.

VÌ SAO TỰ VIẾT THAY VÌ DÙNG CÔNG CỤ BENCHMARK CÓ SẴN?
Các công cụ đo tải tổng quát (JMeter, Locust...) được thiết kế cho API đồng bộ, không hiểu
khái niệm DAG nhiều chặng của Airflow, cũng không đọc được số dòng/số tệp bên trong bảng
Iceberg. Thứ cần đo ở đây rất đặc thù: *thông lượng theo từng tầng Bronze/Silver/Gold*,
*độ trễ của từng task*, *hiệu quả nén tệp*. Script này chỉ ghép nối các lệnh CLI sẵn có
(docker, airflow, trino) — không tự chế thêm bất kỳ framework nào.

ĐẦU VÀO : thư mục bộ dữ liệu + nhãn bài đo + chế độ nạp (full/incremental).
ĐẦU RA  : một file JSON trong ``docs/reports/benchmarks/<nhãn>.json`` và bảng tóm tắt in ra màn hình.

CÁCH CHẠY:
    python scripts/run_benchmark.py --dataset data/synthetic/01_offpeak --label 01_offpeak --mode incremental
    python scripts/run_benchmark.py --dataset data/raw --label baseline_restore --mode full
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import threading
import time


SCHEDULER = "olist-lakehouse-airflow-scheduler-1"
TRINO = "olist-lakehouse-trino-1"

# Các container được theo dõi tài nguyên. Spark Worker là nhân vật chính (nơi thực sự xử lý
# dữ liệu); Trino và Airflow Scheduler được theo dõi kèm để thấy ảnh hưởng lan tỏa.
WATCHED_CONTAINERS = (
    "olist-lakehouse-spark-worker-1",
    "olist-lakehouse-spark-master-1",
    "olist-lakehouse-airflow-scheduler-1",
    "olist-lakehouse-trino-1",
)

# Các bảng được chụp số dòng / số tệp Parquet trước và sau mỗi bài đo.
TRACKED_TABLES = (
    ("bronze", "orders"),
    ("bronze", "order_items"),
    ("silver", "orders"),
    ("silver", "order_items"),
    ("quarantine", "orders"),
    ("quarantine", "order_items"),
    ("gold", "fact_orders"),
    ("gold", "fact_order_items"),
)


def _run(command: list[str], timeout: int = 900) -> str:
    """Chạy một lệnh ngoài và trả về stdout dạng chuỗi (bỏ qua stderr ồn ào của Airflow CLI)."""
    result = subprocess.run(command, text=True, capture_output=True, timeout=timeout)
    return result.stdout


def trino_query(sql: str) -> list[dict[str, str]]:
    """Thực thi một câu SQL trên Trino và trả về kết quả dạng danh sách dict.

    Đầu vào : câu SQL một dòng.
    Xử lý   : gọi Trino CLI trong container với định dạng xuất CSV_HEADER để dễ phân tích.
    Đầu ra  : list các dict {tên_cột: giá_trị_chuỗi}.
    """
    output = _run(
        ["docker", "exec", TRINO, "trino", "--execute", sql, "--output-format", "CSV_HEADER"]
    )
    rows = list(csv.DictReader(io.StringIO(output)))
    return [{key: (value or "").strip('"') for key, value in row.items()} for row in rows]


def snapshot_tables() -> dict[str, dict[str, int]]:
    """Chụp số dòng, số tệp Parquet và tổng dung lượng của các bảng theo dõi.

    Vì sao đo cả SỐ TỆP chứ không chỉ số dòng? Vì bài toán "small-file problem" là điểm
    nghẽn kinh điển của Lakehouse: nạp tăng dần nhiều lô nhỏ sẽ sinh ra hàng trăm tệp
    Parquet tí hon, khiến Trino phải mở quá nhiều tệp khi truy vấn. Số tệp trước/sau chính
    là thước đo trực tiếp hiệu quả của job nén (rewrite_data_files).
    """
    snapshot: dict[str, dict[str, int]] = {}
    for namespace, table in TRACKED_TABLES:
        name = f"{namespace}.{table}"
        try:
            rows = trino_query(f"SELECT count(*) AS n FROM lakehouse.{name}")
            files = trino_query(
                f'SELECT count(*) AS f, coalesce(sum(file_size_in_bytes), 0) AS b '
                f'FROM lakehouse.{namespace}."{table}$files"'
            )
            snapshot[name] = {
                "rows": int(rows[0]["n"]) if rows else 0,
                "files": int(files[0]["f"]) if files else 0,
                "bytes": int(files[0]["b"]) if files else 0,
            }
        except Exception:  # noqa: BLE001 - bảng có thể chưa tồn tại ở lần chạy đầu tiên
            snapshot[name] = {"rows": 0, "files": 0, "bytes": 0}
    return snapshot


class ResourceSampler(threading.Thread):
    """Luồng nền lấy mẫu CPU/RAM của các container theo chu kỳ để tìm giá trị ĐỈNH.

    Vì sao phải lấy mẫu liên tục thay vì đo một lần lúc kết thúc? Vì đỉnh tài nguyên xuất
    hiện đúng vào lúc Spark đang shuffle giữa chừng; đo sau khi job xong chỉ thấy trạng thái
    nhàn rỗi. Giá trị đỉnh mới là thứ trả lời được câu hỏi "Worker có sắp tràn RAM không?".
    """

    def __init__(self, interval_seconds: float = 3.0) -> None:
        super().__init__(daemon=True)
        self.interval = interval_seconds
        self.samples: dict[str, list[tuple[float, float]]] = {
            name: [] for name in WATCHED_CONTAINERS
        }
        # Lưu ý: KHÔNG đặt tên thuộc tính này là `_stop`, vì `threading.Thread` đã có sẵn
        # phương thức nội bộ `_stop()` được `join()` gọi tới; đặt trùng tên sẽ che mất
        # phương thức đó và làm `join()` ném TypeError: 'Event' object is not callable.
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                output = _run(
                    [
                        "docker", "stats", "--no-stream",
                        "--format", "{{.Name}};{{.CPUPerc}};{{.MemUsage}}",
                        *WATCHED_CONTAINERS,
                    ],
                    timeout=30,
                )
                for line in output.strip().splitlines():
                    parts = line.split(";")
                    if len(parts) != 3 or parts[0] not in self.samples:
                        continue
                    cpu = float(parts[1].rstrip("%") or 0)
                    # Chuỗi dạng "1.234GiB / 4GiB" -> lấy phần dung lượng đang dùng, quy ra MiB.
                    match = re.match(r"\s*([\d.]+)\s*([KMG]i?B)", parts[2])
                    memory = 0.0
                    if match:
                        value, unit = float(match.group(1)), match.group(2)
                        memory = value * {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "B": 1 / 1048576}.get(unit, 1)
                    self.samples[parts[0]].append((cpu, memory))
            except Exception:  # noqa: BLE001 - mất một mẫu không được phép làm hỏng bài đo
                pass
            self._stop_event.wait(self.interval)

    def stop(self) -> dict[str, dict[str, float]]:
        """Dừng lấy mẫu và trả về thống kê đỉnh / trung bình cho từng container."""
        self._stop_event.set()
        self.join(timeout=15)
        result: dict[str, dict[str, float]] = {}
        for name, values in self.samples.items():
            if not values:
                continue
            cpus = [item[0] for item in values]
            memories = [item[1] for item in values]
            result[name] = {
                "so_mau": len(values),
                "cpu_dinh_phan_tram": round(max(cpus), 1),
                "cpu_trung_binh_phan_tram": round(sum(cpus) / len(cpus), 1),
                "ram_dinh_mib": round(max(memories), 1),
                "ram_trung_binh_mib": round(sum(memories) / len(memories), 1),
            }
        return result


def task_states(dag_id: str, run_id: str) -> dict[str, dict[str, str]]:
    """Đọc trạng thái và mốc thời gian của mọi task trong một lần chạy DAG.

    Đầu vào : mã DAG và mã lần chạy.
    Xử lý   : gọi ``airflow tasks states-for-dag-run`` rồi tách bảng kết quả theo dấu '|'.
    Đầu ra  : dict {task_id: {state, start_date, end_date}}.
    """
    output = _run(
        ["docker", "exec", SCHEDULER, "airflow", "tasks", "states-for-dag-run", dag_id, run_id],
        timeout=180,
    )
    states: dict[str, dict[str, str]] = {}
    for line in output.splitlines():
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 6 or parts[0] != dag_id:
            continue
        states[parts[2]] = {"state": parts[3], "start": parts[4], "end": parts[5]}
    return states


def _duration_seconds(start: str, end: str) -> float | None:
    """Tính số giây giữa hai mốc thời gian ISO của Airflow; trả về None nếu thiếu dữ liệu."""
    if not start or not end:
        return None
    try:
        return round(
            (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds(), 1
        )
    except ValueError:
        return None


def wait_for_dag(dag_id: str, run_id: str, *, timeout_seconds: int = 5_400) -> dict:
    """Chờ DAG chạy xong và trả về trạng thái cuối cùng của toàn bộ task.

    Điều kiện dừng: mọi task đều đã ở trạng thái kết thúc (success / failed / upstream_failed /
    skipped). Nếu vượt quá ``timeout_seconds`` thì dừng chờ và báo hết giờ — để một bài đo bị
    treo không làm kẹt vô hạn cả chuỗi benchmark.
    """
    terminal = {"success", "failed", "upstream_failed", "skipped"}
    deadline = time.monotonic() + timeout_seconds
    states: dict[str, dict[str, str]] = {}
    while time.monotonic() < deadline:
        states = task_states(dag_id, run_id)
        if states and all(item["state"] in terminal for item in states.values()):
            return states
        time.sleep(10)
    return states


def run_benchmark(
    *,
    dataset: Path,
    label: str,
    mode: str,
    dag_id: str,
    output_dir: Path,
) -> dict:
    """Thực hiện trọn vẹn một bài benchmark và trả về bản ghi kết quả."""
    batch_id = f"bm-{label.replace('_', '-')}"
    run_id = f"bm-{label.replace('_', '-')}-{datetime.now().strftime('%H%M%S')}"

    print(f"\n{'=' * 80}\n  BENCHMARK: {label}   (dataset={dataset}, mode={mode})\n{'=' * 80}")

    print("[1/6] Đo trạng thái TRƯỚC khi nạp ...")
    before = snapshot_tables()

    print("[2/6] Nạp bộ dữ liệu lên vùng raw của MinIO ...")
    staged = subprocess.run(
        [sys.executable, "scripts/stage_dataset.py", str(dataset)],
        text=True, capture_output=True, timeout=1_800,
    )
    if staged.returncode != 0:
        raise RuntimeError(f"Nạp dữ liệu thất bại: {staged.stderr[:500]}")

    print("[3/6] Bật bộ lấy mẫu tài nguyên CPU/RAM ...")
    sampler = ResourceSampler()
    sampler.start()

    print(f"[4/6] Kích hoạt DAG {dag_id} (run_id={run_id}) và theo dõi tới khi kết thúc ...")
    started_at = time.monotonic()
    _run([
        "docker", "exec", SCHEDULER, "airflow", "dags", "trigger", dag_id,
        "--run-id", run_id,
        "--conf", json.dumps(
            {"run_id": run_id, "batch_id": batch_id, "mode": mode}, separators=(",", ":")
        ),
    ], timeout=180)
    states = wait_for_dag(dag_id, run_id)
    wall_clock = round(time.monotonic() - started_at, 1)

    print("[5/6] Dừng lấy mẫu và đo trạng thái SAU ...")
    resources = sampler.stop()
    after = snapshot_tables()

    print("[6/6] Tổng hợp kết quả ...")
    task_durations = {
        name: _duration_seconds(item["start"], item["end"])
        for name, item in sorted(states.items())
    }
    delta = {
        name: {
            "rows": after[name]["rows"] - before[name]["rows"],
            "files": after[name]["files"] - before[name]["files"],
            "bytes": after[name]["bytes"] - before[name]["bytes"],
        }
        for name in before
    }
    rows_written = sum(max(item["rows"], 0) for item in delta.values())
    record = {
        "label": label,
        "dataset": str(dataset),
        "dag_id": dag_id,
        "run_id": run_id,
        "batch_id": batch_id,
        "mode": mode,
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "wall_clock_seconds": wall_clock,
        "dag_result": (
            "SUCCESS" if states and all(i["state"] == "success" for i in states.values())
            else "FAILED"
        ),
        "task_states": {name: item["state"] for name, item in sorted(states.items())},
        "task_durations_seconds": task_durations,
        "slowest_task": max(
            ((k, v) for k, v in task_durations.items() if v is not None),
            key=lambda item: item[1], default=(None, None),
        )[0],
        "tables_before": before,
        "tables_after": after,
        "tables_delta": delta,
        "rows_written_total": rows_written,
        "throughput_rows_per_second": (
            round(rows_written / wall_clock, 1) if wall_clock > 0 else 0
        ),
        "resources": resources,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / f"{label}.json").write_text(
        json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    _print_report(record)
    return record


def _print_report(record: dict) -> None:
    """In bảng tóm tắt kết quả một bài benchmark cho người vận hành đọc nhanh."""
    print(f"\n  Kết quả DAG      : {record['dag_result']}")
    print(f"  Tổng thời gian   : {record['wall_clock_seconds']} giây")
    print(f"  Số dòng ghi thêm : {record['rows_written_total']:,}")
    print(f"  Thông lượng      : {record['throughput_rows_per_second']:,} dòng/giây")
    print(f"  Task chậm nhất   : {record['slowest_task']}")

    print("\n  --- Thời gian từng task (giây) ---")
    for name, seconds in sorted(
        record["task_durations_seconds"].items(),
        key=lambda item: (item[1] is None, -(item[1] or 0)),
    ):
        state = record["task_states"].get(name, "?")
        print(f"    {name:<36} {str(seconds):>8}   [{state}]")

    print("\n  --- Thay đổi dữ liệu theo bảng ---")
    print(f"    {'bảng':<26}{'dòng +':>12}{'tệp +':>8}{'tổng dòng':>14}{'tổng tệp':>10}")
    for name, change in record["tables_delta"].items():
        after = record["tables_after"][name]
        print(
            f"    {name:<26}{change['rows']:>+12,}{change['files']:>+8}"
            f"{after['rows']:>14,}{after['files']:>10}"
        )

    print("\n  --- Tài nguyên đỉnh ---")
    for name, stats in record["resources"].items():
        short = name.replace("olist-lakehouse-", "").replace("-1", "")
        print(
            f"    {short:<22} CPU đỉnh {stats['cpu_dinh_phan_tram']:>6.1f}%  "
            f"RAM đỉnh {stats['ram_dinh_mib']:>8.1f} MiB  "
            f"(TB: {stats['cpu_trung_binh_phan_tram']:.1f}% / {stats['ram_trung_binh_mib']:.0f} MiB, "
            f"{stats['so_mau']} mẫu)"
        )
    print()


def main(argv: list[str] | None = None) -> int:
    """Điểm khởi nhập CLI."""
    parser = argparse.ArgumentParser(description="Chạy một bài benchmark trên cụm Olist Lakehouse.")
    parser.add_argument("--dataset", type=Path, required=True, help="Thư mục chứa 9 file CSV.")
    parser.add_argument("--label", required=True, help="Nhãn bài đo, dùng làm tên file kết quả.")
    parser.add_argument("--mode", choices=("full", "incremental"), default="incremental")
    parser.add_argument("--dag-id", default="olist_daily_pipeline")
    parser.add_argument("--output-dir", type=Path, default=Path("docs/reports/benchmarks"))
    args = parser.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    record = run_benchmark(
        dataset=args.dataset,
        label=args.label,
        mode=args.mode,
        dag_id=args.dag_id,
        output_dir=args.output_dir,
    )
    return 0 if record["dag_result"] == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
