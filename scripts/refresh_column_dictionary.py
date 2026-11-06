"""Làm mới `schema_snapshot.json` và `column_dictionary.md` từ hệ thống đang chạy thật.

VẤN ĐỀ CẦN GIẢI QUYẾT
---------------------
Từ điển cột là tài liệu tham chiếu mà người mới vào dự án tin tưởng tuyệt đối. Nhưng nó được
kết xuất từ một ảnh chụp lược đồ (`schema_snapshot.json`) chụp từ tháng 8/2026 — trước đợt
rebuild. Đợt rebuild đã gỡ bỏ cột `_row_hash` khỏi toàn bộ 9 bảng Bronze (xem ghi chú trong
`transformation/common/registry.py`), nhưng ảnh chụp thì không được cập nhật theo. Hậu quả:
tài liệu mô tả một cột **không hề tồn tại** trong hệ thống. Một tài liệu sai còn tệ hơn
không có tài liệu, vì nó khiến người đọc tin vào điều sai.

CÁCH LÀM
--------
    Spark (trong container Airflow) ──┐
                                      ├──► hợp nhất ──► schema_snapshot.json ──► column_dictionary.md
    Trino information_schema ─────────┘

  * **Spark** là nguồn chân lý cho kiểu dữ liệu gốc của bảng Iceberg và cho số dòng thực tế.
  * **Trino** là nguồn chân lý cho kiểu dữ liệu mà tầng BI (Superset) thực sự nhìn thấy.
  * Các trường mô tả thiết kế (grain, khóa, phụ thuộc, partition, quy tắc chất lượng) được
    **giữ nguyên** từ ảnh chụp cũ, vì chúng mô tả ý đồ thiết kế chứ không phải trạng thái runtime
    — ngoại trừ danh sách cột trong quy tắc `schema_matches_registry`, vốn phải khớp lược đồ thật.

VÌ SAO ĐỐI CHIẾU CHÉO HAI ENGINE THAY VÌ CHỈ HỎI MỘT? Vì đúng cái sai lệch giữa hai engine mới
là thứ gây lỗi khó hiểu cho đội BI (ví dụ Spark `timestamp` hiện ra ở Trino là
`timestamp(6) with time zone`). Ghi cả hai cột cạnh nhau giúp nhìn ra ngay khác biệt đó.

ĐẦU VÀO : cụm container đang chạy + `docs/data_model/schema_snapshot.json` hiện có.
ĐẦU RA  : ghi đè `schema_snapshot.json` và `column_dictionary.md`.

CÁCH CHẠY (cụm phải đang chạy và KHÔNG có job Spark nặng nào khác):
    python scripts/refresh_column_dictionary.py
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import subprocess
import sys


SCHEDULER = "olist-lakehouse-airflow-scheduler-1"
TRINO = "olist-lakehouse-trino-1"
VENV_PYTHON = "/opt/lakehouse/venv/bin/python"

# Đoạn mã PySpark chạy BÊN TRONG container Airflow. Nó dùng lại đúng hàm dựng SparkSession
# của pipeline (`build_spark_session`), nhờ vậy kiểu dữ liệu đọc ra chính là kiểu mà các job
# thật nhìn thấy, chứ không phải kiểu của một phiên Spark cấu hình khác.
SPARK_PROBE = r'''
import json
import os

# Container Airflow chỉ đặt AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, trong khi `Settings.from_env`
# đòi MINIO_ROOT_USER / MINIO_ROOT_PASSWORD. Đây đúng là cách mà `_runtime_settings` trong các job
# Spark thật vẫn làm — ánh xạ lại hai biến này trước khi nạp cấu hình.
os.environ.setdefault("MINIO_ROOT_USER", os.environ.get("AWS_ACCESS_KEY_ID", ""))
os.environ.setdefault("MINIO_ROOT_PASSWORD", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))

from transformation.common.config import Settings
from transformation.common.spark_session import build_spark_session

TABLES = __TABLES__

settings = Settings.from_env()
spark = build_spark_session(settings, "schema-snapshot-refresh")
spark.sparkContext.setLogLevel("ERROR")
result = {}
for identity in TABLES:
    namespace, table = identity.split(".")
    qualified = "`lakehouse`.`%s`.`%s`" % (namespace, table)
    try:
        schema = spark.table(qualified).schema
        count = spark.sql("SELECT count(*) AS n FROM " + qualified).first()["n"]
        partitions = [
            row["name"] if "name" in row.__fields__ else str(row[0])
            for row in spark.sql("SELECT * FROM " + qualified + ".partitions").limit(0).collect()
        ]
        result[identity] = {
            "columns": [
                {"name": f.name, "spark_type": f.dataType.simpleString(), "spark_nullable": f.nullable}
                for f in schema.fields
            ],
            "row_count": int(count),
        }
    except Exception as error:
        result[identity] = {"error": "%s: %s" % (type(error).__name__, error)}
spark.stop()
print("<<<SNAPSHOT>>>" + json.dumps(result) + "<<<END>>>")
'''


def probe_spark(tables: list[str]) -> dict[str, dict]:
    """Chạy đoạn mã PySpark trong container Airflow và lấy về lược đồ + số dòng thật."""
    script = SPARK_PROBE.replace("__TABLES__", json.dumps(tables))
    print(f"Đang hỏi Spark về {len(tables)} bảng (mất khoảng 1-2 phút) ...")
    # Bắt buộc chỉ định encoding="utf-8": mặc định `subprocess` dùng bảng mã của hệ thống
    # (trên máy Windows này là cp932), nên mọi ký tự tiếng Việt trong đoạn mã truyền qua stdin
    # đều gây UnicodeEncodeError trước khi kịp tới container.
    result = subprocess.run(
        ["docker", "exec", "-i", SCHEDULER, VENV_PYTHON, "-"],
        input=script, text=True, capture_output=True, timeout=1_800,
        encoding="utf-8", errors="replace",
    )
    marker_start = result.stdout.find("<<<SNAPSHOT>>>")
    marker_end = result.stdout.find("<<<END>>>")
    if marker_start < 0 or marker_end < 0:
        raise RuntimeError(
            "Không lấy được lược đồ từ Spark.\n"
            f"stdout (1000 ký tự cuối): {result.stdout[-1000:]}\n"
            f"stderr (1000 ký tự cuối): {result.stderr[-1000:]}"
        )
    return json.loads(result.stdout[marker_start + len("<<<SNAPSHOT>>>") : marker_end])


def probe_trino() -> dict[str, dict[str, dict[str, str]]]:
    """Đọc toàn bộ kiểu dữ liệu mà Trino nhìn thấy từ `information_schema.columns`."""
    print("Đang hỏi Trino về kiểu dữ liệu tương ứng ...")
    sql = (
        "SELECT table_schema, table_name, column_name, data_type, is_nullable "
        "FROM lakehouse.information_schema.columns "
        "WHERE table_schema IN ('bronze','silver','gold','quarantine','control','quality') "
        "ORDER BY table_schema, table_name, ordinal_position"
    )
    output = subprocess.run(
        ["docker", "exec", TRINO, "trino", "--execute", sql, "--output-format", "CSV_HEADER"],
        text=True, capture_output=True, timeout=900, encoding="utf-8", errors="replace",
    ).stdout
    types: dict[str, dict[str, dict[str, str]]] = {}
    for row in csv.DictReader(io.StringIO(output)):
        identity = f"{row['table_schema'].strip(chr(34))}.{row['table_name'].strip(chr(34))}"
        types.setdefault(identity, {})[row["column_name"].strip('"')] = {
            "trino_type": row["data_type"].strip('"'),
            "trino_nullable": row["is_nullable"].strip('"').upper() == "YES",
        }
    return types


# Khóa thật của những bảng mà ảnh chụp cũ khai báo sai, đối chiếu từ mã nguồn:
#   - bronze.geolocation : `transformation/common/registry.py` -> TABLE_SPECS["geolocation"].key
#   - quarantine.*       : `transformation/silver/job.py` dòng 362 -> ("_batch_id", *spec.key)
KEY_OVERRIDES: dict[str, tuple[str, ...]] = {
    "bronze.geolocation": (
        "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
        "geolocation_city", "geolocation_state",
    ),
    "quarantine.orders": ("_batch_id", "order_id"),
    "quarantine.geography": ("_batch_id", "zip_code_prefix"),
    "quarantine.order_items": ("_batch_id", "order_id", "order_item_id"),
}


def render_markdown(snapshot: dict) -> str:
    """Kết xuất từ điển cột dạng Markdown từ ảnh chụp lược đồ."""
    lines = [
        "# Từ điển cột (Column Dictionary)",
        "",
        "> Tệp này được **sinh tự động** bởi `scripts/refresh_column_dictionary.py` từ hệ thống đang",
        "> chạy thật: kiểu dữ liệu và số dòng lấy trực tiếp từ Spark, đối chiếu chéo với",
        "> `information_schema` của Trino. **Đừng sửa tay tệp này** — hãy sửa mã nguồn rồi chạy lại",
        "> công cụ kết xuất.",
        "",
        f"- Thời điểm kết xuất: `{snapshot['generated_at_utc']}`",
        f"- Số bảng: `{len(snapshot['tables'])}`",
        f"- Môi trường: `{json.dumps(snapshot['environment'], ensure_ascii=False)}`",
        "",
        "---",
        "",
    ]
    for table in snapshot["tables"]:
        identity = f"{table['schema']}.{table['name']}"
        partition = ", ".join(table["partition"]) if table["partition"] else "không phân vùng"
        lines.extend([
            f"### {identity}",
            "",
            f"- Mức hạt (grain): `{table['grain']}`",
            f"- Khóa: `{', '.join(table['keys']) or 'không khai báo'}`",
            f"- Phụ thuộc: `{', '.join(table['dependencies']) or 'nguồn gốc'}`",
            f"- Partition: `{partition}`",
            f"- Số dòng thực tế: `{table['row_count']:,}`",
            f"- Quy tắc chất lượng: `{json.dumps(table['quality'], sort_keys=True, separators=(',', ':'))}`",
            "",
            "| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |",
            "|---|---|---|---|---|",
        ])
        for column in table["columns"]:
            lines.append(
                f"| `{column['name']}` | `{column['spark_type']}` | "
                f"`{str(column['spark_nullable']).lower()}` | `{column.get('trino_type', '—')}` | "
                f"`{str(column.get('trino_nullable', '—')).lower()}` |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    """Điểm khởi nhập CLI: hỏi hai engine, hợp nhất, ghi lại ảnh chụp và từ điển."""
    parser = argparse.ArgumentParser(description="Làm mới từ điển cột từ hệ thống đang chạy.")
    parser.add_argument("--snapshot", type=Path, default=Path("docs/data_model/schema_snapshot.json"))
    parser.add_argument("--dictionary", type=Path, default=Path("docs/data_model/column_dictionary.md"))
    args = parser.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
    identities = [f"{t['schema']}.{t['name']}" for t in snapshot["tables"]]

    spark_data = probe_spark(identities)
    trino_data = probe_trino()

    kept, dropped, changes = [], [], []
    for table in snapshot["tables"]:
        identity = f"{table['schema']}.{table['name']}"
        live = spark_data.get(identity, {})
        if "error" in live or not live.get("columns"):
            dropped.append(identity)
            continue

        old_names = [column["name"] for column in table["columns"]]
        new_names = [column["name"] for column in live["columns"]]
        if old_names != new_names:
            removed = [name for name in old_names if name not in new_names]
            added = [name for name in new_names if name not in old_names]
            changes.append(f"{identity}: bỏ {removed or '—'} / thêm {added or '—'}")

        trino_columns = trino_data.get(identity, {})
        table["columns"] = [
            {**column, **trino_columns.get(column["name"], {"trino_type": "—", "trino_nullable": "—"})}
            for column in live["columns"]
        ]
        if table["row_count"] != live["row_count"]:
            changes.append(
                f"{identity}: số dòng {table['row_count']:,} -> {live['row_count']:,}"
            )
        table["row_count"] = live["row_count"]

        # Đồng bộ danh sách cột trong quy tắc kiểm định lược đồ với lược đồ thật.
        for rule in table.get("quality", []):
            if rule.get("name") == "schema_matches_registry":
                rule["columns"] = new_names

        # Phát hiện và sửa các khai báo trỏ vào cột KHÔNG CÒN TỒN TẠI.
        # Vì sao cần bước này? Ảnh chụp cũ khai báo khóa của `bronze.geolocation` là `_row_hash` —
        # một cột đã bị gỡ bỏ. Một tài liệu chỉ vào cột không tồn tại thì tệ hơn không có tài liệu,
        # nên ta thay bằng khóa thật lấy từ registry và báo rõ ra màn hình thay vì sửa lén.
        live = set(new_names)
        stale_keys = [name for name in table.get("keys", []) if name not in live]
        if stale_keys:
            replacement = KEY_OVERRIDES.get(identity)
            if replacement is not None:
                changes.append(
                    f"{identity}: khóa {table['keys']} trỏ vào cột không tồn tại {stale_keys} "
                    f"-> thay bằng khóa thật {list(replacement)}"
                )
                table["keys"] = list(replacement)
            else:
                changes.append(
                    f"{identity}: CẢNH BÁO khóa {table['keys']} trỏ vào cột không tồn tại {stale_keys}"
                )
        for rule in table.get("quality", []):
            bad = [name for name in rule.get("columns", []) if name not in live]
            if bad and rule.get("name") != "schema_matches_registry":
                rule["columns"] = [name for name in rule["columns"] if name in live]
                if isinstance(rule.get("config"), dict) and "column_list" in rule["config"]:
                    rule["config"]["column_list"] = rule["columns"]
                changes.append(f"{identity}: luật '{rule['name']}' bỏ cột không tồn tại {bad}")

        kept.append(table)

    snapshot["tables"] = kept
    snapshot["expected_table_count"] = len(kept)
    snapshot["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    snapshot["source"] = (
        "Lược đồ Spark StructType và số dòng đọc trực tiếp từ hệ thống đang chạy, "
        "đối chiếu chéo với Trino information_schema"
    )

    args.snapshot.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    args.dictionary.write_text(render_markdown(snapshot), encoding="utf-8")

    print(f"\nĐã kết xuất {len(kept)} bảng vào {args.dictionary}")
    if dropped:
        print(f"  Bỏ khỏi tài liệu (không còn tồn tại): {', '.join(dropped)}")
    if changes:
        print("  Khác biệt so với ảnh chụp cũ:")
        for item in changes:
            print(f"    - {item}")
    if not changes and not dropped:
        print("  Không có khác biệt nào — tài liệu cũ vốn đã chính xác.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
