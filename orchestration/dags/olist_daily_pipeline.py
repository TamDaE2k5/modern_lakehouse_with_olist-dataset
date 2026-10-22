"""DAG Nhạc Trưởng Tự Động Hóa Toàn Diện: Bronze -> Silver -> Gold (olist_daily_pipeline).

Kiến trúc và Luồng Thực Thi của Master Pipeline:
------------------------------------------------
1. Toàn bộ kiến trúc Medallion Lakehouse được tự động hóa từ đầu đến cuối (End-to-End):
   - Nạp dữ liệu thô (Bronze Ingestion).
   - Làm sạch, cách ly lỗi song song 8 bảng thực thể (Silver Transformation & Quarantine).
   - Mô hình hóa đa chiều Star Schema, Bridges và Data Marts (Gold Modeling).
2. Lập lịch định kỳ (Daily Schedule):
   - Chạy tự động mỗi ngày (hoặc kích hoạt theo yêu cầu qua nút Trigger trên Web UI).
3. Đảm bảo tính Bất biến và Toàn vẹn (Idempotency & Data Lineage):
   - Tự động truyền `run_id`, `batch_id`, `mode` xuyên suốt toàn bộ chuỗi nhiệm vụ.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator

try:
    from orchestration.dags.common import (
        build_job_arguments,
        dag_default_args,
        runtime_params,
        runtime_validation_task,
        spark_submit_task,
    )
except ModuleNotFoundError:
    from common import (  # type: ignore[no-redef]
        build_job_arguments,
        dag_default_args,
        runtime_params,
        runtime_validation_task,
        spark_submit_task,
    )


# Danh sách 8 bảng nghiệp vụ tầng Silver
SILVER_TABLES = (
    "customers",
    "sellers",
    "products",
    "geography",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
)


with DAG(
    dag_id="olist_daily_pipeline",
    description="Master Pipeline điều phối toàn diện: Nạp Bronze -> Làm sạch Silver song song -> Dựng Gold Star Schema",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",  # Lập lịch chạy tự động mỗi ngày
    catchup=False,
    max_active_runs=1,
    params=runtime_params(default_mode="full"),
    default_args=dag_default_args(),
    tags=["olist", "master", "bronze", "silver", "gold"],
) as dag:

    # 1. Kiểm tra tham số đầu vào
    validate_params = runtime_validation_task()

    # 2. Giai đoạn Bronze: Nạp 9 file CSV vào Iceberg lakehouse.bronze.*
    ingest_bronze = spark_submit_task(
        task_id="ingest_bronze_layer",
        application="/opt/lakehouse/transformation/bronze/job.py",
        application_args=build_job_arguments(run_suffix="bronze", include_mode=True),
        write_pool_slots=1,
    )

    bronze_complete = EmptyOperator(task_id="bronze_ingestion_complete")

    # 3. Giai đoạn Silver: 8 task làm sạch song song
    silver_tasks = []
    for table_name in SILVER_TABLES:
        task = spark_submit_task(
            task_id=f"silver_{table_name}",
            application="/opt/lakehouse/transformation/silver/job.py",
            application_args=(
                build_job_arguments(
                    run_suffix=f"silver-{table_name.replace('_', '-')}",
                    include_mode=True,
                )
                + ["--table", table_name]
            ),
            write_pool_slots=1,
        )
        bronze_complete >> task
        silver_tasks.append(task)

    silver_complete = EmptyOperator(task_id="silver_transformation_complete")
    for task in silver_tasks:
        task >> silver_complete

    # 4. Giai đoạn Gold: Xây dựng 14 bảng Gold khép kín
    build_gold = spark_submit_task(
        task_id="build_gold_closure",
        application="/opt/lakehouse/transformation/gold/job.py",
        application_args=build_job_arguments(run_suffix="gold", include_mode=True),
        write_pool_slots=1,
    )

    # 5. Giai đoạn Quality: Kiểm định chất lượng toàn diện Bronze -> Silver -> Gold (198 checks)
    run_quality = spark_submit_task(
        task_id="run_quality_checks",
        application="/opt/lakehouse/transformation/quality/job.py",
        application_args=build_job_arguments(run_suffix="quality", include_mode=False),
    )

    pipeline_complete = EmptyOperator(task_id="lakehouse_pipeline_complete")

    # Xâu chuỗi toàn bộ luồng thực thi End-to-End
    (
        validate_params
        >> ingest_bronze
        >> bronze_complete
        # silver_tasks chạy song song ở giữa
        >> silver_complete
        >> build_gold
        >> run_quality
        >> pipeline_complete
    )
