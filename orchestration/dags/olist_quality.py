"""DAG kiểm định chất lượng dữ liệu định kỳ trên toàn bộ các tầng (Bronze, Silver, Gold).

DAG này thực hiện chế độ chỉ đọc (Read-only Quality Audit):
- Không ghi đè hay thay đổi bất kỳ bảng nghiệp vụ nào.
- Thực thi toàn diện 198 bài kiểm tra chất lượng (kết hợp Great Expectations 1.21.0 và Spark-native checks).
- Lưu kết quả kiểm toán bất biến vào Iceberg table 'lakehouse.quality.quality_results'.
- Ghi log trạng thái vào 'lakehouse.control.pipeline_runs'.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG

try:
    from orchestration.dags.common import (
        dag_default_args,
        runtime_params,
        runtime_validation_task,
        spark_submit_task,
    )
except ModuleNotFoundError:  # Airflow adds only the DAG directory to sys.path.
    from common import (  # type: ignore[no-redef]
        dag_default_args,
        runtime_params,
        runtime_validation_task,
        spark_submit_task,
    )


with DAG(
    dag_id="olist_quality",
    description="Kiểm định chất lượng dữ liệu tự động cho Bronze, Silver, Gold (Great Expectations & Spark)",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,  # Có thể chạy On-demand hoặc được gọi từ Master DAG
    catchup=False,
    max_active_runs=1,
    params=runtime_params(),
    default_args=dag_default_args(),
    tags=["olist", "quality", "great-expectations"],
) as dag:
    # 1. Xác thực tham số đầu vào an toàn
    validate_params = runtime_validation_task()

    # 2. Giao việc tính toán chất lượng phân tán cho cụm Spark
    run_quality_checks = spark_submit_task(
        task_id="run_quality_checks",
        application="/opt/lakehouse/data_quality/run_checks.py",
        application_args=[
            "--run-id",
            "{{ params.run_id }}-quality",
            "--attempt-id",
            "{{ ti.try_number }}",
            "--layers",
            "bronze,silver,gold",
        ],
    )

    validate_params >> run_quality_checks
