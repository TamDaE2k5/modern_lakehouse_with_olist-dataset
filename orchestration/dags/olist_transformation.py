"""DAG Điều phối Chuyển đổi Dữ liệu Độc lập: Silver -> Gold (olist_transformation).

Kiến trúc Luồng Thực Thi của DAG (DAG Execution Architecture):
--------------------------------------------------------------
1. Kiểm tra Tham số Runtime (validate_runtime_params):
   - Đảm bảo `run_id`, `batch_id`, `mode` an toàn và đúng định dạng trước khi phân bổ tài nguyên.
2. Xử lý Song song 8 Bảng Silver (Parallel Silver Extraction & Transformation):
   - Thay vì chạy tuần tự làm nghẽn pipeline, 8 bảng Silver độc lập được bắn song song lên cụm Spark:
     + `customers` (Khách hàng)
     + `sellers` (Người bán)
     + `products` (Sản phẩm)
     + `geography` (Toạ độ địa lý)
     + `orders` (Đơn hàng)
     + `order_items` (Món hàng)
     + `order_payments` (Thanh toán)
     + `order_reviews` (Đánh giá)
   - Mỗi task chạy thông qua `SparkSubmitOperator` với slot quản lý tài nguyên trong pool `lakehouse_writes`.
3. Điểm chốt kiểm định chất lượng (Silver Quality Gate):
   - Đảm bảo toàn bộ 8 bảng Silver đã hoàn tất thành công và dữ liệu đạt chuẩn trước khi bước vào tầng Gold.
4. Xây dựng Trọn vẹn Khép kín 14 Bảng Gold (Complete Gold Dependency Closure):
   - Chạy `gold/job.py` để tự động tính toán các Khóa đại diện liên tục, giải quyết đồ thị DAG
     xây dựng tuần tự 5 Dimensions -> 2 Bridges -> 4 Facts -> 3 Marts.
   - Đối soát bảo toàn doanh thu tuyệt đối trước khi ghi vào Iceberg.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator

try:
    from orchestration.dags.common import (
        IdempotentTriggerDagRunOperator,
        build_job_arguments,
        dag_default_args,
        runtime_params,
        runtime_validation_task,
        spark_submit_task,
    )
except ModuleNotFoundError:
    # Airflow tự động thêm thư mục dags vào sys.path khi chạy bên trong container
    from common import (  # type: ignore[no-redef]
        IdempotentTriggerDagRunOperator,
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
    dag_id="olist_transformation",
    description="Quy trình chuyển đổi tự động: Làm sạch 8 bảng Silver song song và xây dựng 14 bảng Gold",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,  # Được kích hoạt theo yêu cầu hoặc tự động qua master pipeline
    catchup=False,
    max_active_runs=1,
    params=runtime_params(default_mode="full"),
    default_args=dag_default_args(),
    tags=["olist", "transformation", "silver", "gold"],
) as dag:

    # Bước 1: Kiểm định an toàn tham số đầu vào
    validate_params = runtime_validation_task()

    # Bước 2: 8 Task xử lý tầng Silver chạy song song
    silver_tasks = []
    for table_name in SILVER_TABLES:
        silver_task = spark_submit_task(
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
        validate_params >> silver_task
        silver_tasks.append(silver_task)

    # Bước 3: Điểm hội tụ kiểm định chất lượng tầng Silver
    silver_gate = EmptyOperator(
        task_id="silver_quality_gate",
    )
    for task in silver_tasks:
        task >> silver_gate

    # Bước 4: Xây dựng toàn bộ mô hình Gold (Star Schema, Bridges & Data Marts)
    build_gold = spark_submit_task(
        task_id="build_gold_closure",
        application="/opt/lakehouse/transformation/gold/job.py",
        application_args=build_job_arguments(run_suffix="gold", include_mode=True),
        write_pool_slots=1,
    )

    # Các cột mốc đánh dấu hoàn tất theo chuẩn ngữ nghĩa Kimball
    dimensions_complete = EmptyOperator(task_id="dimensions_phase_complete")
    facts_bridges_complete = EmptyOperator(task_id="facts_bridges_phase_complete")
    marts_complete = EmptyOperator(task_id="marts_phase_complete")
    pipeline_success = EmptyOperator(task_id="transformation_pipeline_success")

    # Thiết lập chuỗi phụ thuộc tuần tự
    (
        silver_gate
        >> build_gold
        >> dimensions_complete
        >> facts_bridges_complete
        >> marts_complete
        >> pipeline_success
    )
