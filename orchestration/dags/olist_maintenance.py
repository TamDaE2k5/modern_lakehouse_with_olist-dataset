"""DAG Apache Airflow điều phối quy trình bảo trì và tối ưu hóa an toàn bảng Apache Iceberg (Chặng 8 - Trụ cột 2).

Kiến trúc và Nguyên tắc Hoạt động:
1. Thẩm định tham số đầu vào (validate_params):
   - Đảm bảo namespace (bronze/silver/gold) và table đích nằm trong danh sách an toàn (allowlist).
   - Kiểm tra retention_hours (tối thiểu 168h = 7 ngày) và orphan_older_than_hours (tối thiểu 336h = 14 ngày).
2. Kiểm tra trước không gây biến đổi (maintenance_dry_run):
   - Quét metadata snapshot, đếm số lượng data files và dung lượng byte thực tế.
   - Ký số báo cáo thẩm định dry-run JSON bằng token HMAC SHA-256 qua secret bảo mật.
3. Nén và gộp tệp nhỏ (rewrite_data_files):
   - Thực thi thủ tục hệ thống `CALL system.rewrite_data_files(...)`.
   - Gộp các tệp Parquet phân mảnh nhỏ thành các tệp tiêu chuẩn ~128MB (target-file-size-bytes: 134,217,728).
4. Dọn dẹp Snapshot cũ (expire_snapshots):
   - Thực thi thủ tục hệ thống `CALL system.expire_snapshots(...)`.
   - Cắt tỉa các snapshot lịch sử vượt quá retention_hours nhưng luôn giữ lại tối thiểu 2 snapshot gần nhất.
5. Xóa tệp mồ côi ngoài storage (remove_orphan_files):
   - Thực thi thủ tục `CALL system.remove_orphan_files(...)`.
   - Dọn dẹp triệt để các file rác trên MinIO S3 không còn thuộc bất kỳ snapshot nào, tiết kiệm dung lượng ổ đĩa.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.models.param import Param

try:
    from orchestration.dags.common import (
        build_job_arguments,
        dag_default_args,
        runtime_params,
        runtime_validation_task,
        spark_submit_task,
    )
except ModuleNotFoundError:  # Airflow thêm trực tiếp thư mục DAGs vào sys.path
    from common import (  # type: ignore[no-redef]
        build_job_arguments,
        dag_default_args,
        runtime_params,
        runtime_validation_task,
        spark_submit_task,
    )


# Cấu hình tham số runtime có kiểm tra ràng buộc chặt chẽ
params = runtime_params(default_mode="full")
params.update(
    {
        "namespace": Param(
            "gold",
            type="string",
            enum=["bronze", "silver", "gold"],
            description="Namespace chứa table Iceberg đích cần bảo trì",
        ),
        "table": Param(
            "fact_orders",
            type="string",
            pattern=r"^[a-z][a-z0-9_]{0,63}$",
            description="Tên table Iceberg đích trong allowlist",
        ),
        "retention_hours": Param(
            168,
            type="integer",
            minimum=168,
            maximum=8760,
            description="Số giờ lưu giữ snapshot tối thiểu (mặc định 168 giờ = 7 ngày)",
        ),
        "orphan_older_than_hours": Param(
            336,
            type="integer",
            minimum=168,
            maximum=8760,
            description="Tuổi tối thiểu theo giờ của orphan file để dọn dẹp (mặc định 336 giờ = 14 ngày)",
        ),
    }
)


def maintenance_arguments(*, action: str, run_suffix: str) -> list[str]:
    """Tạo danh sách đối số dòng lệnh an toàn cho job Spark maintenance."""
    arguments = build_job_arguments(run_suffix=run_suffix, include_mode=True)
    run_base = "{% if params.run_id == 'auto' %}{{ ts_nodash }}{% else %}{{ params.run_id }}{% endif %}"
    arguments.extend(
        [
            "--namespace",
            "{{ params.namespace }}",
            "--table",
            "{{ params.table }}",
            "--approval-run-id",
            run_base,
            "--action",
            action,
            "--retention-hours",
            "{{ params.retention_hours }}",
            "--orphan-older-than-hours",
            "{{ params.orphan_older_than_hours }}",
            "--dry-run-report",
            f"/opt/airflow/logs/maintenance/{run_base}/dry-run.json",
        ]
    )
    return arguments


with DAG(
    dag_id="olist_maintenance",
    description="Bảo trì an toàn, nén file và dọn dẹp snapshot một table Iceberg trong allowlist",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params=params,
    default_args=dag_default_args(),
    tags=["olist", "maintenance", "iceberg", "optimization"],
) as dag:
    # 1. Thẩm định tham số runtime trước khi khởi động Spark
    validate_params = runtime_validation_task()

    # 2. Bước khảo sát không phá hủy (Dry-Run): Lập kế hoạch và ký token HMAC
    maintenance_dry_run = spark_submit_task(
        task_id="maintenance_dry_run",
        application="/opt/lakehouse/transformation/maintenance/job.py",
        application_args=maintenance_arguments(
            action="dry-run",
            run_suffix="maintenance-dry-run",
        ),
    )

    # 3. Nén gộp các tệp nhỏ phân mảnh thành tệp Parquet ~128MB
    # Dùng 8 write_pool_slots từ pool lakehouse_writes để cô lập tài nguyên ghi
    rewrite_data_files = spark_submit_task(
        task_id="rewrite_data_files",
        application="/opt/lakehouse/transformation/maintenance/job.py",
        application_args=maintenance_arguments(
            action="rewrite-data-files",
            run_suffix="maintenance-rewrite",
        ),
        write_pool_slots=8,
    )

    # 4. Cắt tỉa các snapshot lịch sử cũ quá hạn
    expire_snapshots = spark_submit_task(
        task_id="expire_snapshots",
        application="/opt/lakehouse/transformation/maintenance/job.py",
        application_args=maintenance_arguments(
            action="expire-snapshots",
            run_suffix="maintenance-expire",
        ),
        write_pool_slots=8,
    )

    # 5. Dọn dẹp triệt để các tệp mồ côi (orphan files) không còn dùng trên S3/MinIO
    remove_orphan_files = spark_submit_task(
        task_id="remove_orphan_files",
        application="/opt/lakehouse/transformation/maintenance/job.py",
        application_args=(
            maintenance_arguments(
                action="remove-orphan-files",
                run_suffix="maintenance-orphans",
            )
            + ["--confirm-orphan-cleanup"]
        ),
        write_pool_slots=8,
    )

    # Thiết lập chuỗi phụ thuộc tuyến tính nghiêm ngặt:
    # Bắt buộc qua dry-run xác thực trước khi thực thi bất kỳ thao tác ghi/xóa nào
    (
        validate_params
        >> maintenance_dry_run
        >> rewrite_data_files
        >> expire_snapshots
        >> remove_orphan_files
    )
