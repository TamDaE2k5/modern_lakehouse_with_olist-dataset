"""Quản lý các thông số cấu hình môi trường (Settings) cho toàn bộ hệ thống Lakehouse.

Nguyên tắc an toàn (Security by Design):
- Các trường mật khẩu (minio_root_password, iceberg_db_password...) được ẩn khỏi hàm repr() (field(repr=False))
  để tránh lộ lọt mật khẩu ra console log khi in đối tượng Settings.
- Tự động fallback lấy giá trị từ các biến môi trường chuẩn.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import os


@dataclass(frozen=True)
class Settings:
    """Cấu hình bất biến (Immutable) chứa thông tin kết nối MinIO, PostgreSQL, Spark và Iceberg."""

    # 1. Các thông tin bảo mật (ẩn khỏi log)
    minio_root_user: str = field(repr=False)
    minio_root_password: str = field(repr=False)
    iceberg_db_password: str = field(repr=False)

    # 2. Cấu hình Object Storage (MinIO S3)
    s3_endpoint: str = "http://minio:9000"
    s3_region: str = "us-east-1"
    iceberg_warehouse: str = "s3://lakehouse/"

    # 3. Cấu hình Spark Cluster
    spark_master_url: str = "spark://spark-master:7077"

    # 4. Cấu hình PostgreSQL Catalog cho Apache Iceberg
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    iceberg_database: str = "iceberg"
    iceberg_catalog_name: str = "lakehouse"

    @property
    def iceberg_jdbc_uri(self) -> str:
        """Tạo chuỗi kết nối JDBC tới database PostgreSQL của Iceberg Catalog."""
        return (
            f"jdbc:postgresql://{self.postgres_host}:"
            f"{self.postgres_port}/{self.iceberg_database}"
        )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        """Đọc và kiểm tra các biến môi trường bắt buộc trước khi khởi chạy job."""
        values = os.environ if env is None else env
        required_secrets = (
            "MINIO_ROOT_USER",
            "MINIO_ROOT_PASSWORD",
            "ICEBERG_DB_PASSWORD",
        )
        missing = [name for name in required_secrets if not values.get(name)]
        if missing:
            raise ValueError(f"Thiếu các biến môi trường bắt buộc: {', '.join(missing)}")

        return cls(
            minio_root_user=values["MINIO_ROOT_USER"],
            minio_root_password=values["MINIO_ROOT_PASSWORD"],
            iceberg_db_password=values["ICEBERG_DB_PASSWORD"],
            s3_endpoint=values.get("S3_ENDPOINT", "http://minio:9000"),
            s3_region=values.get("S3_REGION", "us-east-1"),
            iceberg_warehouse=values.get("ICEBERG_WAREHOUSE", "s3://lakehouse/"),
            spark_master_url=values.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
            postgres_host=values.get("POSTGRES_HOST", "postgres"),
        )
