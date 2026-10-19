"""Khởi tạo và cấu hình phiên làm việc SparkSession tích hợp Apache Iceberg, MinIO S3 và PostgreSQL Catalog."""

from __future__ import annotations

from pyspark.sql import SparkSession

from transformation.common.config import Settings


def build_spark_session(settings: Settings, app_name: str) -> SparkSession:
    """Tạo SparkSession đồng nhất với các cấu hình tối ưu cho Data Lakehouse.

    Các cấu hình trọng yếu:
    - Master URL: Kết nối tới Spark Master phân tán (spark://spark-master:7077).
    - Iceberg Extensions: Kích hoạt cú pháp SQL của Iceberg (MERGE INTO, ALTER TABLE...).
    - JDBC Catalog: Đăng ký metadata bảng Iceberg vào PostgreSQL database.
    - S3 FileIO & Hadoop S3A: Kết nối lưu trữ Object Storage MinIO (s3://lakehouse/...).
    - Adaptive Query Execution (AQE): Tự động tối ưu kế hoạch thực thi câu truy vấn khi chạy.
    """
    catalog = f"spark.sql.catalog.{settings.iceberg_catalog_name}"
    return (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master_url)
        # 1. Kích hoạt tiện ích mở rộng của Apache Iceberg
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.defaultCatalog", settings.iceberg_catalog_name)
        # 2. Cấu hình Catalog Iceberg dùng PostgreSQL qua JDBC
        .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog}.type", "jdbc")
        .config(f"{catalog}.uri", settings.iceberg_jdbc_uri)
        .config(f"{catalog}.jdbc.user", "iceberg")
        .config(f"{catalog}.jdbc.password", settings.iceberg_db_password)
        .config(f"{catalog}.jdbc.schema-version", "V1")
        .config(f"{catalog}.warehouse", settings.iceberg_warehouse)
        # 3. Cấu hình S3FileIO của Iceberg để giao tiếp với MinIO
        .config(f"{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{catalog}.s3.endpoint", settings.s3_endpoint)
        .config(f"{catalog}.s3.path-style-access", "true")
        .config(f"{catalog}.s3.region", settings.s3_region)
        # 4. Cấu hình Hadoop S3A FileSystem (đọc ghi trực tiếp file S3)
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", settings.s3_region)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        # 5. Tối ưu thực thi truy vấn (AQE) và kích thước file Parquet mục tiêu (128MB)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.iceberg.write.distribution-mode", "none")
        .config("spark.sql.iceberg.write.target-file-size-bytes", "134217728")
        .getOrCreate()
    )
