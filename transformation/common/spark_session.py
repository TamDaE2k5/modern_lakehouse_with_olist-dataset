"""Creation of consistently configured Spark sessions."""

from __future__ import annotations

from pyspark.sql import SparkSession

from transformation.common.config import Settings


def build_spark_session(settings: Settings, app_name: str) -> SparkSession:
    """Build a Spark session for the shared JDBC-backed Iceberg catalog."""

    catalog = f"spark.sql.catalog.{settings.iceberg_catalog_name}"
    return (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master_url)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.defaultCatalog", settings.iceberg_catalog_name)
        .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog}.type", "jdbc")
        .config(f"{catalog}.uri", settings.iceberg_jdbc_uri)
        .config(f"{catalog}.jdbc.user", "iceberg")
        .config(f"{catalog}.jdbc.password", settings.iceberg_db_password)
        .config(f"{catalog}.jdbc.schema-version", "V1")
        .config(f"{catalog}.warehouse", settings.iceberg_warehouse)
        .config(f"{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{catalog}.s3.endpoint", settings.s3_endpoint)
        .config(f"{catalog}.s3.path-style-access", "true")
        .config(f"{catalog}.s3.region", settings.s3_region)
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
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.iceberg.write.distribution-mode", "none")
        .config("spark.sql.iceberg.write.target-file-size-bytes", "134217728")
        .getOrCreate()
    )
