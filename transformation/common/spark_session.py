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
        # ======================================================================
        # 5. TỐI ƯU THỰC THI TRUY VẤN THÍCH ỨNG (ADAPTIVE QUERY EXECUTION - AQE)
        # ======================================================================
        # VÌ SAO PHẢI ĐẶT CÁC CỜ NÀY Ở ĐÂY MÀ KHÔNG PHẢI TRONG spark-defaults.conf?
        # Toàn bộ 4 DAG đều chạy job qua SparkSubmitOperator ở chế độ `client`, nghĩa là
        # tiến trình Driver (nơi Spark lập kế hoạch thực thi truy vấn) nằm TRONG container
        # Airflow chứ không phải container Spark. Mà container Airflow lại KHÔNG có thư mục
        # /opt/spark/conf/, nên file spark-defaults.conf của image Spark hoàn toàn không
        # được đọc tới. Vì vậy nơi duy nhất cấu hình AQE thực sự có hiệu lực với pipeline
        # chính là SparkSession builder này.
        #
        # 5.1. Bật AQE: cho phép Spark sửa lại kế hoạch thực thi NGAY TRONG LÚC CHẠY, dựa
        #      trên số liệu thống kê thật đo được sau mỗi giai đoạn shuffle — thay vì bám
        #      cứng vào kế hoạch lập sẵn từ các ước lượng thường sai lệch.
        .config("spark.sql.adaptive.enabled", "true")
        # 5.2. Tự động gộp phân vùng nhỏ sau shuffle. Nếu không bật, một phép tổng hợp trên
        #      lô 500 đơn hàng (bộ dữ liệu giờ thấp điểm) vẫn sinh ra đủ 200 task, trong đó
        #      gần như toàn bộ xử lý phân vùng rỗng — chi phí điều phối lớn hơn cả công việc thật.
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "8MB")
        # 5.3. Kích thước phân vùng mục tiêu sau khi gộp. Chọn 64MB (bằng nửa kích thước tệp
        #      đích 128MB) để Spark Worker 2 nhân / 4 GiB vẫn giữ được độ song song hợp lý
        #      mà không tạo ra các phân vùng quá nhỏ.
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
        # 5.4. Tự động tách phân vùng bị lệch (skew). Đây là lá chắn cho kịch bản Flash-Sale:
        #      khi vài sản phẩm hot chiếm phần lớn số dòng, phân vùng chứa chúng phình to gấp
        #      nhiều lần các phân vùng khác, khiến một executor è cổ xử lý trong lúc các
        #      executor còn lại ngồi chơi — và thường kết thúc bằng lỗi tràn bộ nhớ (OOM).
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        # Hạ ngưỡng phát hiện lệch từ mặc định 256MB xuống 64MB: với quy mô dữ liệu của hệ
        # thống này, mặc định 256MB quá cao nên cơ chế chống lệch gần như không bao giờ kích hoạt.
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")
        # 5.5. Số phân vùng shuffle KHỞI ĐIỂM. Triết lý khi đã bật AQE là "đặt dư rồi để AQE
        #      gộp lại": đặt sẵn 4 phân vùng sẽ chặn mất cả khả năng song song lúc tải cao lẫn
        #      cơ hội để AQE tự tối ưu. Đặt 32 cho AQE đủ nguyên liệu để co giãn hai chiều.
        .config("spark.sql.shuffle.partitions", "32")
        # 5.6. Ngưỡng tự động Broadcast Join 50MB. Lưu ý các gợi ý `F.broadcast()` tường minh
        #      trong tầng Gold vẫn được ưu tiên tuyệt đối, bất kể ngưỡng này là bao nhiêu.
        .config("spark.sql.autoBroadcastJoinThreshold", "52428800")
        # 5.7. Cấu hình ghi Iceberg: không phân phối lại khi ghi, nhắm tệp Parquet đích 128MB.
        .config("spark.sql.iceberg.write.distribution-mode", "none")
        .config("spark.sql.iceberg.write.target-file-size-bytes", "134217728")
        .getOrCreate()
    )
