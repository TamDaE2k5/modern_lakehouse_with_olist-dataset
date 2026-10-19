"""Các phép biến đổi và bổ sung metadata xuất xứ (Lineage) cho dữ liệu thô Tầng Bronze.

Nguyên tắc Tầng Bronze:
- Bảo toàn nguyên vẹn 100% dữ liệu gốc từ CSV, không lọc bỏ, không ép kiểu mất mát.
- Gắn thêm 4 cột metadata hệ thống phục vụ truy vết (Lineage) và phân vùng (Partitioning):
  1. _ingestion_timestamp: Thời điểm chính xác nạp dữ liệu vào Lakehouse (UTC).
  2. _ingestion_date: Ngày nạp (dùng để phân vùng bảng Iceberg theo days(_ingestion_date)).
  3. _batch_id: Mã định danh đợt nạp (phục vụ chạy bù/idempotent batch replay).
  4. _source_file: Tên file nguồn (ví dụ: olist_orders_dataset.csv).
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ingestion_metadata(
    df: DataFrame,
    batch_id: str,
    source_file: str,
    ingested_at: datetime,
) -> DataFrame:
    """Giữ nguyên toàn bộ giá trị nguồn và gắn thêm 4 cột metadata Bronze."""
    # Đảm bảo thời gian theo chuẩn UTC
    utc_time = (
        ingested_at.replace(tzinfo=timezone.utc)
        if ingested_at.tzinfo is None
        else ingested_at.astimezone(timezone.utc)
    )
    timestamp = F.to_timestamp(F.lit(utc_time.isoformat()))

    return (
        df.withColumn("_ingestion_timestamp", timestamp)
        .withColumn("_ingestion_date", F.to_date(F.col("_ingestion_timestamp")))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_file", F.lit(source_file))
    )


# ==============================================================================
# LEGACY / OVER-ENGINEERED CODE (COMMENTED OUT FOR REFERENCE)
#
# Reason for removal:
# The original project calculated a SHA-256 hash for every single row (_row_hash)
# by serializing each row into a JSON string and hashing it.
# In a business e-commerce data lakehouse, this per-row cryptographic hashing adds
# heavy CPU overhead without providing practical value. Standard audit columns
# (_ingestion_timestamp, _ingestion_date, _batch_id, _source_file) are sufficient.
# ==============================================================================
#
# def _legacy_add_ingestion_metadata_with_row_hash(
#     df: DataFrame,
#     batch_id: str,
#     source_file: str,
#     ingested_at: datetime,
# ) -> DataFrame:
#     source_columns = tuple(df.columns)
#     utc_time = (
#         ingested_at.replace(tzinfo=timezone.utc)
#         if ingested_at.tzinfo is None
#         else ingested_at.astimezone(timezone.utc)
#     )
#     timestamp = F.to_timestamp(F.lit(utc_time.isoformat()))
#     canonical_row = F.to_json(
#         F.struct(*(F.col(name).alias(name) for name in source_columns)),
#         options={"ignoreNullFields": "false"},
#     )
#     row_hash = F.sha2(canonical_row, 256)
#
#     return (
#         df.withColumn("_ingestion_timestamp", timestamp)
#         .withColumn("_ingestion_date", F.to_date(F.col("_ingestion_timestamp")))
#         .withColumn("_batch_id", F.lit(batch_id))
#         .withColumn("_source_file", F.lit(source_file))
#         .withColumn("_row_hash", row_hash)
#     )
