"""Chuẩn hóa kiểu dữ liệu, dịch danh mục tiếng Anh và tính toán thể tích Sản phẩm (Products).

Các điểm cải tiến cốt lõi:
1. Sửa lỗi chính tả từ file nguồn Kaggle: `lenght` -> `length`.
2. Ép kiểu dữ liệu chuẩn: kích thước/cân nặng sang decimal(18,3), số lượng ảnh/độ dài chữ sang integer.
3. Broadcast Join với bảng từ điển product_category_translation để dịch danh mục sang tiếng Anh.
4. Feature Engineering: Tính toán thể tích đóng gói `volume_cm3 = length * height * width`.
5. Kiểm định chất lượng: Bắt lỗi ID rỗng, lỗi ép kiểu số và số đo bị âm (< 0).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result

# Kiểu dữ liệu chính xác cho số đo vật lý và thể tích
MEASUREMENT_TYPE = "decimal(18,3)"
VOLUME_TYPE = "decimal(38,6)"


def _translation_lookup(translations: DataFrame) -> DataFrame:
    """Tạo bảng tra cứu bản dịch danh mục tiếng Bồ Đào Nha -> tiếng Anh."""
    category = F.lower(F.trim("product_category_name"))
    ordering = Window.partitionBy(category).orderBy(
        F.col("_ingestion_timestamp").desc_nulls_last(),
        F.col("_batch_id").desc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
    )
    return (
        translations.withColumn("_lookup_rank", F.row_number().over(ordering))
        .where(F.col("_lookup_rank") == 1)
        .select(
            category.alias("_translation_category"),
            F.lower(F.trim("product_category_name_english")).alias(
                "_translated_category_english"
            ),
        )
    )


def _invalid_optional_number(raw_name: str, typed_name: str):
    """Kiểm tra: Nếu chuỗi thô có dữ liệu nhưng khi cast sang số lại ra NULL (chứng tỏ bị lỗi ký tự)."""
    return (~blank(raw_name)) & F.col(typed_name).isNull()


def transform_products(products: DataFrame, translations: DataFrame) -> TransformResult:
    """Biến đổi bảng Bronze products thành dữ liệu Silver sạch và Vùng cách ly Quarantine."""
    # Bước 1: Khắc phục lỗi chính tả từ dataset gốc của Kaggle
    renamed = products.withColumnRenamed(
        "product_name_lenght", "product_name_length"
    ).withColumnRenamed(
        "product_description_lenght", "product_description_length"
    )

    # Danh sách các cột số cần ép kiểu
    typed_columns = {
        "product_name_length": "int",
        "product_description_length": "int",
        "product_photos_qty": "int",
        "product_weight_g": MEASUREMENT_TYPE,
        "product_length_cm": MEASUREMENT_TYPE,
        "product_height_cm": MEASUREMENT_TYPE,
        "product_width_cm": MEASUREMENT_TYPE,
    }

    normalized = renamed.withColumn("product_id", F.trim("product_id")).withColumn(
        "product_category_name", F.lower(F.trim("product_category_name"))
    )

    # Lưu lại chuỗi gốc vào _raw_* để phục vụ kiểm tra lỗi ép kiểu
    for name, spark_type in typed_columns.items():
        normalized = normalized.withColumn(f"_raw_{name}", F.col(name)).withColumn(
            name, F.trim(F.col(name)).cast(spark_type)
        )

    # Bước 2: Broadcast Join với bảng từ điển danh mục (~71 dòng) và tính thể tích
    normalized = (
        normalized.join(
            F.broadcast(_translation_lookup(translations)),
            F.col("product_category_name") == F.col("_translation_category"),
            "left",
        )
        .drop("_translation_category")
        .withColumn(
            # Nếu không tìm thấy bản dịch, gán mặc định là "unknown" thay vì để NULL
            "product_category_name_english",
            F.coalesce(
                F.when(
                    F.length("_translated_category_english") > 0,
                    F.col("_translated_category_english"),
                ),
                F.lit("unknown"),
            ),
        )
        .drop("_translated_category_english")
        .withColumn(
            # Tính thể tích (volume_cm3 = Dài * Rộng * Cao)
            "volume_cm3",
            (
                F.col("product_length_cm")
                * F.col("product_height_cm")
                * F.col("product_width_cm")
            ).cast(VOLUME_TYPE),
        )
    )

    # Bước 3: Tạo các quy tắc kiểm định chất lượng (Data Quality Rules)
    rules: list[ValidationRule] = [ValidationRule("MISSING_PRODUCT_ID", blank("product_id"))]
    for name in (
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ):
        # Lỗi ép kiểu (có ký tự lạ)
        rules.append(
            ValidationRule(
                f"INVALID_{name.upper()}",
                _invalid_optional_number(f"_raw_{name}", name),
            )
        )
        # Lỗi số đo bị âm (< 0)
        rules.append(ValidationRule(f"NEGATIVE_{name.upper()}", F.col(name) < 0))

    # Bước 4: Khử trùng lặp xác định theo product_id
    result = deterministic_result(
        normalized,
        tuple(rules),
        key=("product_id",),
        duplicate_code="DUPLICATE_PRODUCT_ID",
    )

    # Dọn dẹp các cột tạm _raw_* trước khi trả về
    helper_columns = tuple(f"_raw_{name}" for name in typed_columns)
    return TransformResult(
        accepted=result.accepted.drop(*helper_columns),
        rejected=result.rejected.drop(*helper_columns),
    )
