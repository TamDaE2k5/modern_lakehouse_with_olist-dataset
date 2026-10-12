"""Silver product typing, category translation, and physical metrics."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformation.common.validation import TransformResult, ValidationRule
from transformation.silver._common import blank, deterministic_result


MEASUREMENT_TYPE = "decimal(18,3)"
VOLUME_TYPE = "decimal(38,6)"


def _translation_lookup(translations: DataFrame) -> DataFrame:
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
    return (~blank(raw_name)) & F.col(typed_name).isNull()


def transform_products(products: DataFrame, translations: DataFrame) -> TransformResult:
    """Return typed unique products enriched from the Bronze category translation."""
    # Fix typo in original Kaggle dataset columns ("lenght" -> "length")
    renamed = products.withColumnRenamed(
        "product_name_lenght", "product_name_length"
    ).withColumnRenamed(
        "product_description_lenght", "product_description_length"
    )
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
    for name, spark_type in typed_columns.items():
        normalized = normalized.withColumn(f"_raw_{name}", F.col(name)).withColumn(
            name, F.trim(F.col(name)).cast(spark_type)
        )

    # Broadcast join with small category translation table (~71 rows)
    normalized = (
        normalized.join(
            F.broadcast(_translation_lookup(translations)),
            F.col("product_category_name") == F.col("_translation_category"),
            "left",
        )
        .drop("_translation_category")
        .withColumn(
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
            "volume_cm3",
            (
                F.col("product_length_cm")
                * F.col("product_height_cm")
                * F.col("product_width_cm")
            ).cast(VOLUME_TYPE),
        )
    )

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
        rules.append(
            ValidationRule(
                f"INVALID_{name.upper()}",
                _invalid_optional_number(f"_raw_{name}", name),
            )
        )
        rules.append(ValidationRule(f"NEGATIVE_{name.upper()}", F.col(name) < 0))

    result = deterministic_result(
        normalized,
        tuple(rules),
        key=("product_id",),
        duplicate_code="DUPLICATE_PRODUCT_ID",
    )
    helper_columns = tuple(f"_raw_{name}" for name in typed_columns)
    return TransformResult(
        accepted=result.accepted.drop(*helper_columns),
        rejected=result.rejected.drop(*helper_columns),
    )
