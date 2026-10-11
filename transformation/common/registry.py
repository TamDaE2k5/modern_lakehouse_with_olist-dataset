"""Metadata-driven registry for the nine Olist source tables."""

from __future__ import annotations

from transformation.common.models import ColumnSpec, QualityRuleSpec, TableSpec


def _source(name: str, nullable: bool = False) -> ColumnSpec:
    return ColumnSpec(name=name, spark_type="string", nullable=nullable)


BRONZE_METADATA_COLUMNS = (
    ColumnSpec("_ingestion_timestamp", "timestamp", False),
    ColumnSpec("_ingestion_date", "date", False),
    ColumnSpec("_batch_id", "string", False),
    ColumnSpec("_source_file", "string", False),
    ColumnSpec("_row_hash", "string", False),
)

REQUIRED_QUALITY_RULES = (
    QualityRuleSpec("row_count_positive", "row_count", "ERROR"),
    QualityRuleSpec("schema_matches_registry", "schema", "ERROR"),
)


def _columns(*source_columns: ColumnSpec) -> tuple[ColumnSpec, ...]:
    return source_columns + BRONZE_METADATA_COLUMNS


TABLE_SPECS: dict[str, TableSpec] = {
    "customers": TableSpec(
        name="customers",
        csv_file="olist_customers_dataset.csv",
        grain="One source customer row",
        key=("customer_id",),
        columns=_columns(
            _source("customer_id"),
            _source("customer_unique_id"),
            _source("customer_zip_code_prefix"),
            _source("customer_city"),
            _source("customer_state"),
        ),
        quality_rules=REQUIRED_QUALITY_RULES,
    ),
    "orders": TableSpec(
        name="orders",
        csv_file="olist_orders_dataset.csv",
        grain="One source order row",
        key=("order_id",),
        columns=_columns(
            _source("order_id"),
            _source("customer_id"),
            _source("order_status"),
            _source("order_purchase_timestamp"),
            _source("order_approved_at", nullable=True),
            _source("order_delivered_carrier_date", nullable=True),
            _source("order_delivered_customer_date", nullable=True),
            _source("order_estimated_delivery_date"),
        ),
        quality_rules=REQUIRED_QUALITY_RULES,
    ),
    "order_items": TableSpec(
        name="order_items",
        csv_file="olist_order_items_dataset.csv",
        grain="One source order-line row",
        key=("order_id", "order_item_id"),
        columns=_columns(
            _source("order_id"),
            _source("order_item_id"),
            _source("product_id"),
            _source("seller_id"),
            _source("shipping_limit_date"),
            _source("price"),
            _source("freight_value"),
        ),
        quality_rules=REQUIRED_QUALITY_RULES,
    ),
    "order_payments": TableSpec(
        name="order_payments",
        csv_file="olist_order_payments_dataset.csv",
        grain="One source payment row",
        key=("order_id", "payment_sequential"),
        columns=_columns(
            _source("order_id"),
            _source("payment_sequential"),
            _source("payment_type"),
            _source("payment_installments"),
            _source("payment_value"),
        ),
        quality_rules=REQUIRED_QUALITY_RULES,
    ),
    "order_reviews": TableSpec(
        name="order_reviews",
        csv_file="olist_order_reviews_dataset.csv",
        grain="One source review-order row",
        key=("review_id", "order_id"),
        columns=_columns(
            _source("review_id"),
            _source("order_id"),
            _source("review_score"),
            _source("review_comment_title", nullable=True),
            _source("review_comment_message", nullable=True),
            _source("review_creation_date"),
            _source("review_answer_timestamp"),
        ),
        quality_rules=REQUIRED_QUALITY_RULES,
    ),
    "products": TableSpec(
        name="products",
        csv_file="olist_products_dataset.csv",
        grain="One source product row",
        key=("product_id",),
        columns=_columns(
            _source("product_id"),
            _source("product_category_name", nullable=True),
            _source("product_name_lenght", nullable=True),
            _source("product_description_lenght", nullable=True),
            _source("product_photos_qty", nullable=True),
            _source("product_weight_g", nullable=True),
            _source("product_length_cm", nullable=True),
            _source("product_height_cm", nullable=True),
            _source("product_width_cm", nullable=True),
        ),
        quality_rules=REQUIRED_QUALITY_RULES,
    ),
    "sellers": TableSpec(
        name="sellers",
        csv_file="olist_sellers_dataset.csv",
        grain="One source seller row",
        key=("seller_id",),
        columns=_columns(
            _source("seller_id"),
            _source("seller_zip_code_prefix"),
            _source("seller_city"),
            _source("seller_state"),
        ),
        quality_rules=REQUIRED_QUALITY_RULES,
    ),
    "geolocation": TableSpec(
        name="geolocation",
        csv_file="olist_geolocation_dataset.csv",
        grain="One source coordinate observation",
        key=("_row_hash",),
        columns=_columns(
            _source("geolocation_zip_code_prefix"),
            _source("geolocation_lat"),
            _source("geolocation_lng"),
            _source("geolocation_city"),
            _source("geolocation_state"),
        ),
        quality_rules=REQUIRED_QUALITY_RULES,
    ),
    "product_category_translation": TableSpec(
        name="product_category_translation",
        csv_file="product_category_name_translation.csv",
        grain="One category translation row",
        key=("product_category_name",),
        columns=_columns(
            _source("product_category_name"),
            _source("product_category_name_english"),
        ),
        quality_rules=REQUIRED_QUALITY_RULES,
    ),
}


def get_table_spec(name: str) -> TableSpec:
    """Return a table spec or raise a stable error for an unknown table name."""

    try:
        return TABLE_SPECS[name]
    except KeyError:
        raise KeyError(f"Không tìm thấy table: {name}") from None
