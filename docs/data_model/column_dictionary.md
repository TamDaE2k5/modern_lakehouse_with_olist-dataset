# Từ điển cột (Column Dictionary)

> Tệp này được **sinh tự động** bởi `scripts/refresh_column_dictionary.py` từ hệ thống đang
> chạy thật: kiểu dữ liệu và số dòng lấy trực tiếp từ Spark, đối chiếu chéo với
> `information_schema` của Trino. **Đừng sửa tay tệp này** — hãy sửa mã nguồn rồi chạy lại
> công cụ kết xuất.

- Thời điểm kết xuất: `2026-11-05T22:49:41.407673+00:00`
- Số bảng: `37`
- Môi trường: `{"compose_project": "olist-lakehouse", "catalog": "lakehouse", "spark": "3.5.9", "iceberg": "1.11.0", "trino": "483"}`

---

### bronze.customers

- Mức hạt (grain): `One source customer row`
- Khóa: `customer_id`
- Phụ thuộc: `csv:olist_customers_dataset.csv`
- Partition: `day(_ingestion_timestamp)`
- Số dòng thực tế: `99,441`
- Quy tắc chất lượng: `[{"columns":[],"config":{"rule_type":"row_count"},"name":"row_count_positive","severity":"ERROR","threshold":{"min_value":1}},{"columns":["customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state","_ingestion_timestamp","_ingestion_date","_batch_id","_source_file"],"config":{"rule_type":"schema"},"name":"schema_matches_registry","severity":"ERROR","threshold":null},{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["_batch_id","customer_id"],"config":{"column_list":["_batch_id","customer_id"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `customer_id` | `string` | `true` | `varchar` | `true` |
| `customer_unique_id` | `string` | `true` | `varchar` | `true` |
| `customer_zip_code_prefix` | `string` | `true` | `varchar` | `true` |
| `customer_city` | `string` | `true` | `varchar` | `true` |
| `customer_state` | `string` | `true` | `varchar` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |

### bronze.geolocation

- Mức hạt (grain): `One source coordinate observation`
- Khóa: `geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state`
- Phụ thuộc: `csv:olist_geolocation_dataset.csv`
- Partition: `day(_ingestion_timestamp)`
- Số dòng thực tế: `1,000,163`
- Quy tắc chất lượng: `[{"columns":[],"config":{"rule_type":"row_count"},"name":"row_count_positive","severity":"ERROR","threshold":{"min_value":1}},{"columns":["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng","geolocation_city","geolocation_state","_ingestion_timestamp","_ingestion_date","_batch_id","_source_file"],"config":{"rule_type":"schema"},"name":"schema_matches_registry","severity":"ERROR","threshold":null},{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["_batch_id"],"config":{"column_list":["_batch_id"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"WARN","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `geolocation_zip_code_prefix` | `string` | `true` | `varchar` | `true` |
| `geolocation_lat` | `string` | `true` | `varchar` | `true` |
| `geolocation_lng` | `string` | `true` | `varchar` | `true` |
| `geolocation_city` | `string` | `true` | `varchar` | `true` |
| `geolocation_state` | `string` | `true` | `varchar` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |

### bronze.order_items

- Mức hạt (grain): `One source order-line row`
- Khóa: `order_id, order_item_id`
- Phụ thuộc: `csv:olist_order_items_dataset.csv`
- Partition: `day(_ingestion_timestamp)`
- Số dòng thực tế: `112,650`
- Quy tắc chất lượng: `[{"columns":[],"config":{"rule_type":"row_count"},"name":"row_count_positive","severity":"ERROR","threshold":{"min_value":1}},{"columns":["order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value","_ingestion_timestamp","_ingestion_date","_batch_id","_source_file"],"config":{"rule_type":"schema"},"name":"schema_matches_registry","severity":"ERROR","threshold":null},{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["_batch_id","order_id","order_item_id"],"config":{"column_list":["_batch_id","order_id","order_item_id"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `order_id` | `string` | `true` | `varchar` | `true` |
| `order_item_id` | `string` | `true` | `varchar` | `true` |
| `product_id` | `string` | `true` | `varchar` | `true` |
| `seller_id` | `string` | `true` | `varchar` | `true` |
| `shipping_limit_date` | `string` | `true` | `varchar` | `true` |
| `price` | `string` | `true` | `varchar` | `true` |
| `freight_value` | `string` | `true` | `varchar` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |

### bronze.order_payments

- Mức hạt (grain): `One source payment row`
- Khóa: `order_id, payment_sequential`
- Phụ thuộc: `csv:olist_order_payments_dataset.csv`
- Partition: `day(_ingestion_timestamp)`
- Số dòng thực tế: `103,886`
- Quy tắc chất lượng: `[{"columns":[],"config":{"rule_type":"row_count"},"name":"row_count_positive","severity":"ERROR","threshold":{"min_value":1}},{"columns":["order_id","payment_sequential","payment_type","payment_installments","payment_value","_ingestion_timestamp","_ingestion_date","_batch_id","_source_file"],"config":{"rule_type":"schema"},"name":"schema_matches_registry","severity":"ERROR","threshold":null},{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["_batch_id","order_id","payment_sequential"],"config":{"column_list":["_batch_id","order_id","payment_sequential"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `order_id` | `string` | `true` | `varchar` | `true` |
| `payment_sequential` | `string` | `true` | `varchar` | `true` |
| `payment_type` | `string` | `true` | `varchar` | `true` |
| `payment_installments` | `string` | `true` | `varchar` | `true` |
| `payment_value` | `string` | `true` | `varchar` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |

### bronze.order_reviews

- Mức hạt (grain): `One source review-order row`
- Khóa: `review_id, order_id`
- Phụ thuộc: `csv:olist_order_reviews_dataset.csv`
- Partition: `day(_ingestion_timestamp)`
- Số dòng thực tế: `99,224`
- Quy tắc chất lượng: `[{"columns":[],"config":{"rule_type":"row_count"},"name":"row_count_positive","severity":"ERROR","threshold":{"min_value":1}},{"columns":["review_id","order_id","review_score","review_comment_title","review_comment_message","review_creation_date","review_answer_timestamp","_ingestion_timestamp","_ingestion_date","_batch_id","_source_file"],"config":{"rule_type":"schema"},"name":"schema_matches_registry","severity":"ERROR","threshold":null},{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["_batch_id","review_id","order_id"],"config":{"column_list":["_batch_id","review_id","order_id"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `review_id` | `string` | `true` | `varchar` | `true` |
| `order_id` | `string` | `true` | `varchar` | `true` |
| `review_score` | `string` | `true` | `varchar` | `true` |
| `review_comment_title` | `string` | `true` | `varchar` | `true` |
| `review_comment_message` | `string` | `true` | `varchar` | `true` |
| `review_creation_date` | `string` | `true` | `varchar` | `true` |
| `review_answer_timestamp` | `string` | `true` | `varchar` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |

### bronze.orders

- Mức hạt (grain): `One source order row`
- Khóa: `order_id`
- Phụ thuộc: `csv:olist_orders_dataset.csv`
- Partition: `day(_ingestion_timestamp)`
- Số dòng thực tế: `99,441`
- Quy tắc chất lượng: `[{"columns":[],"config":{"rule_type":"row_count"},"name":"row_count_positive","severity":"ERROR","threshold":{"min_value":1}},{"columns":["order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date","_ingestion_timestamp","_ingestion_date","_batch_id","_source_file"],"config":{"rule_type":"schema"},"name":"schema_matches_registry","severity":"ERROR","threshold":null},{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["_batch_id","order_id"],"config":{"column_list":["_batch_id","order_id"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `order_id` | `string` | `true` | `varchar` | `true` |
| `customer_id` | `string` | `true` | `varchar` | `true` |
| `order_status` | `string` | `true` | `varchar` | `true` |
| `order_purchase_timestamp` | `string` | `true` | `varchar` | `true` |
| `order_approved_at` | `string` | `true` | `varchar` | `true` |
| `order_delivered_carrier_date` | `string` | `true` | `varchar` | `true` |
| `order_delivered_customer_date` | `string` | `true` | `varchar` | `true` |
| `order_estimated_delivery_date` | `string` | `true` | `varchar` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |

### bronze.product_category_translation

- Mức hạt (grain): `One category translation row`
- Khóa: `product_category_name`
- Phụ thuộc: `csv:product_category_name_translation.csv`
- Partition: `day(_ingestion_timestamp)`
- Số dòng thực tế: `71`
- Quy tắc chất lượng: `[{"columns":[],"config":{"rule_type":"row_count"},"name":"row_count_positive","severity":"ERROR","threshold":{"min_value":1}},{"columns":["product_category_name","product_category_name_english","_ingestion_timestamp","_ingestion_date","_batch_id","_source_file"],"config":{"rule_type":"schema"},"name":"schema_matches_registry","severity":"ERROR","threshold":null},{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["_batch_id","product_category_name"],"config":{"column_list":["_batch_id","product_category_name"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `product_category_name` | `string` | `true` | `varchar` | `true` |
| `product_category_name_english` | `string` | `true` | `varchar` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |

### bronze.products

- Mức hạt (grain): `One source product row`
- Khóa: `product_id`
- Phụ thuộc: `csv:olist_products_dataset.csv`
- Partition: `day(_ingestion_timestamp)`
- Số dòng thực tế: `32,951`
- Quy tắc chất lượng: `[{"columns":[],"config":{"rule_type":"row_count"},"name":"row_count_positive","severity":"ERROR","threshold":{"min_value":1}},{"columns":["product_id","product_category_name","product_name_lenght","product_description_lenght","product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm","_ingestion_timestamp","_ingestion_date","_batch_id","_source_file"],"config":{"rule_type":"schema"},"name":"schema_matches_registry","severity":"ERROR","threshold":null},{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["_batch_id","product_id"],"config":{"column_list":["_batch_id","product_id"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `product_id` | `string` | `true` | `varchar` | `true` |
| `product_category_name` | `string` | `true` | `varchar` | `true` |
| `product_name_lenght` | `string` | `true` | `varchar` | `true` |
| `product_description_lenght` | `string` | `true` | `varchar` | `true` |
| `product_photos_qty` | `string` | `true` | `varchar` | `true` |
| `product_weight_g` | `string` | `true` | `varchar` | `true` |
| `product_length_cm` | `string` | `true` | `varchar` | `true` |
| `product_height_cm` | `string` | `true` | `varchar` | `true` |
| `product_width_cm` | `string` | `true` | `varchar` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |

### bronze.sellers

- Mức hạt (grain): `One source seller row`
- Khóa: `seller_id`
- Phụ thuộc: `csv:olist_sellers_dataset.csv`
- Partition: `day(_ingestion_timestamp)`
- Số dòng thực tế: `3,095`
- Quy tắc chất lượng: `[{"columns":[],"config":{"rule_type":"row_count"},"name":"row_count_positive","severity":"ERROR","threshold":{"min_value":1}},{"columns":["seller_id","seller_zip_code_prefix","seller_city","seller_state","_ingestion_timestamp","_ingestion_date","_batch_id","_source_file"],"config":{"rule_type":"schema"},"name":"schema_matches_registry","severity":"ERROR","threshold":null},{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["_batch_id","seller_id"],"config":{"column_list":["_batch_id","seller_id"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `seller_id` | `string` | `true` | `varchar` | `true` |
| `seller_zip_code_prefix` | `string` | `true` | `varchar` | `true` |
| `seller_city` | `string` | `true` | `varchar` | `true` |
| `seller_state` | `string` | `true` | `varchar` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |

### control.ingestion_batches

- Mức hạt (grain): `one row per pipeline run, batch, and source file`
- Khóa: `run_id, batch_id, source_file`
- Phụ thuộc: `bronze.*`
- Partition: `không phân vùng`
- Số dòng thực tế: `99`
- Quy tắc chất lượng: `[{"columns":["run_id","batch_id","source_file"],"config":{"unique":true},"name":"declared_key_unique","severity":"ERROR","threshold":null},{"columns":["status"],"config":{"allowed":["STARTED","SUCCESS","FAILED"]},"name":"terminal_status_domain","severity":"ERROR","threshold":null},{"columns":["source_row_count","accepted_count","rejected_count"],"config":{"minimum":0},"name":"non_negative_counts","severity":"ERROR","threshold":{"min_value":0}},{"columns":["started_at","completed_at","duration_ms"],"config":{"unit":"milliseconds"},"name":"duration_matches_timestamps","severity":"WARN","threshold":{"difference_ms":1}},{"columns":["run_id","batch_id","source_file","checksum","source_row_count","source_row_hash","target_committed","accepted_count","rejected_count","status","started_at","completed_at","duration_ms","error_summary"],"config":{"expected_count":14},"name":"structtype_field_count","severity":"ERROR","threshold":{"expected":14}}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `run_id` | `string` | `false` | `varchar` | `false` |
| `batch_id` | `string` | `false` | `varchar` | `false` |
| `source_file` | `string` | `false` | `varchar` | `false` |
| `checksum` | `string` | `false` | `varchar` | `false` |
| `source_row_count` | `bigint` | `false` | `bigint` | `false` |
| `source_row_hash` | `string` | `true` | `varchar` | `true` |
| `target_committed` | `boolean` | `true` | `boolean` | `true` |
| `accepted_count` | `bigint` | `true` | `bigint` | `true` |
| `rejected_count` | `bigint` | `true` | `bigint` | `true` |
| `status` | `string` | `false` | `varchar` | `false` |
| `started_at` | `timestamp` | `false` | `timestamp(6) with time zone` | `false` |
| `completed_at` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `duration_ms` | `bigint` | `true` | `bigint` | `true` |
| `error_summary` | `string` | `true` | `varchar` | `true` |

### control.pipeline_runs

- Mức hạt (grain): `one row per pipeline run attempt`
- Khóa: `run_id`
- Phụ thuộc: `bronze.*, silver.*, gold.*, quality.quality_results`
- Partition: `không phân vùng`
- Số dòng thực tế: `136`
- Quy tắc chất lượng: `[{"columns":["run_id"],"config":{"unique":true},"name":"declared_key_unique","severity":"ERROR","threshold":null},{"columns":["status"],"config":{"allowed":["STARTED","SUCCESS","FAILED"]},"name":"terminal_status_domain","severity":"ERROR","threshold":null},{"columns":["input_count","output_count","rejected_count"],"config":{"minimum":0},"name":"non_negative_counts","severity":"ERROR","threshold":{"min_value":0}},{"columns":["started_at","completed_at","duration_ms"],"config":{"unit":"milliseconds"},"name":"duration_matches_timestamps","severity":"WARN","threshold":{"difference_ms":1}},{"columns":["run_id","logical_run_id","attempt_id","batch_id","job_name","layer","mode","input_count","output_count","rejected_count","status","started_at","completed_at","duration_ms","error_summary","original_status","original_error_summary","resolution_note","resolution_evidence_json","resolved_at"],"config":{"expected_count":20},"name":"structtype_field_count","severity":"ERROR","threshold":{"expected":20}}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `run_id` | `string` | `false` | `varchar` | `false` |
| `logical_run_id` | `string` | `false` | `varchar` | `false` |
| `attempt_id` | `string` | `false` | `varchar` | `false` |
| `batch_id` | `string` | `false` | `varchar` | `false` |
| `job_name` | `string` | `false` | `varchar` | `false` |
| `layer` | `string` | `false` | `varchar` | `false` |
| `mode` | `string` | `false` | `varchar` | `false` |
| `input_count` | `bigint` | `true` | `bigint` | `true` |
| `output_count` | `bigint` | `true` | `bigint` | `true` |
| `rejected_count` | `bigint` | `true` | `bigint` | `true` |
| `status` | `string` | `false` | `varchar` | `false` |
| `started_at` | `timestamp` | `false` | `timestamp(6) with time zone` | `false` |
| `completed_at` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `duration_ms` | `bigint` | `true` | `bigint` | `true` |
| `error_summary` | `string` | `true` | `varchar` | `true` |
| `original_status` | `string` | `true` | `varchar` | `true` |
| `original_error_summary` | `string` | `true` | `varchar` | `true` |
| `resolution_note` | `string` | `true` | `varchar` | `true` |
| `resolution_evidence_json` | `string` | `true` | `varchar` | `true` |
| `resolved_at` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |

### control.transformation_targets

- Mức hạt (grain): `one row per pipeline run and transformation target`
- Khóa: `run_id, target_name`
- Phụ thuộc: `silver.*, gold.*`
- Partition: `không phân vùng`
- Số dòng thực tế: `242`
- Quy tắc chất lượng: `[{"columns":["run_id","target_name"],"config":{"unique":true},"name":"declared_key_unique","severity":"ERROR","threshold":null},{"columns":["status"],"config":{"allowed":["STARTED","SUCCESS","FAILED"]},"name":"terminal_status_domain","severity":"ERROR","threshold":null},{"columns":["input_count","accepted_count","rejected_count"],"config":{"minimum":0},"name":"non_negative_counts","severity":"ERROR","threshold":{"min_value":0}},{"columns":["started_at","completed_at","duration_ms"],"config":{"unit":"milliseconds"},"name":"duration_matches_timestamps","severity":"WARN","threshold":{"difference_ms":1}},{"columns":["run_id","batch_id","target_name","mode","input_count","accepted_count","rejected_count","accepted_committed","quarantine_committed","status","started_at","completed_at","duration_ms","error_summary"],"config":{"expected_count":14},"name":"structtype_field_count","severity":"ERROR","threshold":{"expected":14}}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `run_id` | `string` | `false` | `varchar` | `false` |
| `batch_id` | `string` | `false` | `varchar` | `false` |
| `target_name` | `string` | `false` | `varchar` | `false` |
| `mode` | `string` | `false` | `varchar` | `false` |
| `input_count` | `bigint` | `false` | `bigint` | `false` |
| `accepted_count` | `bigint` | `true` | `bigint` | `true` |
| `rejected_count` | `bigint` | `true` | `bigint` | `true` |
| `accepted_committed` | `boolean` | `false` | `boolean` | `false` |
| `quarantine_committed` | `boolean` | `false` | `boolean` | `false` |
| `status` | `string` | `false` | `varchar` | `false` |
| `started_at` | `timestamp` | `false` | `timestamp(6) with time zone` | `false` |
| `completed_at` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `duration_ms` | `bigint` | `true` | `bigint` | `true` |
| `error_summary` | `string` | `true` | `varchar` | `true` |

### gold.bridge_review_products

- Mức hạt (grain): `one review-order by distinct product`
- Khóa: `review_id, order_id, product_key`
- Phụ thuộc: `silver.order_reviews, silver.order_items, gold.dim_product`
- Partition: `không phân vùng`
- Số dòng thực tế: `102,230`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["review_id","order_id","product_key"],"config":{"column_list":["review_id","order_id","product_key"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `review_id` | `string` | `true` | `varchar` | `true` |
| `order_id` | `string` | `true` | `varchar` | `true` |
| `product_key` | `bigint` | `true` | `bigint` | `true` |

### gold.bridge_review_sellers

- Mức hạt (grain): `one review-order by distinct seller`
- Khóa: `review_id, order_id, seller_key`
- Phụ thuộc: `silver.order_reviews, silver.order_items, gold.dim_seller`
- Partition: `không phân vùng`
- Số dòng thực tế: `99,809`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["review_id","order_id","seller_key"],"config":{"column_list":["review_id","order_id","seller_key"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `review_id` | `string` | `true` | `varchar` | `true` |
| `order_id` | `string` | `true` | `varchar` | `true` |
| `seller_key` | `bigint` | `true` | `bigint` | `true` |

### gold.dim_customer

- Mức hạt (grain): `one real customer`
- Khóa: `customer_key`
- Phụ thuộc: `silver.customers, silver.orders, gold.dim_geography`
- Partition: `không phân vùng`
- Số dòng thực tế: `96,097`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["customer_key"],"config":{"column":"customer_key"},"name":"gx_expect_column_values_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `customer_key` | `bigint` | `true` | `bigint` | `true` |
| `customer_unique_id` | `string` | `true` | `varchar` | `true` |
| `representative_customer_id` | `string` | `true` | `varchar` | `true` |
| `customer_zip_code_prefix` | `string` | `true` | `varchar` | `true` |
| `customer_city` | `string` | `true` | `varchar` | `true` |
| `customer_state` | `string` | `true` | `varchar` | `true` |
| `geography_key` | `bigint` | `true` | `bigint` | `true` |
| `order_count` | `bigint` | `true` | `bigint` | `true` |
| `repeat_customer` | `boolean` | `true` | `boolean` | `true` |

### gold.dim_date

- Mức hạt (grain): `one calendar date`
- Khóa: `date_key`
- Phụ thuộc: `silver.orders, silver.order_items, silver.order_payments, silver.order_reviews`
- Partition: `không phân vùng`
- Số dòng thực tế: `2,047`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["date_key"],"config":{"column":"date_key"},"name":"gx_expect_column_values_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `date_key` | `int` | `true` | `integer` | `true` |
| `full_date` | `date` | `true` | `date` | `true` |
| `day_of_week` | `int` | `true` | `integer` | `true` |
| `day_name` | `string` | `true` | `varchar` | `true` |
| `day_of_month` | `int` | `true` | `integer` | `true` |
| `day_of_year` | `int` | `true` | `integer` | `true` |
| `week_of_year` | `int` | `true` | `integer` | `true` |
| `month` | `int` | `true` | `integer` | `true` |
| `month_name` | `string` | `true` | `varchar` | `true` |
| `quarter` | `int` | `true` | `integer` | `true` |
| `year` | `int` | `true` | `integer` | `true` |
| `is_weekend` | `boolean` | `true` | `boolean` | `true` |

### gold.dim_geography

- Mức hạt (grain): `one ZIP prefix`
- Khóa: `geography_key`
- Phụ thuộc: `silver.geography`
- Partition: `không phân vùng`
- Số dòng thực tế: `19,011`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["geography_key"],"config":{"column":"geography_key"},"name":"gx_expect_column_values_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `geography_key` | `bigint` | `true` | `bigint` | `true` |
| `zip_code_prefix` | `string` | `true` | `varchar` | `true` |
| `city` | `string` | `true` | `varchar` | `true` |
| `state` | `string` | `true` | `varchar` | `true` |
| `region` | `string` | `true` | `varchar` | `true` |
| `latitude` | `decimal(10,7)` | `true` | `decimal(10,7)` | `true` |
| `longitude` | `decimal(10,7)` | `true` | `decimal(10,7)` | `true` |
| `source_observation_count` | `bigint` | `true` | `bigint` | `true` |

### gold.dim_product

- Mức hạt (grain): `one product`
- Khóa: `product_key`
- Phụ thuộc: `silver.products`
- Partition: `không phân vùng`
- Số dòng thực tế: `32,952`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["product_key"],"config":{"column":"product_key"},"name":"gx_expect_column_values_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `product_key` | `bigint` | `true` | `bigint` | `true` |
| `product_id` | `string` | `true` | `varchar` | `true` |
| `product_category_name` | `string` | `true` | `varchar` | `true` |
| `product_category_name_english` | `string` | `true` | `varchar` | `true` |
| `product_name_length` | `int` | `true` | `integer` | `true` |
| `product_description_length` | `int` | `true` | `integer` | `true` |
| `product_photos_qty` | `int` | `true` | `integer` | `true` |
| `product_weight_g` | `decimal(18,3)` | `true` | `decimal(18,3)` | `true` |
| `product_length_cm` | `decimal(18,3)` | `true` | `decimal(18,3)` | `true` |
| `product_height_cm` | `decimal(18,3)` | `true` | `decimal(18,3)` | `true` |
| `product_width_cm` | `decimal(18,3)` | `true` | `decimal(18,3)` | `true` |
| `volume_cm3` | `decimal(38,6)` | `true` | `decimal(38,6)` | `true` |

### gold.dim_seller

- Mức hạt (grain): `one seller`
- Khóa: `seller_key`
- Phụ thuộc: `silver.sellers, gold.dim_geography`
- Partition: `không phân vùng`
- Số dòng thực tế: `3,096`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["seller_key"],"config":{"column":"seller_key"},"name":"gx_expect_column_values_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `seller_key` | `bigint` | `true` | `bigint` | `true` |
| `seller_id` | `string` | `true` | `varchar` | `true` |
| `seller_zip_code_prefix` | `string` | `true` | `varchar` | `true` |
| `seller_city` | `string` | `true` | `varchar` | `true` |
| `seller_state` | `string` | `true` | `varchar` | `true` |
| `geography_key` | `bigint` | `true` | `bigint` | `true` |

### gold.fact_order_items

- Mức hạt (grain): `one line in an order`
- Khóa: `order_id, order_item_id`
- Phụ thuộc: `silver.order_items, silver.orders, silver.customers, gold.dim_customer, gold.dim_product, gold.dim_seller, gold.dim_date`
- Partition: `year(order_date)`
- Số dòng thực tế: `112,650`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["order_id","order_item_id"],"config":{"column_list":["order_id","order_item_id"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `order_id` | `string` | `true` | `varchar` | `true` |
| `order_item_id` | `int` | `true` | `integer` | `true` |
| `customer_key` | `bigint` | `true` | `bigint` | `true` |
| `product_key` | `bigint` | `true` | `bigint` | `true` |
| `seller_key` | `bigint` | `true` | `bigint` | `true` |
| `order_date_key` | `int` | `true` | `integer` | `true` |
| `shipping_limit_date_key` | `int` | `true` | `integer` | `true` |
| `order_date` | `date` | `true` | `date` | `true` |
| `shipping_limit_date` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `price` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `freight_value` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |

### gold.fact_orders

- Mức hạt (grain): `one order`
- Khóa: `order_id`
- Phụ thuộc: `silver.orders, silver.customers, silver.order_items, silver.order_payments, silver.order_reviews, gold.dim_customer, gold.dim_date`
- Partition: `year(purchase_timestamp)`
- Số dòng thực tế: `99,441`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["order_id"],"config":{"column":"order_id"},"name":"gx_expect_column_values_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `order_id` | `string` | `true` | `varchar` | `true` |
| `customer_key` | `bigint` | `true` | `bigint` | `true` |
| `purchase_date_key` | `int` | `true` | `integer` | `true` |
| `approval_date_key` | `int` | `true` | `integer` | `true` |
| `carrier_date_key` | `int` | `true` | `integer` | `true` |
| `delivered_date_key` | `int` | `true` | `integer` | `true` |
| `estimated_delivery_date_key` | `int` | `true` | `integer` | `true` |
| `purchase_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `order_status` | `string` | `true` | `varchar` | `true` |
| `total_items` | `bigint` | `true` | `bigint` | `true` |
| `item_revenue` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `freight_total` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `payment_total` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `delivery_duration_days` | `int` | `true` | `integer` | `true` |
| `carrier_delivery_duration_days` | `int` | `true` | `integer` | `true` |
| `is_late_delivery` | `boolean` | `true` | `boolean` | `true` |
| `average_review_score` | `decimal(4,2)` | `true` | `decimal(4,2)` | `true` |

### gold.fact_payments

- Mức hạt (grain): `one payment component`
- Khóa: `order_id, payment_sequential`
- Phụ thuộc: `silver.order_payments, silver.orders, silver.customers, gold.dim_customer, gold.dim_date`
- Partition: `year(order_date)`
- Số dòng thực tế: `103,886`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["order_id","payment_sequential"],"config":{"column_list":["order_id","payment_sequential"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `order_id` | `string` | `true` | `varchar` | `true` |
| `payment_sequential` | `int` | `true` | `integer` | `true` |
| `customer_key` | `bigint` | `true` | `bigint` | `true` |
| `order_date_key` | `int` | `true` | `integer` | `true` |
| `order_date` | `date` | `true` | `date` | `true` |
| `payment_type` | `string` | `true` | `varchar` | `true` |
| `payment_installments` | `int` | `true` | `integer` | `true` |
| `payment_value` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |

### gold.fact_reviews

- Mức hạt (grain): `one review-order relation`
- Khóa: `review_id, order_id`
- Phụ thuộc: `silver.order_reviews, silver.orders, silver.customers, gold.dim_customer, gold.dim_date`
- Partition: `year(review_date)`
- Số dòng thực tế: `99,224`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["review_id","order_id"],"config":{"column_list":["review_id","order_id"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `review_id` | `string` | `true` | `varchar` | `true` |
| `order_id` | `string` | `true` | `varchar` | `true` |
| `customer_key` | `bigint` | `true` | `bigint` | `true` |
| `review_date_key` | `int` | `true` | `integer` | `true` |
| `answer_date_key` | `int` | `true` | `integer` | `true` |
| `review_date` | `date` | `true` | `date` | `true` |
| `review_answer_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `review_score` | `int` | `true` | `integer` | `true` |
| `review_comment_title` | `string` | `true` | `varchar` | `true` |
| `review_comment_message` | `string` | `true` | `varchar` | `true` |
| `has_title` | `boolean` | `true` | `boolean` | `true` |
| `has_message` | `boolean` | `true` | `boolean` | `true` |

### gold.mart_daily_revenue

- Mức hạt (grain): `one date by customer state by product category`
- Khóa: `revenue_date, customer_state, product_category`
- Phụ thuộc: `gold.fact_order_items, gold.dim_customer, gold.dim_product`
- Partition: `year(revenue_date)`
- Số dòng thực tế: `56,949`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["revenue_date","customer_state","product_category"],"config":{"column_list":["revenue_date","customer_state","product_category"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `revenue_date` | `date` | `true` | `date` | `true` |
| `customer_state` | `string` | `true` | `varchar` | `true` |
| `product_category` | `string` | `true` | `varchar` | `true` |
| `item_revenue` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `freight_total` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `item_count` | `bigint` | `true` | `bigint` | `true` |
| `order_count` | `bigint` | `true` | `bigint` | `true` |
| `customer_count` | `bigint` | `true` | `bigint` | `true` |

### gold.mart_product_performance

- Mức hạt (grain): `one calendar month by product category`
- Khóa: `month_start, product_category`
- Phụ thuộc: `gold.fact_order_items, gold.fact_reviews, gold.bridge_review_products, gold.dim_product`
- Partition: `year(month_start)`
- Số dòng thực tế: `1,274`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["month_start","product_category"],"config":{"column_list":["month_start","product_category"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `month_start` | `date` | `true` | `date` | `true` |
| `product_category` | `string` | `true` | `varchar` | `true` |
| `item_revenue` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `freight_total` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `item_count` | `bigint` | `true` | `bigint` | `true` |
| `order_count` | `bigint` | `true` | `bigint` | `true` |
| `review_count` | `bigint` | `true` | `bigint` | `true` |
| `average_review_score` | `decimal(4,2)` | `true` | `decimal(4,2)` | `true` |

### gold.mart_seller_performance

- Mức hạt (grain): `one calendar month by seller`
- Khóa: `month_start, seller_key`
- Phụ thuộc: `gold.fact_order_items, gold.fact_orders, gold.fact_reviews, gold.bridge_review_sellers, gold.dim_seller`
- Partition: `year(month_start)`
- Số dòng thực tế: `16,441`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["month_start","seller_key"],"config":{"column_list":["month_start","seller_key"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `month_start` | `date` | `true` | `date` | `true` |
| `seller_key` | `bigint` | `true` | `bigint` | `true` |
| `seller_id` | `string` | `true` | `varchar` | `true` |
| `seller_state` | `string` | `true` | `varchar` | `true` |
| `item_revenue` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `freight_total` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `item_count` | `bigint` | `true` | `bigint` | `true` |
| `order_count` | `bigint` | `true` | `bigint` | `true` |
| `average_delivery_days` | `decimal(10,2)` | `true` | `decimal(10,2)` | `true` |
| `on_time_delivery_rate` | `decimal(5,4)` | `true` | `decimal(5,4)` | `true` |
| `review_count` | `bigint` | `true` | `bigint` | `true` |
| `average_review_score` | `decimal(4,2)` | `true` | `decimal(4,2)` | `true` |

### quality.quality_results

- Mức hạt (grain): `one quality result per run, layer, table, and rule`
- Khóa: `run_id, layer, table_name, rule_name`
- Phụ thuộc: `data_quality/suites/bronze.json, data_quality/suites/silver.json, data_quality/suites/gold.json, bronze.*, silver.*, gold.*`
- Partition: `day(checked_at)`
- Số dòng thực tế: `2,052`
- Quy tắc chất lượng: `[{"columns":["run_id","layer","table_name","rule_name"],"config":{"unique":true},"name":"unique_quality_grain","severity":"ERROR","threshold":null},{"columns":["severity","status"],"config":{"severity":["INFO","WARN","ERROR"],"status":["PASS","INFO","WARN","ERROR"]},"name":"known_severity_status_domain","severity":"ERROR","threshold":null},{"columns":["layer","table_name","rule_name","severity","observed_value","threshold","status","run_id","checked_at"],"config":{"expected_count":9},"name":"structtype_field_count","severity":"ERROR","threshold":{"expected":9}},{"columns":["severity","status"],"config":{"filter":"severity=ERROR AND status<>PASS"},"name":"zero_failed_error_rules","severity":"ERROR","threshold":{"max_count":0}}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `layer` | `string` | `true` | `varchar` | `true` |
| `table_name` | `string` | `true` | `varchar` | `true` |
| `rule_name` | `string` | `true` | `varchar` | `true` |
| `severity` | `string` | `true` | `varchar` | `true` |
| `observed_value` | `string` | `true` | `varchar` | `true` |
| `threshold` | `string` | `true` | `varchar` | `true` |
| `status` | `string` | `true` | `varchar` | `true` |
| `run_id` | `string` | `true` | `varchar` | `true` |
| `checked_at` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |

### quarantine.geography

- Mức hạt (grain): `one rejected one Brazilian ZIP prefix`
- Khóa: `_batch_id, zip_code_prefix`
- Phụ thuộc: `bronze.geolocation`
- Partition: `không phân vùng`
- Số dòng thực tế: `42`
- Quy tắc chất lượng: `[{"columns":["_error_codes"],"config":{"allow_empty":false},"name":"nonempty_error_codes","severity":"ERROR","threshold":null},{"columns":[],"config":{"scope":"silver.geography"},"name":"accepted_rejected_count_reconciliation","severity":"ERROR","threshold":{"difference":0}}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |
| `zip_code_prefix` | `string` | `true` | `varchar` | `true` |
| `latitude` | `decimal(10,7)` | `true` | `decimal(10,7)` | `true` |
| `longitude` | `decimal(10,7)` | `true` | `decimal(10,7)` | `true` |
| `city` | `string` | `true` | `varchar` | `true` |
| `state` | `string` | `true` | `varchar` | `true` |
| `_error_codes` | `array<string>` | `true` | `array(varchar)` | `true` |
| `_rejected_at` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |

### quarantine.orders

- Mức hạt (grain): `one rejected one order`
- Khóa: `_batch_id, order_id`
- Phụ thuộc: `bronze.orders`
- Partition: `không phân vùng`
- Số dòng thực tế: `197`
- Quy tắc chất lượng: `[{"columns":["_error_codes"],"config":{"allow_empty":false},"name":"nonempty_error_codes","severity":"ERROR","threshold":null},{"columns":[],"config":{"scope":"silver.orders"},"name":"accepted_rejected_count_reconciliation","severity":"ERROR","threshold":{"difference":0}}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `order_id` | `string` | `true` | `varchar` | `true` |
| `customer_id` | `string` | `true` | `varchar` | `true` |
| `order_status` | `string` | `true` | `varchar` | `true` |
| `order_purchase_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `order_approved_at` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `order_delivered_carrier_date` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `order_delivered_customer_date` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `order_estimated_delivery_date` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |
| `delivery_duration_days` | `int` | `true` | `integer` | `true` |
| `carrier_delivery_duration_days` | `int` | `true` | `integer` | `true` |
| `is_late_delivery` | `boolean` | `true` | `boolean` | `true` |
| `_error_codes` | `array<string>` | `true` | `array(varchar)` | `true` |
| `_rejected_at` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |

### silver.customers

- Mức hạt (grain): `one order-level customer identity`
- Khóa: `customer_id`
- Phụ thuộc: `bronze.customers`
- Partition: `không phân vùng`
- Số dòng thực tế: `99,441`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["customer_id"],"config":{"column":"customer_id"},"name":"gx_expect_column_values_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `customer_id` | `string` | `true` | `varchar` | `true` |
| `customer_unique_id` | `string` | `true` | `varchar` | `true` |
| `customer_zip_code_prefix` | `string` | `true` | `varchar` | `true` |
| `customer_city` | `string` | `true` | `varchar` | `true` |
| `customer_state` | `string` | `true` | `varchar` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |

### silver.geography

- Mức hạt (grain): `one Brazilian ZIP prefix`
- Khóa: `zip_code_prefix`
- Phụ thuộc: `bronze.geolocation`
- Partition: `không phân vùng`
- Số dòng thực tế: `19,010`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["zip_code_prefix"],"config":{"column":"zip_code_prefix"},"name":"gx_expect_column_values_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `zip_code_prefix` | `string` | `true` | `varchar` | `true` |
| `latitude` | `decimal(10,7)` | `true` | `decimal(10,7)` | `true` |
| `longitude` | `decimal(10,7)` | `true` | `decimal(10,7)` | `true` |
| `source_observation_count` | `bigint` | `true` | `bigint` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |
| `city` | `string` | `true` | `varchar` | `true` |
| `state` | `string` | `true` | `varchar` | `true` |

### silver.order_items

- Mức hạt (grain): `one line in an order`
- Khóa: `order_id, order_item_id`
- Phụ thuộc: `bronze.order_items, bronze.orders`
- Partition: `year(order_purchase_timestamp)`
- Số dòng thực tế: `112,650`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["order_id","order_item_id"],"config":{"column_list":["order_id","order_item_id"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `order_id` | `string` | `true` | `varchar` | `true` |
| `order_item_id` | `int` | `true` | `integer` | `true` |
| `product_id` | `string` | `true` | `varchar` | `true` |
| `seller_id` | `string` | `true` | `varchar` | `true` |
| `shipping_limit_date` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `price` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `freight_value` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |
| `order_purchase_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |

### silver.order_payments

- Mức hạt (grain): `one payment component of an order`
- Khóa: `order_id, payment_sequential`
- Phụ thuộc: `bronze.order_payments, bronze.orders`
- Partition: `year(order_purchase_timestamp)`
- Số dòng thực tế: `103,886`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["order_id","payment_sequential"],"config":{"column_list":["order_id","payment_sequential"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `order_id` | `string` | `true` | `varchar` | `true` |
| `payment_sequential` | `int` | `true` | `integer` | `true` |
| `payment_type` | `string` | `true` | `varchar` | `true` |
| `payment_installments` | `int` | `true` | `integer` | `true` |
| `payment_value` | `decimal(18,2)` | `true` | `decimal(18,2)` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |
| `order_purchase_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |

### silver.order_reviews

- Mức hạt (grain): `one review linked to one order`
- Khóa: `review_id, order_id`
- Phụ thuộc: `bronze.order_reviews`
- Partition: `year(review_creation_date)`
- Số dòng thực tế: `99,224`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["review_score"],"config":{"column":"review_score","max_value":5,"min_value":1},"name":"gx_expect_column_values_to_be_between","severity":"ERROR","threshold":{"max_value":5,"min_value":1}},{"columns":["review_id","order_id"],"config":{"column_list":["review_id","order_id"]},"name":"gx_expect_compound_columns_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `review_id` | `string` | `true` | `varchar` | `true` |
| `order_id` | `string` | `true` | `varchar` | `true` |
| `review_score` | `int` | `true` | `integer` | `true` |
| `review_comment_title` | `string` | `true` | `varchar` | `true` |
| `review_comment_message` | `string` | `true` | `varchar` | `true` |
| `review_creation_date` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `review_answer_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |
| `has_title` | `boolean` | `true` | `boolean` | `true` |
| `has_message` | `boolean` | `true` | `boolean` | `true` |

### silver.orders

- Mức hạt (grain): `one order`
- Khóa: `order_id`
- Phụ thuộc: `bronze.orders`
- Partition: `year(order_purchase_timestamp)`
- Số dòng thực tế: `99,244`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["order_status"],"config":{"column":"order_status","value_set":["approved","canceled","created","delivered","invoiced","processing","shipped","unavailable"]},"name":"gx_expect_column_values_to_be_in_set","severity":"ERROR","threshold":null},{"columns":["order_id"],"config":{"column":"order_id"},"name":"gx_expect_column_values_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `order_id` | `string` | `true` | `varchar` | `true` |
| `customer_id` | `string` | `true` | `varchar` | `true` |
| `order_status` | `string` | `true` | `varchar` | `true` |
| `order_purchase_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `order_approved_at` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `order_delivered_carrier_date` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `order_delivered_customer_date` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `order_estimated_delivery_date` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |
| `delivery_duration_days` | `int` | `true` | `integer` | `true` |
| `carrier_delivery_duration_days` | `int` | `true` | `integer` | `true` |
| `is_late_delivery` | `boolean` | `true` | `boolean` | `true` |

### silver.products

- Mức hạt (grain): `one product`
- Khóa: `product_id`
- Phụ thuộc: `bronze.products, bronze.product_category_translation`
- Partition: `không phân vùng`
- Số dòng thực tế: `32,951`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["product_id"],"config":{"column":"product_id"},"name":"gx_expect_column_values_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `product_id` | `string` | `true` | `varchar` | `true` |
| `product_category_name` | `string` | `true` | `varchar` | `true` |
| `product_name_length` | `int` | `true` | `integer` | `true` |
| `product_description_length` | `int` | `true` | `integer` | `true` |
| `product_photos_qty` | `int` | `true` | `integer` | `true` |
| `product_weight_g` | `decimal(18,3)` | `true` | `decimal(18,3)` | `true` |
| `product_length_cm` | `decimal(18,3)` | `true` | `decimal(18,3)` | `true` |
| `product_height_cm` | `decimal(18,3)` | `true` | `decimal(18,3)` | `true` |
| `product_width_cm` | `decimal(18,3)` | `true` | `decimal(18,3)` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |
| `product_category_name_english` | `string` | `true` | `varchar` | `true` |
| `volume_cm3` | `decimal(38,6)` | `true` | `decimal(38,6)` | `true` |

### silver.sellers

- Mức hạt (grain): `one seller`
- Khóa: `seller_id`
- Phụ thuộc: `bronze.sellers`
- Partition: `không phân vùng`
- Số dòng thực tế: `3,095`
- Quy tắc chất lượng: `[{"columns":[],"config":{"min_value":1},"name":"gx_expect_table_row_count_to_be_between","severity":"ERROR","threshold":{"min_value":1}},{"columns":["seller_id"],"config":{"column":"seller_id"},"name":"gx_expect_column_values_to_be_unique","severity":"ERROR","threshold":null}]`

| Cột | Kiểu Spark | Spark nullable | Kiểu Trino | Trino nullable |
|---|---|---|---|---|
| `seller_id` | `string` | `true` | `varchar` | `true` |
| `seller_zip_code_prefix` | `string` | `true` | `varchar` | `true` |
| `seller_city` | `string` | `true` | `varchar` | `true` |
| `seller_state` | `string` | `true` | `varchar` | `true` |
| `_ingestion_timestamp` | `timestamp` | `true` | `timestamp(6) with time zone` | `true` |
| `_ingestion_date` | `date` | `true` | `date` | `true` |
| `_batch_id` | `string` | `true` | `varchar` | `true` |
| `_source_file` | `string` | `true` | `varchar` | `true` |

