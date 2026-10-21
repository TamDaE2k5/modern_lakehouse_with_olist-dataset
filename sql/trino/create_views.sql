CREATE OR REPLACE VIEW lakehouse.gold.vw_order_summary AS
SELECT
    o.order_id,
    o.purchase_timestamp,
    CAST(o.purchase_timestamp AS DATE) AS purchase_date,
    o.order_status,
    o.customer_key,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    c.repeat_customer,
    o.total_items,
    o.item_revenue,
    o.freight_total,
    o.payment_total,
    o.delivery_duration_days,
    o.carrier_delivery_duration_days,
    o.is_late_delivery,
    o.average_review_score
FROM lakehouse.gold.fact_orders o
LEFT JOIN lakehouse.gold.dim_customer c ON o.customer_key = c.customer_key;

CREATE OR REPLACE VIEW lakehouse.gold.vw_order_lines AS
SELECT
    i.order_id,
    i.order_item_id,
    i.order_date,
    i.shipping_limit_date,
    i.customer_key,
    c.customer_unique_id,
    c.customer_state,
    i.product_key,
    p.product_id,
    p.product_category_name_english AS product_category,
    i.seller_key,
    s.seller_id,
    s.seller_state,
    i.price,
    i.freight_value
FROM lakehouse.gold.fact_order_items i
LEFT JOIN lakehouse.gold.dim_customer c ON i.customer_key = c.customer_key
LEFT JOIN lakehouse.gold.dim_product p ON i.product_key = p.product_key
LEFT JOIN lakehouse.gold.dim_seller s ON i.seller_key = s.seller_key;

CREATE OR REPLACE VIEW lakehouse.gold.vw_payment_summary AS
SELECT
    p.order_id,
    p.payment_sequential,
    p.order_date,
    p.customer_key,
    c.customer_unique_id,
    c.customer_state,
    p.payment_type,
    p.payment_installments,
    p.payment_value
FROM lakehouse.gold.fact_payments p
LEFT JOIN lakehouse.gold.dim_customer c ON p.customer_key = c.customer_key;

CREATE OR REPLACE VIEW lakehouse.gold.vw_review_summary AS
SELECT
    r.review_id,
    r.order_id,
    r.review_date,
    r.review_answer_timestamp,
    r.customer_key,
    c.customer_unique_id,
    c.customer_state,
    r.review_score,
    r.has_title,
    r.has_message
FROM lakehouse.gold.fact_reviews r
LEFT JOIN lakehouse.gold.dim_customer c ON r.customer_key = c.customer_key;
