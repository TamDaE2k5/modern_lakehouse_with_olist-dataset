-- Each statement is an executable assertion. The Trino CLI exits non-zero on failure.
WITH item_totals AS (
    SELECT
        order_id,
        COUNT(*) AS item_count,
        SUM(price) AS item_revenue,
        SUM(freight_value) AS freight_total
    FROM lakehouse.gold.fact_order_items
    GROUP BY order_id
), duplicate_orders AS (
    SELECT order_id FROM lakehouse.gold.fact_orders GROUP BY order_id HAVING COUNT(*) <> 1
), duplicate_items AS (
    SELECT order_id, order_item_id
    FROM lakehouse.gold.fact_order_items
    GROUP BY order_id, order_item_id
    HAVING COUNT(*) <> 1
), violations AS (
    SELECT COALESCE(o.order_id, i.order_id) AS order_id
    FROM lakehouse.gold.fact_orders o
    FULL OUTER JOIN item_totals i ON o.order_id = i.order_id
    WHERE o.order_id IS NULL
       OR COALESCE(o.total_items, 0) <> COALESCE(i.item_count, 0)
       OR COALESCE(o.item_revenue, DECIMAL '0.00')
          IS DISTINCT FROM COALESCE(i.item_revenue, DECIMAL '0.00')
       OR COALESCE(o.freight_total, DECIMAL '0.00')
          IS DISTINCT FROM COALESCE(i.freight_total, DECIMAL '0.00')
    UNION ALL SELECT order_id FROM duplicate_orders
    UNION ALL SELECT order_id FROM duplicate_items
)
SELECT IF(
    COUNT(*) = 0,
    'PASS revenue_order_item',
    fail(format('revenue_order_item reconciliation failed: violations=%s', COUNT(*)))
) AS revenue_order_item
FROM violations;

WITH payment_totals AS (
    SELECT order_id, SUM(payment_value) AS payment_total
    FROM lakehouse.gold.fact_payments
    GROUP BY order_id
), duplicate_payments AS (
    SELECT order_id
    FROM lakehouse.gold.fact_payments
    GROUP BY order_id, payment_sequential
    HAVING COUNT(*) <> 1
), violations AS (
    SELECT COALESCE(o.order_id, p.order_id) AS order_id
    FROM lakehouse.gold.fact_orders o
    FULL OUTER JOIN payment_totals p ON o.order_id = p.order_id
    WHERE o.order_id IS NULL
       OR COALESCE(o.payment_total, DECIMAL '0.00')
          IS DISTINCT FROM COALESCE(p.payment_total, DECIMAL '0.00')
    UNION ALL SELECT order_id FROM duplicate_payments
)
SELECT IF(
    COUNT(*) = 0,
    'PASS payment',
    fail(format('payment reconciliation failed: violations=%s', COUNT(*)))
) AS payment
FROM violations;

WITH expected AS (
    SELECT DISTINCT r.review_id, r.order_id, i.product_key
    FROM lakehouse.gold.fact_reviews r
    JOIN lakehouse.gold.fact_order_items i ON r.order_id = i.order_id
), missing_or_extra AS (
    (SELECT review_id, order_id, product_key FROM expected
     EXCEPT ALL
     SELECT review_id, order_id, product_key FROM lakehouse.gold.bridge_review_products)
    UNION ALL
    (SELECT review_id, order_id, product_key FROM lakehouse.gold.bridge_review_products
     EXCEPT ALL
     SELECT review_id, order_id, product_key FROM expected)
), duplicate_grain AS (
    SELECT review_id, order_id, product_key
    FROM lakehouse.gold.bridge_review_products
    GROUP BY review_id, order_id, product_key
    HAVING COUNT(*) <> 1
), violations AS (
    SELECT review_id, order_id, product_key FROM missing_or_extra
    UNION ALL
    SELECT review_id, order_id, product_key FROM duplicate_grain
)
SELECT IF(
    COUNT(*) = 0,
    'PASS review_product_bridge',
    fail(format('review_product_bridge reconciliation failed: violations=%s', COUNT(*)))
) AS review_product_bridge
FROM violations;

WITH expected AS (
    SELECT DISTINCT r.review_id, r.order_id, i.seller_key
    FROM lakehouse.gold.fact_reviews r
    JOIN lakehouse.gold.fact_order_items i ON r.order_id = i.order_id
), missing_or_extra AS (
    (SELECT review_id, order_id, seller_key FROM expected
     EXCEPT ALL
     SELECT review_id, order_id, seller_key FROM lakehouse.gold.bridge_review_sellers)
    UNION ALL
    (SELECT review_id, order_id, seller_key FROM lakehouse.gold.bridge_review_sellers
     EXCEPT ALL
     SELECT review_id, order_id, seller_key FROM expected)
), duplicate_grain AS (
    SELECT review_id, order_id, seller_key
    FROM lakehouse.gold.bridge_review_sellers
    GROUP BY review_id, order_id, seller_key
    HAVING COUNT(*) <> 1
), violations AS (
    SELECT review_id, order_id, seller_key FROM missing_or_extra
    UNION ALL
    SELECT review_id, order_id, seller_key FROM duplicate_grain
)
SELECT IF(
    COUNT(*) = 0,
    'PASS review_seller_bridge',
    fail(format('review_seller_bridge reconciliation failed: violations=%s', COUNT(*)))
) AS review_seller_bridge
FROM violations;

WITH unknown_member_violations AS (
    SELECT 'dim_date' AS relation_name, 'date_key_zero_count' AS column_name,
           CAST(COUNT_IF(date_key = 0) AS VARCHAR) AS key_value
    FROM lakehouse.gold.dim_date HAVING COUNT_IF(date_key = 0) <> 1
    UNION ALL
    SELECT 'dim_geography', 'geography_key_zero_count', CAST(COUNT_IF(geography_key = 0) AS VARCHAR)
    FROM lakehouse.gold.dim_geography HAVING COUNT_IF(geography_key = 0) <> 1
    UNION ALL
    SELECT 'dim_customer', 'customer_key_zero_count', CAST(COUNT_IF(customer_key = 0) AS VARCHAR)
    FROM lakehouse.gold.dim_customer HAVING COUNT_IF(customer_key = 0) <> 1
    UNION ALL
    SELECT 'dim_product', 'product_key_zero_count', CAST(COUNT_IF(product_key = 0) AS VARCHAR)
    FROM lakehouse.gold.dim_product HAVING COUNT_IF(product_key = 0) <> 1
    UNION ALL
    SELECT 'dim_seller', 'seller_key_zero_count', CAST(COUNT_IF(seller_key = 0) AS VARCHAR)
    FROM lakehouse.gold.dim_seller HAVING COUNT_IF(seller_key = 0) <> 1
), foreign_key_violations AS (
    SELECT 'dim_customer' AS relation_name, 'geography_key' AS column_name,
           CAST(x.geography_key AS VARCHAR) AS key_value
    FROM lakehouse.gold.dim_customer x
    LEFT JOIN lakehouse.gold.dim_geography d ON x.geography_key = d.geography_key
    WHERE d.geography_key IS NULL
    UNION ALL
    SELECT 'dim_seller', 'geography_key', CAST(x.geography_key AS VARCHAR)
    FROM lakehouse.gold.dim_seller x
    LEFT JOIN lakehouse.gold.dim_geography d ON x.geography_key = d.geography_key
    WHERE d.geography_key IS NULL
    UNION ALL
    SELECT 'fact_orders', 'customer_key', CAST(x.customer_key AS VARCHAR)
    FROM lakehouse.gold.fact_orders x
    LEFT JOIN lakehouse.gold.dim_customer d ON x.customer_key = d.customer_key
    WHERE d.customer_key IS NULL
    UNION ALL
    SELECT 'fact_orders', 'purchase_date_key', CAST(x.purchase_date_key AS VARCHAR)
    FROM lakehouse.gold.fact_orders x LEFT JOIN lakehouse.gold.dim_date d ON x.purchase_date_key = d.date_key
    WHERE d.date_key IS NULL
    UNION ALL
    SELECT 'fact_orders', 'approval_date_key', CAST(x.approval_date_key AS VARCHAR)
    FROM lakehouse.gold.fact_orders x LEFT JOIN lakehouse.gold.dim_date d ON x.approval_date_key = d.date_key
    WHERE d.date_key IS NULL
    UNION ALL
    SELECT 'fact_orders', 'carrier_date_key', CAST(x.carrier_date_key AS VARCHAR)
    FROM lakehouse.gold.fact_orders x LEFT JOIN lakehouse.gold.dim_date d ON x.carrier_date_key = d.date_key
    WHERE d.date_key IS NULL
    UNION ALL
    SELECT 'fact_orders', 'delivered_date_key', CAST(x.delivered_date_key AS VARCHAR)
    FROM lakehouse.gold.fact_orders x LEFT JOIN lakehouse.gold.dim_date d ON x.delivered_date_key = d.date_key
    WHERE d.date_key IS NULL
    UNION ALL
    SELECT 'fact_orders', 'estimated_delivery_date_key', CAST(x.estimated_delivery_date_key AS VARCHAR)
    FROM lakehouse.gold.fact_orders x LEFT JOIN lakehouse.gold.dim_date d ON x.estimated_delivery_date_key = d.date_key
    WHERE d.date_key IS NULL
    UNION ALL
    SELECT 'fact_order_items', 'customer_key', CAST(x.customer_key AS VARCHAR)
    FROM lakehouse.gold.fact_order_items x LEFT JOIN lakehouse.gold.dim_customer d ON x.customer_key = d.customer_key
    WHERE d.customer_key IS NULL
    UNION ALL
    SELECT 'fact_order_items', 'product_key', CAST(x.product_key AS VARCHAR)
    FROM lakehouse.gold.fact_order_items x LEFT JOIN lakehouse.gold.dim_product d ON x.product_key = d.product_key
    WHERE d.product_key IS NULL
    UNION ALL
    SELECT 'fact_order_items', 'seller_key', CAST(x.seller_key AS VARCHAR)
    FROM lakehouse.gold.fact_order_items x LEFT JOIN lakehouse.gold.dim_seller d ON x.seller_key = d.seller_key
    WHERE d.seller_key IS NULL
    UNION ALL
    SELECT 'fact_order_items', 'order_date_key', CAST(x.order_date_key AS VARCHAR)
    FROM lakehouse.gold.fact_order_items x LEFT JOIN lakehouse.gold.dim_date d ON x.order_date_key = d.date_key
    WHERE d.date_key IS NULL
    UNION ALL
    SELECT 'fact_order_items', 'shipping_limit_date_key', CAST(x.shipping_limit_date_key AS VARCHAR)
    FROM lakehouse.gold.fact_order_items x LEFT JOIN lakehouse.gold.dim_date d ON x.shipping_limit_date_key = d.date_key
    WHERE d.date_key IS NULL
    UNION ALL
    SELECT 'fact_payments', 'customer_key', CAST(x.customer_key AS VARCHAR)
    FROM lakehouse.gold.fact_payments x LEFT JOIN lakehouse.gold.dim_customer d ON x.customer_key = d.customer_key
    WHERE d.customer_key IS NULL
    UNION ALL
    SELECT 'fact_payments', 'order_date_key', CAST(x.order_date_key AS VARCHAR)
    FROM lakehouse.gold.fact_payments x LEFT JOIN lakehouse.gold.dim_date d ON x.order_date_key = d.date_key
    WHERE d.date_key IS NULL
    UNION ALL
    SELECT 'fact_reviews', 'customer_key', CAST(x.customer_key AS VARCHAR)
    FROM lakehouse.gold.fact_reviews x LEFT JOIN lakehouse.gold.dim_customer d ON x.customer_key = d.customer_key
    WHERE d.customer_key IS NULL
    UNION ALL
    SELECT 'fact_reviews', 'review_date_key', CAST(x.review_date_key AS VARCHAR)
    FROM lakehouse.gold.fact_reviews x LEFT JOIN lakehouse.gold.dim_date d ON x.review_date_key = d.date_key
    WHERE d.date_key IS NULL
    UNION ALL
    SELECT 'fact_reviews', 'answer_date_key', CAST(x.answer_date_key AS VARCHAR)
    FROM lakehouse.gold.fact_reviews x LEFT JOIN lakehouse.gold.dim_date d ON x.answer_date_key = d.date_key
    WHERE d.date_key IS NULL
    UNION ALL
    SELECT 'bridge_review_products', 'product_key', CAST(x.product_key AS VARCHAR)
    FROM lakehouse.gold.bridge_review_products x LEFT JOIN lakehouse.gold.dim_product d ON x.product_key = d.product_key
    WHERE d.product_key IS NULL
    UNION ALL
    SELECT 'bridge_review_sellers', 'seller_key', CAST(x.seller_key AS VARCHAR)
    FROM lakehouse.gold.bridge_review_sellers x LEFT JOIN lakehouse.gold.dim_seller d ON x.seller_key = d.seller_key
    WHERE d.seller_key IS NULL
    UNION ALL
    SELECT 'mart_seller_performance', 'seller_key', CAST(x.seller_key AS VARCHAR)
    FROM lakehouse.gold.mart_seller_performance x LEFT JOIN lakehouse.gold.dim_seller d ON x.seller_key = d.seller_key
    WHERE d.seller_key IS NULL
), order_relation_violations AS (
    SELECT 'fact_order_items' AS relation_name, 'order_id' AS column_name, x.order_id AS key_value
    FROM lakehouse.gold.fact_order_items x LEFT JOIN lakehouse.gold.fact_orders o ON x.order_id = o.order_id
    WHERE o.order_id IS NULL
    UNION ALL
    SELECT 'fact_payments', 'order_id', x.order_id
    FROM lakehouse.gold.fact_payments x LEFT JOIN lakehouse.gold.fact_orders o ON x.order_id = o.order_id
    WHERE o.order_id IS NULL
    UNION ALL
    SELECT 'fact_reviews', 'order_id', x.order_id
    FROM lakehouse.gold.fact_reviews x LEFT JOIN lakehouse.gold.fact_orders o ON x.order_id = o.order_id
    WHERE o.order_id IS NULL
    UNION ALL
    SELECT 'bridge_review_products', 'review_order', CONCAT(x.review_id, '/', x.order_id)
    FROM lakehouse.gold.bridge_review_products x
    LEFT JOIN lakehouse.gold.fact_reviews r ON x.review_id = r.review_id AND x.order_id = r.order_id
    WHERE r.review_id IS NULL
    UNION ALL
    SELECT 'bridge_review_sellers', 'review_order', CONCAT(x.review_id, '/', x.order_id)
    FROM lakehouse.gold.bridge_review_sellers x
    LEFT JOIN lakehouse.gold.fact_reviews r ON x.review_id = r.review_id AND x.order_id = r.order_id
    WHERE r.review_id IS NULL
), violations AS (
    SELECT relation_name, column_name, key_value FROM unknown_member_violations
    UNION ALL
    SELECT relation_name, column_name, key_value FROM foreign_key_violations
    UNION ALL
    SELECT relation_name, column_name, key_value FROM order_relation_violations
), summary AS (
    SELECT
        COUNT(*) AS n,
        ARRAY_JOIN(
            SLICE(
                ARRAY_AGG(
                    FORMAT('%s.%s=%s', relation_name, column_name, key_value)
                    ORDER BY relation_name, column_name, key_value
                ),
                1,
                10
            ),
            ', '
        ) AS sample
    FROM violations
)
SELECT IF(
    n = 0,
    'PASS unknown_key',
    fail(format('unknown_key reconciliation failed: violations=%s sample=%s', n, COALESCE(sample, 'none')))
) AS unknown_key
FROM summary;

WITH expected AS (
    SELECT
        i.order_date AS revenue_date,
        COALESCE(c.customer_state, 'Unknown') AS customer_state,
        COALESCE(p.product_category_name_english, 'unknown') AS product_category,
        SUM(i.price) AS item_revenue,
        SUM(i.freight_value) AS freight_total,
        COUNT(*) AS item_count,
        COUNT(DISTINCT i.order_id) AS order_count,
        COUNT(DISTINCT i.customer_key) AS customer_count
    FROM lakehouse.gold.fact_order_items i
    LEFT JOIN lakehouse.gold.dim_customer c ON i.customer_key = c.customer_key
    LEFT JOIN lakehouse.gold.dim_product p ON i.product_key = p.product_key
    GROUP BY i.order_date, COALESCE(c.customer_state, 'Unknown'),
             COALESCE(p.product_category_name_english, 'unknown')
), violations AS (
    SELECT COALESCE(e.revenue_date, a.revenue_date) AS revenue_date
    FROM expected e
    FULL OUTER JOIN lakehouse.gold.mart_daily_revenue a
      ON e.revenue_date IS NOT DISTINCT FROM a.revenue_date
     AND e.customer_state = a.customer_state
     AND e.product_category = a.product_category
    WHERE e.item_count IS NULL OR a.item_count IS NULL
       OR e.item_revenue IS DISTINCT FROM a.item_revenue
       OR e.freight_total IS DISTINCT FROM a.freight_total
       OR e.item_count IS DISTINCT FROM a.item_count
       OR e.order_count IS DISTINCT FROM a.order_count
       OR e.customer_count IS DISTINCT FROM a.customer_count
)
SELECT IF(
    COUNT(*) = 0,
    'PASS daily_revenue_mart',
    fail(format('daily_revenue_mart reconciliation failed: violations=%s', COUNT(*)))
) AS daily_revenue_mart
FROM violations;

WITH sellers AS (
    SELECT seller_key, seller_id, seller_state FROM lakehouse.gold.dim_seller
), revenue AS (
    SELECT
        CAST(DATE_TRUNC('month', order_date) AS DATE) AS month_start,
        seller_key,
        SUM(price) AS item_revenue,
        SUM(freight_value) AS freight_total,
        COUNT(*) AS item_count,
        COUNT(DISTINCT order_id) AS order_count
    FROM lakehouse.gold.fact_order_items
    GROUP BY CAST(DATE_TRUNC('month', order_date) AS DATE), seller_key
), seller_orders AS (
    SELECT DISTINCT
        CAST(DATE_TRUNC('month', order_date) AS DATE) AS month_start,
        seller_key,
        order_id
    FROM lakehouse.gold.fact_order_items
), delivery AS (
    SELECT
        so.month_start,
        so.seller_key,
        CAST(AVG(CAST(o.delivery_duration_days AS DECIMAL(10,2))) AS DECIMAL(10,2))
            AS average_delivery_days,
        CAST(
            AVG(
                CASE WHEN o.delivery_duration_days IS NOT NULL
                     THEN IF(NOT COALESCE(o.is_late_delivery, FALSE), 1, 0)
                END
            ) AS DECIMAL(5,4)
        ) AS on_time_delivery_rate
    FROM seller_orders so
    LEFT JOIN lakehouse.gold.fact_orders o ON so.order_id = o.order_id
    GROUP BY so.month_start, so.seller_key
), review_relations AS (
    SELECT DISTINCT
        so.month_start,
        so.seller_key,
        r.review_id,
        r.order_id,
        r.review_score
    FROM lakehouse.gold.bridge_review_sellers b
    JOIN lakehouse.gold.fact_reviews r ON b.review_id = r.review_id AND b.order_id = r.order_id
    JOIN seller_orders so ON b.order_id = so.order_id AND b.seller_key = so.seller_key
), review_metrics AS (
    SELECT
        month_start,
        seller_key,
        COUNT(DISTINCT ROW(review_id, order_id)) AS review_count,
        CAST(AVG(CAST(review_score AS DECIMAL(4,2))) AS DECIMAL(4,2)) AS average_review_score
    FROM review_relations
    GROUP BY month_start, seller_key
), expected AS (
    SELECT
        r.month_start,
        r.seller_key,
        s.seller_id,
        COALESCE(s.seller_state, 'Unknown') AS seller_state,
        r.item_revenue,
        r.freight_total,
        r.item_count,
        r.order_count,
        d.average_delivery_days,
        d.on_time_delivery_rate,
        COALESCE(v.review_count, 0) AS review_count,
        v.average_review_score
    FROM revenue r
    LEFT JOIN delivery d ON r.month_start = d.month_start AND r.seller_key = d.seller_key
    LEFT JOIN review_metrics v ON r.month_start = v.month_start AND r.seller_key = v.seller_key
    LEFT JOIN sellers s ON r.seller_key = s.seller_key
), violations AS (
    SELECT COALESCE(e.month_start, a.month_start) AS month_start
    FROM expected e
    FULL OUTER JOIN lakehouse.gold.mart_seller_performance a
      ON e.month_start IS NOT DISTINCT FROM a.month_start AND e.seller_key = a.seller_key
    WHERE e.item_count IS NULL OR a.item_count IS NULL
       OR e.seller_id IS DISTINCT FROM a.seller_id
       OR e.seller_state IS DISTINCT FROM a.seller_state
       OR e.item_revenue IS DISTINCT FROM a.item_revenue
       OR e.freight_total IS DISTINCT FROM a.freight_total
       OR e.item_count IS DISTINCT FROM a.item_count
       OR e.order_count IS DISTINCT FROM a.order_count
       OR e.average_delivery_days IS DISTINCT FROM a.average_delivery_days
       OR e.on_time_delivery_rate IS DISTINCT FROM a.on_time_delivery_rate
       OR e.review_count IS DISTINCT FROM a.review_count
       OR e.average_review_score IS DISTINCT FROM a.average_review_score
)
SELECT IF(
    COUNT(*) = 0,
    'PASS seller_performance_mart',
    fail(format('seller_performance_mart reconciliation failed: violations=%s', COUNT(*)))
) AS seller_performance_mart
FROM violations;

WITH products AS (
    SELECT
        product_key,
        COALESCE(product_category_name_english, 'unknown') AS product_category
    FROM lakehouse.gold.dim_product
), enriched_items AS (
    SELECT
        i.order_id,
        i.product_key,
        i.order_date,
        i.price,
        i.freight_value,
        COALESCE(p.product_category, 'unknown') AS product_category
    FROM lakehouse.gold.fact_order_items i
    LEFT JOIN products p ON i.product_key = p.product_key
), revenue AS (
    SELECT
        CAST(DATE_TRUNC('month', order_date) AS DATE) AS month_start,
        product_category,
        SUM(price) AS item_revenue,
        SUM(freight_value) AS freight_total,
        COUNT(*) AS item_count,
        COUNT(DISTINCT order_id) AS order_count
    FROM enriched_items
    GROUP BY CAST(DATE_TRUNC('month', order_date) AS DATE), product_category
), order_products AS (
    SELECT DISTINCT
        CAST(DATE_TRUNC('month', order_date) AS DATE) AS month_start,
        order_id,
        product_key,
        product_category
    FROM enriched_items
), review_relations AS (
    SELECT DISTINCT
        op.month_start,
        op.product_category,
        r.review_id,
        r.order_id,
        r.review_score
    FROM lakehouse.gold.bridge_review_products b
    JOIN lakehouse.gold.fact_reviews r ON b.review_id = r.review_id AND b.order_id = r.order_id
    JOIN order_products op ON b.order_id = op.order_id AND b.product_key = op.product_key
), review_metrics AS (
    SELECT
        month_start,
        product_category,
        COUNT(DISTINCT ROW(review_id, order_id)) AS review_count,
        CAST(AVG(CAST(review_score AS DECIMAL(4,2))) AS DECIMAL(4,2)) AS average_review_score
    FROM review_relations
    GROUP BY month_start, product_category
), expected AS (
    SELECT
        r.month_start,
        r.product_category,
        r.item_revenue,
        r.freight_total,
        r.item_count,
        r.order_count,
        COALESCE(v.review_count, 0) AS review_count,
        v.average_review_score
    FROM revenue r
    LEFT JOIN review_metrics v
      ON r.month_start = v.month_start AND r.product_category = v.product_category
), violations AS (
    SELECT COALESCE(e.month_start, a.month_start) AS month_start
    FROM expected e
    FULL OUTER JOIN lakehouse.gold.mart_product_performance a
      ON e.month_start IS NOT DISTINCT FROM a.month_start
     AND e.product_category = a.product_category
    WHERE e.item_count IS NULL OR a.item_count IS NULL
       OR e.item_revenue IS DISTINCT FROM a.item_revenue
       OR e.freight_total IS DISTINCT FROM a.freight_total
       OR e.item_count IS DISTINCT FROM a.item_count
       OR e.order_count IS DISTINCT FROM a.order_count
       OR e.review_count IS DISTINCT FROM a.review_count
       OR e.average_review_score IS DISTINCT FROM a.average_review_score
)
SELECT IF(
    COUNT(*) = 0,
    'PASS product_performance_mart',
    fail(format('product_performance_mart reconciliation failed: violations=%s', COUNT(*)))
) AS product_performance_mart
FROM violations;
