-- Executable reconciliation for the 16 measures exposed by the Superset charts.
-- Each branch compares the exported dataset expression with an independently
-- reconstructed Gold result. Any mismatch makes the Trino CLI exit non-zero.
WITH
order_summary AS (
    SELECT * FROM lakehouse.gold.vw_order_summary
),
spending_bins AS (
    SELECT
        CASE
            WHEN payment_total < 50 THEN '00-49.99'
            WHEN payment_total < 100 THEN '50-99.99'
            WHEN payment_total < 200 THEN '100-199.99'
            WHEN payment_total < 500 THEN '200-499.99'
            WHEN payment_total < 1000 THEN '500-999.99'
            ELSE '1000+'
        END AS spending_band,
        COUNT(DISTINCT order_id) AS order_count
    FROM order_summary
    GROUP BY 1
),
expected_spending_bins AS (
    SELECT
        CASE
            WHEN payment_total < 50 THEN '00-49.99'
            WHEN payment_total < 100 THEN '50-99.99'
            WHEN payment_total < 200 THEN '100-199.99'
            WHEN payment_total < 500 THEN '200-499.99'
            WHEN payment_total < 1000 THEN '500-999.99'
            ELSE '1000+'
        END AS spending_band,
        COUNT(DISTINCT order_id) AS order_count
    FROM lakehouse.gold.fact_orders
    GROUP BY 1
),
distinct_seller_orders AS (
    SELECT DISTINCT
        CAST(DATE_TRUNC('month', order_date) AS DATE) AS month_start,
        seller_key,
        order_id
    FROM lakehouse.gold.fact_order_items
),
seller_delivery_dashboard AS (
    SELECT
        COUNT_IF(o.delivery_duration_days IS NOT NULL) AS delivered_order_count,
        SUM(
            CASE WHEN o.delivery_duration_days IS NOT NULL
                THEN CAST(o.delivery_duration_days AS DOUBLE) ELSE 0.0 END
        ) AS delivery_days_sum,
        COUNT_IF(
            o.delivery_duration_days IS NOT NULL
            AND NOT COALESCE(o.is_late_delivery, FALSE)
        ) AS on_time_delivery_count
    FROM distinct_seller_orders so
    LEFT JOIN lakehouse.gold.fact_orders o ON so.order_id = o.order_id
),
seller_review_relations AS (
    SELECT DISTINCT
        so.month_start,
        so.seller_key,
        r.review_id,
        r.order_id,
        r.review_score
    FROM lakehouse.gold.bridge_review_sellers b
    JOIN lakehouse.gold.fact_reviews r
      ON b.review_id = r.review_id AND b.order_id = r.order_id
    JOIN distinct_seller_orders so
      ON b.order_id = so.order_id AND b.seller_key = so.seller_key
),
seller_review_gold AS (
    SELECT
        COUNT(DISTINCT ROW(review_id, order_id)) AS review_count,
        CAST(AVG(CAST(review_score AS DECIMAL(4,2))) AS DECIMAL(4,2))
            AS average_review_score
    FROM seller_review_relations
    GROUP BY month_start, seller_key
),
distinct_product_orders AS (
    SELECT DISTINCT
        CAST(DATE_TRUNC('month', i.order_date) AS DATE) AS month_start,
        i.product_key,
        i.order_id,
        COALESCE(p.product_category_name_english, 'unknown') AS product_category
    FROM lakehouse.gold.fact_order_items i
    LEFT JOIN lakehouse.gold.dim_product p ON i.product_key = p.product_key
),
product_review_relations AS (
    SELECT DISTINCT
        po.month_start,
        po.product_category,
        r.review_id,
        r.order_id,
        r.review_score
    FROM lakehouse.gold.bridge_review_products b
    JOIN lakehouse.gold.fact_reviews r
      ON b.review_id = r.review_id AND b.order_id = r.order_id
    JOIN distinct_product_orders po
      ON b.order_id = po.order_id AND b.product_key = po.product_key
),
product_review_gold AS (
    SELECT
        COUNT(DISTINCT ROW(review_id, order_id)) AS review_count,
        CAST(AVG(CAST(review_score AS DECIMAL(4,2))) AS DECIMAL(4,2))
            AS average_review_score
    FROM product_review_relations
    GROUP BY month_start, product_category
),
checks(dashboard_name, metric_name, dashboard_value, gold_value) AS (
    SELECT 'Customer Analytics', 'customer_count',
        CAST(COUNT(DISTINCT customer_unique_id) AS DOUBLE),
        CAST((SELECT COUNT(DISTINCT c.customer_unique_id)
              FROM lakehouse.gold.fact_orders o
              JOIN lakehouse.gold.dim_customer c ON o.customer_key = c.customer_key) AS DOUBLE)
    FROM order_summary
    UNION ALL
    SELECT 'Customer Analytics', 'repeat_customer_share',
        CAST(COUNT(DISTINCT CASE WHEN repeat_customer THEN customer_unique_id END) AS DOUBLE)
            / NULLIF(CAST(COUNT(DISTINCT customer_unique_id) AS DOUBLE), 0.0),
        CAST((SELECT COUNT(DISTINCT CASE WHEN c.repeat_customer THEN c.customer_unique_id END)
              FROM lakehouse.gold.fact_orders o
              JOIN lakehouse.gold.dim_customer c ON o.customer_key = c.customer_key) AS DOUBLE)
            / NULLIF(CAST((SELECT COUNT(DISTINCT c.customer_unique_id)
                           FROM lakehouse.gold.fact_orders o
                           JOIN lakehouse.gold.dim_customer c ON o.customer_key = c.customer_key)
                          AS DOUBLE), 0.0)
    FROM order_summary
    UNION ALL
    SELECT 'Customer Analytics', 'spending_distribution_6_bins',
        CAST(
            COUNT_IF(
                d.spending_band IS NULL
                OR e.spending_band IS NULL
                OR d.order_count <> e.order_count
            ) + IF(COUNT(e.spending_band) = 6, 0, 1)
            AS DOUBLE
        ),
        CAST(0 AS DOUBLE)
    FROM spending_bins d
    FULL OUTER JOIN expected_spending_bins e
      ON d.spending_band = e.spending_band
    UNION ALL
    SELECT 'Executive Overview', 'order_count',
        CAST(COUNT(DISTINCT order_id) AS DOUBLE),
        CAST((SELECT COUNT(*) FROM lakehouse.gold.fact_orders) AS DOUBLE)
    FROM order_summary
    UNION ALL
    SELECT 'Executive Overview', 'average_order_value',
        CAST(SUM(item_revenue) AS DOUBLE) / NULLIF(CAST(COUNT(DISTINCT order_id) AS DOUBLE), 0.0),
        CAST((SELECT SUM(price) FROM lakehouse.gold.fact_order_items) AS DOUBLE)
            / NULLIF(CAST((SELECT COUNT(*) FROM lakehouse.gold.fact_orders) AS DOUBLE), 0.0)
    FROM order_summary
    UNION ALL
    SELECT 'Executive Overview', 'average_delivery_days',
        AVG(CAST(delivery_duration_days AS DOUBLE)),
        (SELECT AVG(CAST(delivery_duration_days AS DOUBLE)) FROM lakehouse.gold.fact_orders)
    FROM order_summary
    UNION ALL
    SELECT 'Executive Overview', 'on_time_delivery_share',
        AVG(CASE WHEN delivery_duration_days IS NOT NULL
            THEN IF(NOT COALESCE(is_late_delivery, FALSE),
                    CAST(1 AS DOUBLE), CAST(0 AS DOUBLE)) END),
        (SELECT AVG(CASE WHEN delivery_duration_days IS NOT NULL
                    THEN IF(NOT COALESCE(is_late_delivery, FALSE),
                            CAST(1 AS DOUBLE), CAST(0 AS DOUBLE)) END)
         FROM lakehouse.gold.fact_orders)
    FROM order_summary
    UNION ALL
    SELECT 'Executive Overview', 'revenue_trend',
        CAST((SELECT SUM(item_revenue) FROM lakehouse.gold.mart_daily_revenue) AS DOUBLE),
        CAST((SELECT SUM(price) FROM lakehouse.gold.fact_order_items) AS DOUBLE)
    UNION ALL
    SELECT 'Sales Analytics', 'revenue_by_state',
        CAST((SELECT SUM(item_revenue) FROM lakehouse.gold.mart_daily_revenue) AS DOUBLE),
        CAST((SELECT SUM(i.price) FROM lakehouse.gold.fact_order_items i
              LEFT JOIN lakehouse.gold.dim_customer c ON i.customer_key = c.customer_key) AS DOUBLE)
    UNION ALL
    SELECT 'Sales Analytics', 'revenue_by_category',
        CAST((SELECT SUM(item_revenue) FROM lakehouse.gold.mart_daily_revenue) AS DOUBLE),
        CAST((SELECT SUM(i.price) FROM lakehouse.gold.fact_order_items i
              LEFT JOIN lakehouse.gold.dim_product p ON i.product_key = p.product_key) AS DOUBLE)
    UNION ALL
    SELECT 'Seller and Logistics Analytics', 'item_revenue',
        CAST((SELECT SUM(item_revenue) FROM lakehouse.gold.mart_seller_performance) AS DOUBLE),
        CAST((SELECT SUM(price) FROM lakehouse.gold.fact_order_items) AS DOUBLE)
    UNION ALL
    SELECT 'Seller and Logistics Analytics', 'average_review_score',
        CAST((SELECT SUM(average_review_score * review_count) / NULLIF(SUM(review_count), 0)
              FROM lakehouse.gold.mart_seller_performance) AS DOUBLE),
        CAST((SELECT SUM(average_review_score * review_count) / NULLIF(SUM(review_count), 0)
              FROM seller_review_gold) AS DOUBLE)
    UNION ALL
    SELECT 'Seller and Logistics Analytics', 'average_delivery_days',
        delivery_days_sum / NULLIF(CAST(delivered_order_count AS DOUBLE), 0.0),
        (SELECT AVG(CAST(o.delivery_duration_days AS DOUBLE))
         FROM distinct_seller_orders so
         JOIN lakehouse.gold.fact_orders o ON so.order_id = o.order_id
         WHERE o.delivery_duration_days IS NOT NULL)
    FROM seller_delivery_dashboard
    UNION ALL
    SELECT 'Seller and Logistics Analytics', 'on_time_delivery_share',
        CAST(on_time_delivery_count AS DOUBLE) / NULLIF(CAST(delivered_order_count AS DOUBLE), 0.0),
        (SELECT AVG(IF(NOT COALESCE(o.is_late_delivery, FALSE),
                       CAST(1 AS DOUBLE), CAST(0 AS DOUBLE)))
         FROM distinct_seller_orders so
         JOIN lakehouse.gold.fact_orders o ON so.order_id = o.order_id
         WHERE o.delivery_duration_days IS NOT NULL)
    FROM seller_delivery_dashboard
    UNION ALL
    SELECT 'Product Analytics', 'item_revenue',
        CAST((SELECT SUM(item_revenue) FROM lakehouse.gold.mart_product_performance) AS DOUBLE),
        CAST((SELECT SUM(price) FROM lakehouse.gold.fact_order_items) AS DOUBLE)
    UNION ALL
    SELECT 'Product Analytics', 'average_review_score',
        CAST((SELECT SUM(average_review_score * review_count) / NULLIF(SUM(review_count), 0)
              FROM lakehouse.gold.mart_product_performance) AS DOUBLE),
        CAST((SELECT SUM(average_review_score * review_count) / NULLIF(SUM(review_count), 0)
              FROM product_review_gold) AS DOUBLE)
)
SELECT
    dashboard_name,
    metric_name,
    dashboard_value,
    gold_value,
    IF(
        ABS(dashboard_value - gold_value) <= 0.000001,
        'PASS',
        fail(FORMAT(
            'Superset KPI mismatch: dashboard=%s metric=%s dashboard_value=%s gold_value=%s',
            dashboard_name, metric_name, dashboard_value, gold_value
        ))
    ) AS result
FROM checks
ORDER BY dashboard_name, metric_name;
