EXPLAIN ANALYZE
SELECT revenue_date, SUM(item_revenue) AS item_revenue
FROM lakehouse.gold.mart_daily_revenue
GROUP BY revenue_date
ORDER BY revenue_date;

EXPLAIN ANALYZE
SELECT month_start, seller_state, SUM(item_revenue) AS item_revenue
FROM lakehouse.gold.mart_seller_performance
GROUP BY month_start, seller_state
ORDER BY month_start, item_revenue DESC;

EXPLAIN ANALYZE
SELECT month_start, product_category, SUM(item_revenue) AS item_revenue
FROM lakehouse.gold.mart_product_performance
GROUP BY month_start, product_category
ORDER BY month_start, item_revenue DESC;
