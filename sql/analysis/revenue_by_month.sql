SELECT
    DATE_TRUNC('month', revenue_date) AS month_start,
    SUM(item_revenue) AS item_revenue,
    SUM(freight_total) AS freight_total,
    SUM(order_count) AS segment_order_count
FROM lakehouse.gold.mart_daily_revenue
GROUP BY DATE_TRUNC('month', revenue_date)
ORDER BY month_start;
