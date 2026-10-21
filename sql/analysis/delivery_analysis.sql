SELECT
    DATE_TRUNC('month', purchase_date) AS month_start,
    customer_state,
    COUNT(*) AS order_count,
    AVG(delivery_duration_days) AS average_delivery_days,
    AVG(CASE WHEN is_late_delivery THEN 1.0 ELSE 0.0 END) AS late_delivery_rate
FROM lakehouse.gold.vw_order_summary
WHERE delivery_duration_days IS NOT NULL
GROUP BY DATE_TRUNC('month', purchase_date), customer_state
ORDER BY month_start, customer_state;
