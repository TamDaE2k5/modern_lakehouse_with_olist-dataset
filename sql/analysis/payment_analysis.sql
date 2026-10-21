SELECT
    DATE_TRUNC('month', order_date) AS month_start,
    payment_type,
    COUNT(*) AS payment_component_count,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(payment_value) AS payment_value,
    AVG(payment_installments) AS average_installments
FROM lakehouse.gold.vw_payment_summary
GROUP BY DATE_TRUNC('month', order_date), payment_type
ORDER BY month_start, payment_value DESC;
