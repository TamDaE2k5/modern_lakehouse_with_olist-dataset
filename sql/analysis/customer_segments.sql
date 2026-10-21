SELECT
    CASE WHEN repeat_customer THEN 'Repeat' ELSE 'One-time' END AS customer_segment,
    customer_state,
    COUNT(*) AS customer_count,
    SUM(order_count) AS lifetime_order_count
FROM lakehouse.gold.dim_customer
WHERE customer_key <> 0
GROUP BY CASE WHEN repeat_customer THEN 'Repeat' ELSE 'One-time' END, customer_state
ORDER BY customer_count DESC, customer_segment, customer_state;
