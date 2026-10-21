SELECT
    month_start,
    product_category,
    item_revenue,
    freight_total,
    item_count,
    order_count,
    review_count,
    average_review_score
FROM lakehouse.gold.mart_product_performance
ORDER BY month_start, item_revenue DESC, product_category;
