WITH ranked_sellers AS (
    SELECT
        month_start,
        seller_id,
        seller_state,
        item_revenue,
        order_count,
        on_time_delivery_rate,
        average_review_score,
        ROW_NUMBER() OVER (
            PARTITION BY month_start
            ORDER BY item_revenue DESC, seller_id
        ) AS revenue_rank
    FROM lakehouse.gold.mart_seller_performance
)
SELECT
    month_start,
    seller_id,
    seller_state,
    item_revenue,
    order_count,
    on_time_delivery_rate,
    average_review_score,
    revenue_rank
FROM ranked_sellers
WHERE revenue_rank <= 20
ORDER BY month_start, revenue_rank;
