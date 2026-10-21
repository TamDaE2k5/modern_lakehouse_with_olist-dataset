SELECT check_name, CAST(actual_count AS varchar) AS actual_count
            FROM (
              SELECT 'bridge_products_order_orphan' check_name, count_if(o.order_id IS NULL) actual_count
                FROM lakehouse.gold.bridge_review_products b LEFT JOIN lakehouse.gold.fact_orders o ON b.order_id = o.order_id
              UNION ALL SELECT 'bridge_products_product_key0', count_if(product_key = 0) FROM lakehouse.gold.bridge_review_products
              UNION ALL SELECT 'bridge_products_product_orphan', count_if(d.product_key IS NULL) FROM lakehouse.gold.bridge_review_products b LEFT JOIN lakehouse.gold.dim_product d ON b.product_key = d.product_key
              UNION ALL SELECT 'bridge_products_review_orphan', count_if(r.review_id IS NULL) FROM lakehouse.gold.bridge_review_products b LEFT JOIN lakehouse.gold.fact_reviews r ON b.review_id = r.review_id AND b.order_id = r.order_id
              UNION ALL SELECT 'bridge_sellers_order_orphan', count_if(o.order_id IS NULL) FROM lakehouse.gold.bridge_review_sellers b LEFT JOIN lakehouse.gold.fact_orders o ON b.order_id = o.order_id
              UNION ALL SELECT 'bridge_sellers_review_orphan', count_if(r.review_id IS NULL) FROM lakehouse.gold.bridge_review_sellers b LEFT JOIN lakehouse.gold.fact_reviews r ON b.review_id = r.review_id AND b.order_id = r.order_id
              UNION ALL SELECT 'bridge_sellers_seller_key0', count_if(seller_key = 0) FROM lakehouse.gold.bridge_review_sellers
              UNION ALL SELECT 'bridge_sellers_seller_orphan', count_if(d.seller_key IS NULL) FROM lakehouse.gold.bridge_review_sellers b LEFT JOIN lakehouse.gold.dim_seller d ON b.seller_key = d.seller_key
              UNION ALL SELECT 'dim_customer_geography_key0', count_if(customer_key <> 0 AND geography_key = 0) FROM lakehouse.gold.dim_customer
              UNION ALL SELECT 'dim_customer_geography_orphan', count_if(g.geography_key IS NULL) FROM lakehouse.gold.dim_customer c LEFT JOIN lakehouse.gold.dim_geography g ON c.geography_key = g.geography_key
              UNION ALL SELECT 'dim_customer_unknown', count_if(customer_key = 0) FROM lakehouse.gold.dim_customer
              UNION ALL SELECT 'dim_date_unknown', count_if(date_key = 0) FROM lakehouse.gold.dim_date
              UNION ALL SELECT 'dim_geography_unknown', count_if(geography_key = 0) FROM lakehouse.gold.dim_geography
              UNION ALL SELECT 'dim_product_unknown', count_if(product_key = 0) FROM lakehouse.gold.dim_product
              UNION ALL SELECT 'dim_seller_geography_key0', count_if(seller_key <> 0 AND geography_key = 0) FROM lakehouse.gold.dim_seller
              UNION ALL SELECT 'dim_seller_geography_orphan', count_if(g.geography_key IS NULL) FROM lakehouse.gold.dim_seller s LEFT JOIN lakehouse.gold.dim_geography g ON s.geography_key = g.geography_key
              UNION ALL SELECT 'dim_seller_unknown', count_if(seller_key = 0) FROM lakehouse.gold.dim_seller
              UNION ALL SELECT 'fact_items_customer_key0', count_if(customer_key = 0) FROM lakehouse.gold.fact_order_items
              UNION ALL SELECT 'fact_items_customer_orphan', count_if(d.customer_key IS NULL) FROM lakehouse.gold.fact_order_items f LEFT JOIN lakehouse.gold.dim_customer d ON f.customer_key = d.customer_key
              UNION ALL SELECT 'fact_items_order_date_key0', count_if(order_date_key = 0) FROM lakehouse.gold.fact_order_items
              UNION ALL SELECT 'fact_items_order_date_orphan', count_if(d.date_key IS NULL) FROM lakehouse.gold.fact_order_items f LEFT JOIN lakehouse.gold.dim_date d ON f.order_date_key = d.date_key
              UNION ALL SELECT 'fact_items_order_orphan', count_if(o.order_id IS NULL) FROM lakehouse.gold.fact_order_items f LEFT JOIN lakehouse.gold.fact_orders o ON f.order_id = o.order_id
              UNION ALL SELECT 'fact_items_product_key0', count_if(product_key = 0) FROM lakehouse.gold.fact_order_items
              UNION ALL SELECT 'fact_items_product_orphan', count_if(d.product_key IS NULL) FROM lakehouse.gold.fact_order_items f LEFT JOIN lakehouse.gold.dim_product d ON f.product_key = d.product_key
              UNION ALL SELECT 'fact_items_seller_key0', count_if(seller_key = 0) FROM lakehouse.gold.fact_order_items
              UNION ALL SELECT 'fact_items_seller_orphan', count_if(d.seller_key IS NULL) FROM lakehouse.gold.fact_order_items f LEFT JOIN lakehouse.gold.dim_seller d ON f.seller_key = d.seller_key
              UNION ALL SELECT 'fact_items_shipping_date_key0', count_if(shipping_limit_date_key = 0) FROM lakehouse.gold.fact_order_items
              UNION ALL SELECT 'fact_items_shipping_date_orphan', count_if(d.date_key IS NULL) FROM lakehouse.gold.fact_order_items f LEFT JOIN lakehouse.gold.dim_date d ON f.shipping_limit_date_key = d.date_key
              UNION ALL SELECT 'fact_orders_approval_date_key0', count_if(approval_date_key = 0) FROM lakehouse.gold.fact_orders
              UNION ALL SELECT 'fact_orders_approval_date_orphan', count_if(d.date_key IS NULL) FROM lakehouse.gold.fact_orders f LEFT JOIN lakehouse.gold.dim_date d ON f.approval_date_key = d.date_key
              UNION ALL SELECT 'fact_orders_carrier_date_key0', count_if(carrier_date_key = 0) FROM lakehouse.gold.fact_orders
              UNION ALL SELECT 'fact_orders_carrier_date_orphan', count_if(d.date_key IS NULL) FROM lakehouse.gold.fact_orders f LEFT JOIN lakehouse.gold.dim_date d ON f.carrier_date_key = d.date_key
              UNION ALL SELECT 'fact_orders_customer_key0', count_if(customer_key = 0) FROM lakehouse.gold.fact_orders
              UNION ALL SELECT 'fact_orders_customer_orphan', count_if(d.customer_key IS NULL) FROM lakehouse.gold.fact_orders f LEFT JOIN lakehouse.gold.dim_customer d ON f.customer_key = d.customer_key
              UNION ALL SELECT 'fact_orders_delivered_date_key0', count_if(delivered_date_key = 0) FROM lakehouse.gold.fact_orders
              UNION ALL SELECT 'fact_orders_delivered_date_orphan', count_if(d.date_key IS NULL) FROM lakehouse.gold.fact_orders f LEFT JOIN lakehouse.gold.dim_date d ON f.delivered_date_key = d.date_key
              UNION ALL SELECT 'fact_orders_estimated_date_key0', count_if(estimated_delivery_date_key = 0) FROM lakehouse.gold.fact_orders
              UNION ALL SELECT 'fact_orders_estimated_date_orphan', count_if(d.date_key IS NULL) FROM lakehouse.gold.fact_orders f LEFT JOIN lakehouse.gold.dim_date d ON f.estimated_delivery_date_key = d.date_key
              UNION ALL SELECT 'fact_orders_purchase_date_key0', count_if(purchase_date_key = 0) FROM lakehouse.gold.fact_orders
              UNION ALL SELECT 'fact_orders_purchase_date_orphan', count_if(d.date_key IS NULL) FROM lakehouse.gold.fact_orders f LEFT JOIN lakehouse.gold.dim_date d ON f.purchase_date_key = d.date_key
              UNION ALL SELECT 'fact_payments_customer_key0', count_if(customer_key = 0) FROM lakehouse.gold.fact_payments
              UNION ALL SELECT 'fact_payments_customer_orphan', count_if(d.customer_key IS NULL) FROM lakehouse.gold.fact_payments f LEFT JOIN lakehouse.gold.dim_customer d ON f.customer_key = d.customer_key
              UNION ALL SELECT 'fact_payments_order_date_key0', count_if(order_date_key = 0) FROM lakehouse.gold.fact_payments
              UNION ALL SELECT 'fact_payments_order_date_orphan', count_if(d.date_key IS NULL) FROM lakehouse.gold.fact_payments f LEFT JOIN lakehouse.gold.dim_date d ON f.order_date_key = d.date_key
              UNION ALL SELECT 'fact_payments_order_orphan', count_if(o.order_id IS NULL) FROM lakehouse.gold.fact_payments f LEFT JOIN lakehouse.gold.fact_orders o ON f.order_id = o.order_id
              UNION ALL SELECT 'fact_reviews_answer_date_key0', count_if(answer_date_key = 0) FROM lakehouse.gold.fact_reviews
              UNION ALL SELECT 'fact_reviews_answer_date_orphan', count_if(d.date_key IS NULL) FROM lakehouse.gold.fact_reviews f LEFT JOIN lakehouse.gold.dim_date d ON f.answer_date_key = d.date_key
              UNION ALL SELECT 'fact_reviews_customer_key0', count_if(customer_key = 0) FROM lakehouse.gold.fact_reviews
              UNION ALL SELECT 'fact_reviews_customer_orphan', count_if(d.customer_key IS NULL) FROM lakehouse.gold.fact_reviews f LEFT JOIN lakehouse.gold.dim_customer d ON f.customer_key = d.customer_key
              UNION ALL SELECT 'fact_reviews_order_orphan', count_if(o.order_id IS NULL) FROM lakehouse.gold.fact_reviews f LEFT JOIN lakehouse.gold.fact_orders o ON f.order_id = o.order_id
              UNION ALL SELECT 'fact_reviews_review_date_key0', count_if(review_date_key = 0) FROM lakehouse.gold.fact_reviews
              UNION ALL SELECT 'fact_reviews_review_date_orphan', count_if(d.date_key IS NULL) FROM lakehouse.gold.fact_reviews f LEFT JOIN lakehouse.gold.dim_date d ON f.review_date_key = d.date_key
            ) checks
            ORDER BY check_name;
