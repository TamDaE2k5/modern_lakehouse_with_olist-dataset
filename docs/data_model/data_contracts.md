# Data contract

Snapshot chính thức của môi trường chạy là [schema_snapshot.json](schema_snapshot.json). Kiểm thử contract yêu cầu mọi table được lưu bền bên dưới phải tồn tại trong snapshot đó và trong từ điển column.

| Lớp | Table được lưu bền | Contract |
|---|---|---|
| Bronze | bronze.customers, bronze.geolocation, bronze.order_items, bronze.order_payments, bronze.order_reviews, bronze.orders, bronze.product_category_translation, bronze.products, bronze.sellers | Đúng tuyệt đối thành phần/header/checksum CSV; giữ nguyên chuỗi; điền đầy đủ column lineage; partition theo ngày nạp. |
| Silver | silver.customers, silver.geography, silver.order_items, silver.order_payments, silver.order_reviews, silver.orders, silver.products, silver.sellers | Schema đã định kiểu; grain tổng hợp; miền giá trị hợp lệ; partition theo năm liên kết với đơn hàng khi được cấu hình; từ chối nguyên tử các dòng lỗi. |
| Vùng cách ly | quarantine.geography, quarantine.orders | Lineage gốc, các trường ứng viên đã định kiểu, `_error_codes` không rỗng và `_rejected_at`; được commit cùng đích hợp lệ. Schema dành riêng cho từng đích thay vì một khối dữ liệu chung không minh bạch. |
| Gold dimension | gold.dim_customer, gold.dim_date, gold.dim_geography, gold.dim_product, gold.dim_seller | Surrogate key ổn định và xác định; khóa duy nhất; thành viên chưa biết có khóa `0`; bao phủ FK của dimension. |
| Gold fact | gold.fact_order_items, gold.fact_orders, gold.fact_payments, gold.fact_reviews | Grain giao dịch đã khai báo, partition theo năm, FK dimension bao gồm thành viên chưa biết và đối soát đại lượng cộng được. |
| Gold bridge/mart | gold.bridge_review_products, gold.bridge_review_sellers, gold.mart_daily_revenue, gold.mart_product_performance, gold.mart_seller_performance | Grain quan hệ/tổng hợp phức hợp; không nhân đôi đại lượng do fan-out; đối soát mart với fact. |
| Control | control.ingestion_batches, control.pipeline_runs, control.transformation_targets, control.maintenance_results | Quyền sở hữu lần thử, định danh logic bất biến, chuyển trạng thái, cờ commit, số lượng, thời lượng và bằng chứng xử lý rõ ràng. Riêng `maintenance_results` ghi một dòng cho mỗi (run bảo trì × hành động) kèm số tệp và dung lượng đo được trước/sau, dùng để chứng minh hiệu quả thực tế của việc nén tệp. |
| Quality | quality.quality_results | Một kết quả cho mỗi run/layer/table/rule, severity thuộc ERROR/WARN/INFO, trạng thái cùng bằng chứng quan sát/ngưỡng và partition theo ngày kiểm tra. |

Nullability trong snapshot là contract vật lý của Iceberg. Các giá trị bắt buộc theo nghiệp vụ còn được thực thi bởi những bộ kiểm thử chất lượng; trường nullable vật lý không đồng nghĩa với quyền công bố khóa nghiệp vụ null. Partition là phép biến đổi, không phải column dành cho người dùng.
