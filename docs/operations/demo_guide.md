# Hướng dẫn Demo & Thuyết minh Dự án (Rebuilt Lakehouse)

Tài liệu này cung cấp kịch bản demo và các câu lệnh Linux/Bash thực chiến để người thuyết minh trình diễn trực tiếp hoạt động của hệ sinh thái **Olist Modern Data Lakehouse** đã được rebuild và xác minh hoàn tất 100%.

---

## 1. Chuẩn bị Môi trường & Khởi động Hệ thống

Mở terminal tại thư mục gốc dự án (`Big_Data/rebuild`), khởi động toàn bộ cụm 12 container và kiểm tra tính sẵn sàng:

```bash
# 1. Khởi động 12 container dịch vụ
docker compose --project-name olist-lakehouse \
  -f docker-compose.yml \
  -f docker-compose.query.yml \
  -f docker-compose.orchestration.yml \
  -f docker-compose.bi.yml \
  -f docker-compose.monitoring.yml \
up -d

# 2. Kiểm tra sức khỏe các container (Đảm bảo 12/12 container healthy/running)
docker compose --project-name olist-lakehouse \
  -f docker-compose.yml \
  -f docker-compose.query.yml \
  -f docker-compose.orchestration.yml \
  -f docker-compose.bi.yml \
  -f docker-compose.monitoring.yml \
ps
```

---

## 2. Kịch bản Thuyết minh Demo theo 4 Tầng Cốt Lõi

### Bước 1: Trình diễn Điều phối Luồng dữ liệu (Apache Airflow - Port 8083)
* **Truy cập:** `http://127.0.0.1:8083` (Tài khoản: `admin` / `admin`).
* **Nội dung thuyết minh:**
  * Giới thiệu 4 DAGs chuẩn hóa theo trách nhiệm độc lập:
    1. `olist_daily_pipeline`: DAG mẹ điều phối Master Pipeline đầu-cuối.
    2. `olist_transformation`: Pipeline ETL biến đổi dữ liệu (Bronze $\rightarrow$ Silver $\rightarrow$ Gold).
    3. `olist_quality`: Cổng kiểm soát chất lượng tự động hóa (198 checks Great Expectations).
    4. `olist_maintenance`: Tự động nén tệp Parquet và dọn dẹp snapshot thừa trên Iceberg.
  * Chỉ ra tính năng **Resource Throttling**: Sử dụng Airflow Pool `lakehouse_writes` để giới hạn số slot ghi Spark đồng thời, bảo vệ cụm Spark Master/Worker không bị tràn RAM.

### Bước 2: Kiểm tra Tính Toàn Vẹn & Doanh Thu Chính Xác (Trino SQL - Port 8082)
* **Truy cập:** `http://127.0.0.1:8082` (User: `trino`).
* **Thực thi truy vấn đối soát dữ liệu trực tiếp:**
```bash
# Kiểm tra sự sẵn sàng của 4 Semantic Views phục vụ BI
docker exec -i olist-lakehouse-trino-1 trino --execute \
  "SELECT table_name FROM lakehouse.information_schema.views WHERE table_schema='gold' AND table_name LIKE 'vw_%' ORDER BY table_name;"

# Kiểm định Định luật Bảo toàn Doanh thu (Conservation of Money)
docker exec -i olist-lakehouse-trino-1 trino --execute \
  "SELECT count(*) AS total_orders, sum(item_revenue) AS item_rev, sum(freight_value) AS freight_val, sum(payment_total) AS payment_tot FROM lakehouse.gold.fact_orders;"
```
* **Kết quả đối soát kỳ vọng chuẩn xác tuyệt đối:**
  * `total_orders`: **99,441**
  * `item_rev`: **13,591,643.70**
  * `freight_val`: **2,417,228.42**
  * `payment_tot`: **16,008,872.12**
  * *Sai lệch giữa Fact Orders, Payment Fact và Daily Revenue Mart bằng 0.00.*

### Bước 3: Trình chiếu Báo cáo Phân tích Kinh doanh (Apache Superset - Port 8088)
* **Truy cập:** `http://127.0.0.1:8088` (Tài khoản: `olist_admin` / `admin`).
* **Nội dung thuyết minh:**
  * Mở 5 Dashboards kinh doanh đã được liên kết với Trino Semantic Views:
    1. `executive-overview`: Bức tranh tổng quan doanh thu, số lượng đơn hàng và giá trị đơn trung bình (AOV).
    2. `customer-insights`: Phân khúc địa lý khách hàng, hành vi mua sắm và tỷ lệ giữ chân.
    3. `seller-performance`: Xếp hạng hiệu quả kinh doanh của các nhà bán hàng và tốc độ xử lý đơn.
    4. `logistics-delivery`: Thời gian giao hàng thực tế, độ trễ vận chuyển theo từng bang của Brazil.
    5. `product-performance`: Danh mục hàng hóa bán chạy nhất và đánh giá mức độ hài lòng (Review scores).

### Bước 4: Giám sát Hệ thống & Chất lượng Thời gian thực (Prometheus & Grafana)
* **Prometheus:** `http://127.0.0.1:9090/targets`
  * Trình chiếu **9/9 Scrape Targets** đều ở trạng thái `UP` với 0 lỗi kết nối.
* **Grafana:** `http://127.0.0.1:3000` (Tài khoản: `admin` / `admin`).
  * Mở thư mục `Olist Lakehouse` và thuyết minh 3 dashboards:
    1. `olist-pipeline`: Theo dõi thời gian thực thi, số lượng bản ghi nạp vào (Input rows), xuất ra (Output rows), và tỷ lệ bản ghi bị cách ly (Quarantine ratio).
    2. `olist-data-quality`: Hiển thị kết quả của **198/198 checks**, phân loại theo mức độ nghiêm trọng (`ERROR`, `WARN`) và phân tầng (`bronze`, `silver`, `gold`).
    3. `olist-infrastructure`: Giám sát tải CPU, dung lượng RAM sử dụng của Spark Master, Worker, Trino, MinIO và PostgreSQL.

---

## 3. Lệnh Kết thúc & Dọn dẹp Tài nguyên sau Demo

Sau khi kết thúc buổi thuyết minh, giải phóng toàn bộ RAM nhưng vẫn giữ nguyên vẹn dữ liệu cho lần demo kế tiếp:

```bash
docker compose --project-name olist-lakehouse \
  -f docker-compose.yml \
  -f docker-compose.query.yml \
  -f docker-compose.orchestration.yml \
  -f docker-compose.bi.yml \
  -f docker-compose.monitoring.yml \
down --remove-orphans
```
