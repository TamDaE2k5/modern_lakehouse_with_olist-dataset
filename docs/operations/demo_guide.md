# Hướng dẫn Demo & Thuyết minh Dự án (Rebuilt Lakehouse)

Tài liệu này cung cấp kịch bản demo và các câu lệnh Linux/Bash thực chiến để người thuyết minh trình diễn trực tiếp hoạt động của hệ sinh thái **Olist Modern Data Lakehouse** đã được rebuild và xác minh hoàn tất 100%.

---

## 1. Chuẩn bị Môi trường & Khởi động Hệ thống

Mở terminal tại thư mục gốc dự án (`Big_Data/rebuild`), khởi động toàn bộ cụm 13 container và kiểm tra tính sẵn sàng:

```bash
# 1. Khởi động 13 container dịch vụ
docker compose --project-name olist-lakehouse \
  -f docker-compose.yml \
  -f docker-compose.query.yml \
  -f docker-compose.orchestration.yml \
  -f docker-compose.bi.yml \
  -f docker-compose.monitoring.yml \
up -d

# 2. Kiểm tra sức khỏe các container (Đảm bảo 13/13 container healthy/running)
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
  "SELECT count(*) AS total_orders, sum(item_revenue) AS item_rev, sum(freight_total) AS freight_tot, sum(payment_total) AS payment_tot FROM lakehouse.gold.fact_orders;"
```
* **Kết quả đối soát kỳ vọng chuẩn xác tuyệt đối:**

  | Chỉ số | Giá trị | File CSV nguồn tương ứng |
  |:---|---:|:---|
  | `total_orders` | **99,441** | `olist_orders_dataset.csv` |
  | `item_rev` | **13,591,643.70** | `olist_order_items_dataset.csv` |
  | `freight_tot` | **2,251,909.54** | `olist_order_items_dataset.csv` |
  | `payment_tot` | **16,008,872.12** | `olist_order_payments_dataset.csv` |

* **Điểm cần thuyết minh cho đúng (rất hay bị hỏi vặn):** ba tổng tiền trên đến từ **ba file nguồn
  khác nhau** nên **không cộng ra nhau**: `13,591,643.70 + 2,251,909.54 = 15,843,553.24`, lệch
  `165,318.88` so với tổng thanh toán. Đây là **đặc tính có thật của bộ dữ liệu Olist gốc** (trả góp
  làm tròn, voucher, đơn không có dòng hàng), chứng minh được ngay trên CSV thô bằng `pandas`.
  Ý nghĩa của "bảo toàn doanh thu" là: **mỗi tổng tiền ở tầng Gold khớp từng cent với chính file
  CSV nguồn của nó** — pipeline đi qua Bronze → Silver → Gold không đánh rơi hay nhân thêm đồng nào.
  Sai lệch giữa Fact, Mart và Semantic View bằng `0.00`.
* ⚠️ Cột tên là **`freight_total`**, KHÔNG phải `freight_value` (`freight_value` chỉ tồn tại ở
  `gold.fact_order_items`, mức hạt dòng hàng).

### Bước 3: Trình chiếu Báo cáo Phân tích Kinh doanh (Apache Superset - Port 8088)
* **Truy cập:** `http://127.0.0.1:8088` (Tài khoản: `olist_admin` / `admin`).
* **Nội dung thuyết minh:**
  * Mở 5 Dashboards kinh doanh đã được liên kết với Trino Semantic Views:
    1. `executive-overview` — *Tổng quan điều hành*: KPI doanh thu, xu hướng theo tháng và
       **Phễu chuyển đổi vòng đời đơn hàng** (Đặt hàng → Duyệt thanh toán → Bàn giao vận chuyển →
       Giao thành công → Khách đánh giá).
    2. `sales-analytics` — *Phân tích bán hàng*: doanh thu theo bang và theo danh mục ngành hàng.
    3. `customer-analytics` — *Phân tích khách hàng*: phân bố theo bang, phân phối chi tiêu và
       tỷ lệ khách quay lại.
    4. `product-analytics` — *Phân tích sản phẩm*: hiệu quả danh mục và điểm đánh giá sản phẩm.
    5. `seller-logistics` — *Phân tích người bán và logistics*: xếp hạng nhà bán, điểm đánh giá,
       xu hướng số ngày giao hàng và tỷ lệ giao đúng hẹn.
  * **Điểm nhấn khi demo — Bộ lọc đồng bộ toàn cục (Cross-dashboard filtering):** trên
    `executive-overview`, chọn một khoảng ngày hoặc một bang ở thanh lọc bên trái; **mọi biểu đồ
    con, kể cả biểu đồ phễu, sẽ tự lọc đồng bộ**. Ngoài ra bấm trực tiếp vào một cột trong biểu đồ
    sẽ kích hoạt lọc chéo (cross-filter) sang các biểu đồ còn lại.

### Bước 4: Giám sát Hệ thống & Chất lượng Thời gian thực (Prometheus & Grafana)
* **Prometheus:** `http://127.0.0.1:9090/targets`
  * Trình chiếu **10/10 Scrape Targets** đều ở trạng thái `UP` với 0 lỗi kết nối. Đáng chú ý có target `mailhog-health`: hệ thống **giám sát chính kênh cảnh báo của nó** — vì module `email_alert.py` cố ý nuốt mọi lỗi gửi thư để không làm sập pipeline, nên nếu không giám sát riêng thì khả năng báo động có thể mất đi trong im lặng.
* **Grafana:** `http://127.0.0.1:3000` (Tài khoản: `admin` / `admin`).
  * Mở thư mục `Olist Lakehouse` và thuyết minh 3 dashboards:
    1. `olist-pipeline` (9 panel): thời gian thực thi, số dòng vào/ra, và đặc biệt là đồng hồ đo
       **Tỷ lệ Dữ liệu Cách Ly (Quarantine Rate %)** tự đổi màu theo ngưỡng — xanh dưới 2%,
       vàng 2-5%, **đỏ khi vượt 5%**. Mốc tham chiếu để thuyết minh: dữ liệu Olist gốc tự nó
       đã có sẵn khoảng 0,2% bản ghi vi phạm logic thời gian (197/99.441 đơn).
    2. `olist-data-quality`: Hiển thị kết quả của **198/198 checks**, phân loại theo mức độ nghiêm trọng (`ERROR`, `WARN`) và phân tầng (`bronze`, `silver`, `gold`).
    3. `olist-infrastructure`: Giám sát tải CPU, dung lượng RAM sử dụng của Spark Master, Worker, Trino, MinIO và PostgreSQL.

### Bước 5: Trình diễn Cảnh báo Sự cố Tự động qua Email (MailHog - Port 8025)
* **Truy cập hộp thư:** `http://127.0.0.1:8025`
* **Kịch bản demo trực tiếp (gây lỗi có kiểm soát rồi xem thư bay về):**
```bash
# Kích hoạt DAG bảo trì với tên bảng KHÔNG nằm trong allowlist -> task chắc chắn thất bại
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_maintenance   --run-id demo-alert-01   --conf '{"run_id":"demo-alert-01","batch_id":"demo-alert-01","mode":"full","namespace":"gold","table":"bang_khong_ton_tai","retention_hours":168,"orphan_older_than_hours":336}'

# Sau khi task dùng hết 2 lần thử lại (khoảng 4-5 phút), kiểm tra hộp thư:
curl -s "http://localhost:8025/api/v2/messages?limit=5"
```
* **Nội dung thuyết minh:** email mức `[CRITICAL]` chứa tên DAG, tên task, Run ID, số lần thử và
  nguyên nhân lỗi. Toàn bộ cơ chế gói gọn trong `infrastructure/alerts/email_alert.py` (43 dòng
  code, chỉ dùng `smtplib` của thư viện chuẩn). Nhấn mạnh: mọi lỗi của chính kênh gửi mail đều bị
  bắt lại và chỉ ghi log — **sự cố ở kênh thông báo tuyệt đối không được phép làm sập pipeline**.
  Chuyển sang Gmail thật chỉ cần đổi các biến `ALERT_SMTP_*` trong `.env`, không sửa một dòng code.

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
