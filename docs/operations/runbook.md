# Sổ tay Vận hành Hệ thống (Operations Runbook)

Tài liệu này cung cấp các quy trình chuẩn (SOP), kịch bản ứng phó sự cố và chỉ dẫn vận hành thường nhật cho đội ngũ Kỹ sư Vận hành (DataOps / Platform Engineers) quản lý cụm **Olist Modern Data Lakehouse**.

---

## 1. Danh mục Dịch vụ & Cổng Truy cập

| Dịch vụ | Port Host | Địa chỉ Bind | Mục đích & Trách nhiệm |
|:---|:---:|:---:|:---|
| **Airflow Webserver** | `8083` | `127.0.0.1:8083` | Giao diện điều phối, cấu hình connections, theo dõi DAGs |
| **Trino Query Engine**| `8082` | `127.0.0.1:8082` | Phục vụ truy vấn phân tán qua JDBC/ODBC và Web UI |
| **Apache Superset**   | `8088` | `127.0.0.1:8088` | Nền tảng phân tích kinh doanh, charts và dashboards |
| **MinIO Storage API** | `9000` | `127.0.0.1:9000` | API S3 cho Spark và Trino đọc/ghi tệp Parquet & Metadata |
| **MinIO Web Console** | `9001` | `127.0.0.1:9001` | Quản lý Buckets, Access Keys và duyệt cây thư mục S3 |
| **Spark Master UI**   | `8084` | `127.0.0.1:8084` | Quản lý cụm Spark, theo dõi ứng dụng đang chạy và tài nguyên |
| **Spark Worker UI**   | `8085` | `127.0.0.1:8085` | Theo dõi trạng thái Executor, Thread pool và bộ nhớ Worker |
| **Prometheus Server** | `9090` | `127.0.0.1:9090` | Thu thập metrics, đánh giá Alert Rules và Service Discovery |
| **Grafana Server**    | `3000` | `127.0.0.1:3000` | Bảng điều khiển giám sát tập trung hạ tầng, pipeline, chất lượng |
| **MailHog (SMTP giả)**| `8025` | `127.0.0.1:8025` | Hộp thư nhận email cảnh báo sự cố do Airflow và cổng chất lượng gửi ra |

*Giao tiếp nội bộ (Không lộ ra host):*
* `5432`: PostgreSQL (Catalog cho Iceberg, DB cho Airflow và Superset).
* `7077`: Spark Master RPC (Giao tiếp giữa Worker / Driver với Master).
* `9101`: Pipeline Metrics Exporter (Phơi bày metrics nghiệp vụ Lakehouse cho Prometheus).
* `9115`: Blackbox Exporter (Thăm dò HTTP/TCP health check cho Prometheus).
* `1025`: MailHog SMTP (nơi `infrastructure/alerts/email_alert.py` gửi email cảnh báo tới).

---

## 2. Quản lý Vòng đời Hệ thống (Lifecycle Management)

### 2.1. Khởi động Toàn bộ Hệ sinh thái
Khởi động cụm 13 container theo thứ tự phụ thuộc xác định:
```bash
docker compose --project-name olist-lakehouse \
  -f docker-compose.yml \
  -f docker-compose.query.yml \
  -f docker-compose.orchestration.yml \
  -f docker-compose.bi.yml \
  -f docker-compose.monitoring.yml \
up -d
```

### 2.2. Kiểm tra Sức khỏe Cụm Container
Kiểm tra đảm bảo 13 container dài hạn đều đạt trạng thái `healthy`:
```bash
docker compose --project-name olist-lakehouse \
  -f docker-compose.yml \
  -f docker-compose.query.yml \
  -f docker-compose.orchestration.yml \
  -f docker-compose.bi.yml \
  -f docker-compose.monitoring.yml \
ps
```

### 2.3. Tạm dừng Hệ thống (Bảo toàn Dữ liệu)
Khi cần giải phóng RAM và CPU máy chủ mà không làm mất dữ liệu trong Iceberg, Airflow DB, Superset DB:
```bash
docker compose --project-name olist-lakehouse \
  -f docker-compose.yml \
  -f docker-compose.query.yml \
  -f docker-compose.orchestration.yml \
  -f docker-compose.bi.yml \
  -f docker-compose.monitoring.yml \
down --remove-orphans
```

### 2.4. Khởi tạo lại Toàn diện (Reset Toàn bộ Volumes - Cẩn trọng!)
> [!CAUTION]
> Lệnh này sẽ xóa sạch toàn bộ dữ liệu bảng Iceberg, metadata và logs. Chỉ dùng khi muốn rebuild lại hệ thống từ đầu.
```bash
docker compose --project-name olist-lakehouse \
  -f docker-compose.yml \
  -f docker-compose.query.yml \
  -f docker-compose.orchestration.yml \
  -f docker-compose.bi.yml \
  -f docker-compose.monitoring.yml \
down -v --remove-orphans
```

---

### 2.5. Lối tắt bằng Script Tự Động (Khuyến nghị trên cả Linux & Windows)

Thay cho các lệnh `docker compose` dài 6 dòng ở trên, dùng bộ script trong `scripts/`:

**Trên Linux / Ubuntu / WSL2:**
```bash
./scripts/up.sh                  # Bật cụm và CHỜ tới khi mọi healthcheck xanh, rồi in bảng cổng
./scripts/verify_stack.sh        # Kiểm tra sức khỏe toàn diện trong ~60 giây (5 nhóm kiểm tra)
./scripts/logs.sh airflow-scheduler --tail 200
./scripts/down.sh                # Dừng, BẢO TOÀN dữ liệu
./scripts/down.sh -v             # Dừng và XÓA SẠCH dữ liệu (bắt gõ chữ XOA để xác nhận)
```

**Trên Windows PowerShell:**
```powershell
.\scripts\up.ps1                  # Bật cụm và CHỜ tới khi mọi healthcheck xanh, rồi in bảng cổng
.\scripts\verify_stack.ps1        # Kiểm tra sức khỏe toàn diện trong ~60 giây (5 nhóm kiểm tra)
.\scripts\logs.ps1 -Service airflow-scheduler -Tail 200
.\scripts\down.ps1                # Dừng, BẢO TOÀN dữ liệu
.\scripts\down.ps1 -RemoveVolumes # Dừng và XÓA SẠCH dữ liệu (bắt gõ chữ XOA để xác nhận)
```

---

## 3. Vận hành Điều phối & Kích hoạt Pipeline qua CLI

### 3.1. Kích hoạt Pipeline Hàng ngày (Daily Master Pipeline)
```bash
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_daily_pipeline
```

### 3.2. Kích hoạt Quy trình Kiểm định Chất lượng (Great Expectations)
```bash
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_quality
```

### 3.3. Kích hoạt Bảo trì Nén tệp & Dọn Snapshot Iceberg
```bash
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_maintenance
```

### 3.4. Kiểm tra Trạng thái và Danh sách các Lần chạy của DAG
```bash
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags list-runs -d olist_daily_pipeline
docker exec olist-lakehouse-airflow-scheduler-1 airflow tasks states-for-dag-run olist_daily_pipeline <RUN_ID>
```

---

## 4. Xử lý Sự Cố Thường Gặp (Troubleshooting Guide)

### 4.1. Task Bảo trì `rewrite_data_files` thất bại
* **Bối cảnh lịch sử:** Phiên bản cũ của `transformation/maintenance/job.py` (2.569 dòng) yêu cầu
  task nén tệp phải đọc lại báo cáo dry-run đã được ký HMAC. Khi chạy với `run_id="auto"`, hai task
  biên dịch biểu thức Jinja ở hai thời điểm khác nhau nên sinh ra hai đường dẫn báo cáo lệch nhau,
  làm task thất bại với `ValueError: Run tiêu thụ của báo cáo dry run không khớp yêu cầu`.
* **Hiện trạng:** Lỗi này **đã được loại bỏ tận gốc**. Module bảo trì đã viết lại còn 627 dòng,
  bỏ hoàn toàn cơ chế HMAC, nonce và lease lock. Các task thay đổi dữ liệu **không còn đọc file
  dry-run** nữa, nên không còn tồn tại khả năng lệch pha giữa các task.
* **Nếu task vẫn thất bại, kiểm tra theo thứ tự sau:**
  1. Bảng đích có nằm trong allowlist không (xem `ALLOWED_TARGETS` trong `maintenance/job.py`)?
     Sai tên bảng sẽ báo `Đích bảo trì '<ns>.<table>' không nằm trong allowlist`.
  2. `retention_hours` có >= 168 và `orphan_older_than_hours` có >= `retention_hours` không?
  3. Đọc bản ghi lỗi đã được ghi lại trong bảng kiểm toán:
     ```bash
     docker exec olist-lakehouse-trino-1 trino --execute "SELECT run_id, action, status, error_summary FROM lakehouse.control.maintenance_results WHERE status = 'FAILED' ORDER BY started_at DESC LIMIT 5;"
     ```

### 4.1b. Nhận email cảnh báo sự cố
Mỗi khi một task Airflow thất bại (sau khi đã dùng hết số lần thử lại) hoặc cổng chất lượng
Great Expectations trả về mức `ERROR`, hệ thống tự động gửi email cho Quản trị viên.
* Đọc thư đã nhận: mở `http://localhost:8025`, hoặc gọi API:
  ```bash
  curl -s "http://localhost:8025/api/v2/messages?limit=5"
  ```
* Tắt tạm tính năng (ví dụ khi chạy kiểm thử hàng loạt): đặt `ALERT_EMAIL_ENABLED=false` trong `.env`.
* Chuyển sang SMTP thật: đổi `ALERT_SMTP_HOST`, `ALERT_SMTP_PORT`, `ALERT_SMTP_USER`,
  `ALERT_SMTP_PASSWORD`, `ALERT_SMTP_STARTTLS=true` trong `.env` rồi khởi động lại Airflow.

### 4.2. Khắc phục Xung đột Tài nguyên Ghi (Concurrency & Resource Lock)
* **Hiện tượng:** Spark Worker bị nghẽn (hanging) hoặc Airflow Scheduler ngốn quá nhiều RAM do nhiều job Spark ghi đồng thời vào Iceberg catalog.
* **Xử lý (Đã tối ưu ở Phiên bản V3 Senior):**
  * Airflow đã được trang bị cơ chế điều tiết thông qua Pool `lakehouse_writes` giới hạn ở **2 slots** (giảm từ 8 slots ở V1/V2 xuống 2 slots ở V3). Nhờ đó, tại mỗi thời điểm chỉ có tối đa 2 tiến trình Spark Driver khởi chạy trong `airflow-scheduler` (cắt giảm ~3.6 GiB RAM đỉnh từ 6.9 GiB xuống 3.2 GiB và giúp mỗi bảng Silver hoàn thành trong ~32 giây mà không bị tranh chấp 2 cores của Worker).
  * Riêng các tác vụ ghi/xóa của `olist_maintenance` yêu cầu chiếm trọn cả 2/2 slots (`write_pool_slots=2`), đảm bảo độc quyền tuyệt đối khi bảo trì.
  * Kiểm tra slots khả dụng trong pool:
    ```bash
    docker exec olist-lakehouse-airflow-scheduler-1 airflow pools list
    ```

### 4.3. Kiểm tra Tính Sẵn sàng của Trino Semantic Views
Nếu Apache Superset báo lỗi không tìm thấy bảng `vw_*`:
1. Kiểm tra Trino coordinator:
   ```bash
   docker exec -i olist-lakehouse-trino-1 trino --execute "SHOW SCHEMAS IN lakehouse;"
   ```
2. Nạp lại 4 Semantic Views nếu cần:
   ```bash
   docker exec -i olist-lakehouse-trino-1 trino --file /opt/lakehouse/sql/trino/create_views.sql
   ```

---

## 5. Sao lưu và Khôi phục (Backup & Disaster Recovery)

### 5.1. Sao lưu Metadata PostgreSQL & Iceberg Catalog
Tạo bản sao lưu metadata trước các đợt bảo trì nâng cấp lớn:
```bash
mkdir -p runtime/backups
docker exec olist-lakehouse-postgres-1 pg_dump -U postgres --format=custom --file /tmp/iceberg_catalog.dump iceberg
docker cp olist-lakehouse-postgres-1:/tmp/iceberg_catalog.dump runtime/backups/iceberg_catalog_$(date +%Y%m%d).dump
```

### 5.2. Sao lưu Dữ liệu Tệp Parquet trên MinIO
Sử dụng MinIO Client (`mc`) hoặc copy trực tiếp volume dữ liệu MinIO sang thư mục backup an toàn.
