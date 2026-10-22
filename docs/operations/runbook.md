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

*Giao tiếp nội bộ (Không lộ ra host):*
* `5432`: PostgreSQL (Catalog cho Iceberg, DB cho Airflow và Superset).
* `7077`: Spark Master RPC (Giao tiếp giữa Worker / Driver với Master).
* `9101`: Pipeline Metrics Exporter (Phơi bày metrics nghiệp vụ Lakehouse cho Prometheus).
* `9115`: Blackbox Exporter (Thăm dò HTTP/TCP health check cho Prometheus).

---

## 2. Quản lý Vòng đời Hệ thống (Lifecycle Management)

### 2.1. Khởi động Toàn bộ Hệ sinh thái
Khởi động cụm 12 container theo thứ tự phụ thuộc xác định:
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
Kiểm tra đảm bảo 12 container dài hạn đều đạt trạng thái `healthy`:
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

### 4.1. Sự cố Lệch pha Run ID trong Task Bảo trì (`rewrite_data_files`)
* **Hiện tượng:** Task `rewrite_data_files` thất bại với thông báo `ValueError: Run tiêu thụ của báo cáo dry run không khớp yêu cầu`.
* **Nguyên nhân:** Khi chạy với `run_id="auto"`, task khảo sát (`maintenance_dry_run`) và task nén file biên dịch biểu thức Jinja template khác nhau.
* **Xử lý:**
  1. Đảm bảo file `rebuild/orchestration/dags/olist_maintenance.py` sử dụng biến dùng chung `run_base = "{% if params.run_id == 'auto' %}{{ ts_nodash }}{% else %}{{ params.run_id }}{% endif %}"`.
  2. Clear task từ đầu để tạo lại báo cáo dry-run đồng bộ:
     ```bash
     docker exec olist-lakehouse-airflow-scheduler-1 airflow tasks clear olist_maintenance -d -t validate_runtime_params -y
     ```

### 4.2. Khắc phục Xung đột Tài nguyên Ghi (Concurrency & Resource Lock)
* **Hiện tượng:** Spark Worker bị nghẽn (hanging) hoặc báo lỗi `AirflowTaskTimeout` do nhiều job Spark ghi đồng thời vào Iceberg catalog.
* **Xử lý:**
  * Airflow đã được trang bị cơ chế điều tiết thông qua Pool `lakehouse_writes` với 8 slots. Mọi job ghi lớn (Silver, Gold, Maintenance) bắt buộc phải chiếm slots trong pool này để xếp hàng tuần tự.
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
   docker exec -i olist-lakehouse-trino-1 trino --file /opt/lakehouse/sql/trino_semantic_views.sql
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
