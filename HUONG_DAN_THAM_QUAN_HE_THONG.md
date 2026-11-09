# CẨM NANG TOÀN DIỆN THAM QUAN & THỰC HÀNH OLIST DATA LAKEHOUSE
## (HANDS-ON TOUR, ARCHITECTURAL WALKTHROUGH & OPERATIONS PLAYBOOK)

> **Dành cho:** Kỹ sư Dữ liệu (Data Engineers), Chuyên viên Phân tích (Data Analysts), Kiến trúc sư Hệ thống (Solution Architects) và Người vận hành (Ops).  
> **Mục tiêu:** Hướng dẫn thực hành từng bước khám phá toàn bộ 13 container, 37 bảng Iceberg, các luồng dữ liệu Medallion, cơ chế kiểm định chất lượng, giám sát và cảnh báo của Olist Data Lakehouse trên hệ điều hành Ubuntu / Linux.  
> **Nguyên tắc tài liệu:** Mọi mục đều đi kèm câu lệnh thực tế (CLI, Bash, SQL, Python, Docker) có thể sao chép và thực thi ngay lập tức; giải thích bản chất kiến trúc và các cạm bẫy kỹ thuật ngầm.  
> **Thời lượng tham quan dự kiến:** 4 - 6 giờ thực hành có hướng dẫn.  

---

## MỤC LỤC TỔNG QUAN

- [PHẦN 1: BẢN ĐỒ TỔNG THỂ & KHỞI TẠO HỆ THỐNG](#phần-1-bản-đồ-tổng-thể--khởi-tạo-hệ-thống)
  - [1.1. Bản đồ kiến trúc 13 Container & 37 Bảng Iceberg](#11-bản-đồ-kiến-trúc-13-container--37-bảng-iceberg)
  - [1.2. Chuẩn bị môi trường Ubuntu/Linux: Docker, Docker Compose, Python, Curl, Jq](#12-chuẩn-bị-môi-trường-ubuntulinux)
  - [1.3. Khởi động toàn cụm bằng `./scripts/up.sh`](#13-khởi-động-toàn-cụm-bằng-scriptsupsh)
  - [1.4. Kiểm tra sức khỏe toàn diện bằng `./scripts/verify_stack.sh`](#14-kiểm-tra-sức-khỏe-toàn-diện-bằng-scriptsverify_stacksh)
  - [1.5. Danh mục 8 Cổng Web UI và thông tin đăng nhập mặc định](#15-danh-mục-8-cổng-web-ui-và-thông-tin-đăng-nhập-mặc-định)
- [PHẦN 2: THAM QUAN TẦNG LƯU TRỮ ĐỐI TƯỢNG (MINIO S3 OBJECT STORAGE)](#phần-2-tham-quan-tầng-lưu-trữ-đối-tượng-minio-s3-object-storage)
  - [2.1. Cấu trúc Bucket S3: `raw`, `lakehouse`, `checkpoints`](#21-cấu-trúc-bucket-s3-raw-lakehouse-checkpoints)
  - [2.2. Khám phá 9 tệp CSV Olist nguồn trong `s3://raw/olist/`](#22-khám-phá-9-tệp-csv-olist-nguồn-trong-s3rawolist)
  - [2.3. Khám phá kho lưu trữ Apache Iceberg: Metadata, Manifest và Parquet files](#23-khám-phá-kho-lưu-trữ-apache-iceberg-metadata-manifest-và-parquet-files)
  - [2.4. Trực tiếp thao tác qua MinIO Client (`mc`) và Web UI Console](#24-trực-tiếp-thao-tác-qua-minio-client-mc-và-web-ui-console)
- [PHẦN 3: THAM QUAN METADATA CATALOG (POSTGRESQL & ICEBERG JDBC CATALOG)](#phần-3-tham-quan-metadata-catalog-postgresql--iceberg-jdbc-catalog)
  - [3.1. Vai trò của PostgreSQL trong hệ sinh thái Lakehouse](#31-vai-trò-của-postgresql-trong-hệ-sinh-thái-lakehouse)
  - [3.2. Truy cập PostgreSQL bằng `psql` CLI](#32-truy-cập-postgresql-bằng-psql-cli)
  - [3.3. Bảng `iceberg_tables` & `iceberg_namespace_properties`](#33-bảng-iceberg_tables--iceberg_namespace_properties)
  - [3.4. Bảng kiểm toán nghiệp vụ nội bộ (`control.*`)](#34-bảng-kiểm-toán-nghiệp-vụ-nội-bộ-control)
  - [3.5. Trực tiếp kiểm tra và truy vấn trạng thái run qua SQL](#35-trực-tiếp-kiểm-tra-và-truy-vấn-trạng-thái-run-qua-sql)
- [PHẦN 4: THAM QUAN ĐỘNG CƠ XỬ LÝ PHÂN TÁN (APACHE SPARK 3.5)](#phần-4-tham-quan-động-cơ-xử-lý-phân-tán-apache-spark-35)
  - [4.1. Kiến trúc Spark Master & Worker trong Docker](#41-kiến-trúc-spark-master--worker-trong-docker)
  - [4.2. Khám phá cấu hình `spark_session.py`: JDBC Catalog, S3A, và 8 cờ AQE](#42-khám-phá-cấu-hình-spark_sessionpy)
  - [4.3. Chạy tương tác với PySpark Shell trên cụm](#43-chạy-tương-tác-với-pyspark-shell-trên-cụm)
  - [4.4. Đọc dữ liệu Bronze, Silver, Gold qua DataFrame API](#44-đọc-dữ-liệu-bronze-silver-gold-qua-dataframe-api)
  - [4.5. Thực hành các tính năng độc quyền của Apache Iceberg qua Spark SQL](#45-thực-hành-các-tính-năng-độc-quyền-của-apache-iceberg-qua-spark-sql)
- [PHẦN 5: THAM QUAN TẦNG TRUY VẤN TỐC ĐỘ CAO (TRINO COORDINATOR)](#phần-5-tham-quan-tầng-truy-vấn-tốc-độ-cao-trino-coordinator)
  - [5.1. Khởi động và làm chủ Trino CLI](#51-khởi-động-và-làm-chủ-trino-cli)
  - [5.2. Khám phá cấu hình Connector Iceberg trong Trino](#52-khám-phá-cấu-hình-connector-iceberg-trong-trino)
  - [5.3. Truy vấn dữ liệu tầng Bronze (9 bảng Raw Ingestion)](#53-truy-vấn-dữ-liệu-tầng-bronze-9-bảng-raw-ingestion)
  - [5.4. Truy vấn dữ liệu tầng Silver (8 bảng Chuẩn hóa & Khử trùng lặp)](#54-truy-vấn-dữ-liệu-tầng-silver-8-bảng-chuẩn-hóa--khử-trùng-lặp)
  - [5.5. Truy vấn dữ liệu tầng Gold Star Schema: Facts & Dimensions](#55-truy-vấn-dữ-liệu-tầng-gold-star-schema-facts--dimensions)
  - [5.6. Truy vấn 4 Semantic Views phục vụ BI](#56-truy-vấn-4-semantic-views-phục-vụ-bi)
  - [5.7. Benchmark hiệu năng truy vấn Trino và File-Skipping](#57-benchmark-hiệu-năng-truy-vấn-trino-và-file-skipping)
- [PHẦN 6: THAM QUAN TẦNG ĐIỀU PHỐI QUY TRÌNH (APACHE AIRFLOW)](#phần-6-tham-quan-tầng-điều-phối-quy-trình-apache-airflow)
  - [6.1. Tổng quan giao diện Airflow Web UI](#61-tổng-quan-giao-diện-airflow-web-ui)
  - [6.2. Mổ xẻ 4 DAG nghiệp vụ cốt lõi](#62-mổ-xẻ-4-dag-nghiệp-vụ-cốt-lõi)
  - [6.3. Làm chủ Airflow CLI từ dòng lệnh Ubuntu](#63-làm-chủ-airflow-cli-từ-dòng-lệnh-ubuntu)
  - [6.4. Cơ chế điều tiết ghi: Airflow Pool `lakehouse_writes` và `max_active_runs`](#64-cơ-chế-điều-tiết-ghi-airflow-pool-lakehouse_writes-và-max_active_runs)
- [PHẦN 7: THAM QUAN CƠ CHẾ KIỂM ĐỊNH CHẤT LƯỢNG (GREAT EXPECTATIONS & QUARANTINE)](#phần-7-tham-quan-cơ-chế-kiểm-định-chất-lượng-great-expectations--quarantine)
  - [7.1. Triết lý Data Contracts trong Medallion Architecture](#71-triết-lý-data-contracts-trong-medallion-architecture)
  - [7.2. 198 phép kiểm định chất lượng tự động](#72-198-phép-kiểm-định-chất-lượng-tự-động)
  - [7.3. Phân biệt cấp độ nghiêm trọng: WARN vs ERROR](#73-phân-biệt-cấp-độ-nghiêm-trọng-warn-vs-error)
  - [7.4. Khám phá tầng cách ly (Quarantine Layer)](#74-khám-phá-tầng-cách-ly-quarantine-layer)
  - [7.5. Phân tích 197 đơn hàng lỗi thời gian trong `quarantine.orders`](#75-phân-tích-197-đơn-hàng-lỗi-thời-gian-trong-quarantineorders)
  - [7.6. Truy vấn lịch sử kiểm định từ bảng `quality.quality_results`](#76-truy-vấn-lịch-sử-kiểm-định-từ-bảng-qualityquality_results)
- [PHẦN 8: THAM QUAN HỆ THỐNG CẢNH BÁO SỰ CỐ (SMTP & MAILHOG)](#phần-8-tham-quan-hệ-thống-cảnh-báo-sự-cố-smtp--mailhog)
  - [8.1. Cơ chế cảnh báo lỗi tự động `email_alert.py`](#81-cơ-chế-cảnh-báo-lỗi-tự-động-email_alertpy)
  - [8.2. Mở giao diện Hộp thư sự cố MailHog](#82-mở-giao-diện-hộp-thư-sự-cố-mailhog)
  - [8.3. Thử nghiệm bắn email cảnh báo độc lập qua CLI](#83-thử-nghiệm-bắn-email-cảnh-báo-độc-lập-qua-cli)
  - [8.4. Thử nghiệm kích hoạt lỗi thật trong pipeline để nhận email](#84-thử-nghiệm-kích-hoạt-lỗi-thật-trong-pipeline-để-nhận-email)
- [PHẦN 9: THAM QUAN TẦNG TRỰC QUAN HÓA & PHÂN TÍCH KINH DOANH (APACHE SUPERSET)](#phần-9-tham-quan-tầng-trực-quan-hóa--phân-tích-kinh-doanh-apache-superset)
  - [9.1. Đăng nhập Superset Web UI và kết nối Database](#91-đăng-nhập-superset-web-ui-và-kết-nối-database)
  - [9.2. Tham quan Dashboard 1: Tổng quan Doanh thu & Đơn hàng](#92-tham-quan-dashboard-1-tổng-quan-doanh-thu--đơn-hàng)
  - [9.3. Tham quan Dashboard 2: Phễu Vòng đời Đơn hàng (Order Lifecycle Funnel)](#93-tham-quan-dashboard-2-phễu-vòng-đời-đơn-hàng-order-lifecycle-funnel)
  - [9.4. Tham quan Dashboard 3: Phân tích Khách hàng & Địa lý](#94-tham-quan-dashboard-3-phân-tích-khách-hàng--địa-lý)
  - [9.5. Tham quan Dashboard 4: Vận hành Giao hàng & Người bán](#95-tham-quan-dashboard-4-vận-hành-giao-hàng--người-bán)
  - [9.6. Tham quan Dashboard 5: Đánh giá Đơn hàng & Sentiment](#96-tham-quan-dashboard-5-đánh-giá-đơn-hàng--sentiment)
- [PHẦN 10: THAM QUAN TẦNG GIÁM SÁT THỜI GIAN THỰC (PROMETHEUS & GRAFANA)](#phần-10-tham-quan-tầng-giám-sát-thời-gian-thực-prometheus--grafana)
  - [10.1. Khám phá 10/10 Prometheus Scrape Targets](#101-khám-phá-1010-prometheus-scrape-targets)
  - [10.2. Các câu lệnh PromQL trọng yếu giám sát Lakehouse](#102-các-câu-lệnh-promql-trọng-yếu-giám-sát-lakehouse)
  - [10.3. Tham quan Dashboard Grafana `olist-pipeline`](#103-tham-quan-dashboard-grafana-olist-pipeline)
  - [10.4. Tham quan Dashboard Grafana `docker-monitoring`](#104-tham-quan-dashboard-grafana-docker-monitoring)
- [PHẦN 11: THỰC HÀNH CÁC KỊCH BẢN NÂNG CAO (CHAOS TESTING & BENCHMARK)](#phần-11-thực-hành-các-kịch-bản-nâng-cao-chaos-testing--benchmark)
  - [11.1. Chạy một bài đo Benchmark đơn lẻ bằng Python](#111-chạy-một-bài-đo-benchmark-đơn-lẻ-bằng-python)
  - [11.2. Thực hành tiêm lỗi Dữ liệu bẩn `04_dirty_metadata`](#112-thực-hành-tiêm-lỗi-dữ-liệu-bẩn-04_dirty_metadata)
  - [11.3. Thực hành tiêm lỗi Vi phạm khóa `05_integrity_violations`](#113-thực-hành-tiêm-lỗi-vi-phạm-khóa-05_integrity_violations)
  - [11.4. Thực hành Nén tệp tự động (Iceberg Compaction)](#114-thực-hành-nén-tệp-tự-động-iceberg-compaction)
  - [11.5. Phục hồi Baseline nguyên bản và đối soát doanh thu từng cent](#115-phục-hồi-baseline-nguyên-bản-và-đối-soát-doanh-thu-từng-cent)
- [PHẦN 12: BẢNG TRA CỨU COMMAND TOÀN TẬP (CHEATSHEET & TROUBLESHOOTING)](#phần-12-bảng-tra-cứu-command-toàn-tập-cheatsheet--troubleshooting)
  - [12.1. Cheatsheet Docker & Compose](#121-cheatsheet-docker--compose)
  - [12.2. Cheatsheet Airflow CLI](#122-cheatsheet-airflow-cli)
  - [12.3. Cheatsheet Trino SQL](#123-cheatsheet-trino-sql)
  - [12.4. Bảng chẩn đoán và khắc phục sự cố (Troubleshooting Matrix)](#124-bảng-chẩn-đoán-và-khắc-phục-sự-cố-troubleshooting-matrix)
  - [12.5. Lời kết và các bước tiếp theo](#125-lời-kết-và-các-bước-tiếp-theo)

---

# PHẦN 1: BẢN ĐỒ TỔNG THỂ & KHỞI TẠO HỆ THỐNG

## 1.1. Bản đồ kiến trúc 13 Container & 37 Bảng Iceberg

Hệ sinh thái Olist Data Lakehouse được thiết kế theo kiến trúc **Medallion Lakehouse** trên nền tảng Docker Container, tích hợp định dạng bảng **Apache Iceberg**, động cơ tính toán phân tán **Apache Spark 3.5**, công cụ truy vấn phân tích tương tác **Trino 435**, hệ thống điều phối **Apache Airflow 2.10**, nền tảng BI **Apache Superset 4.0**, và bộ giám sát thời gian thực **Prometheus / Grafana**.

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   KIẾN TRÚC OLIST DATA LAKEHOUSE (13 CONTAINERS)                        │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                        │
│   [ NGUỒN CSV THÔ ]                                                                                    │
│          │                                                                                             │
│          ▼                                                                                             │
│   ┌───────────────┐        ┌────────────────────────┐        ┌─────────────────────────────────────┐   │
│   │   MinIO S3    │◄──────►│  PostgreSQL Metadata   │◄──────►│       Apache Iceberg Warehouse      │   │
│   │  Object Store │        │  (Iceberg JDBC Catalog)│        │   (s3://lakehouse/ - 37 tables)     │   │
│   │    (:9000)    │        │        (:5432)         │        │  bronze.* | silver.* | gold.*       │   │
│   └───────┬───────┘        └───────────┬────────────┘        └──────────────────┬──────────────────┘   │
│           │                            │                                        │                      │
│           │                            │                                        │                      │
│           ▼                            ▼                                        ▼                      │
│   ┌────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                   TẦNG TÍNH TOÁN & ĐIỀU PHỐI                                   │   │
│   │  ┌─────────────────────────┐     ┌────────────────────────┐     ┌───────────────────────────┐  │   │
│   │  │   Spark Master (:7077)  │◄───►│ Airflow Scheduler      │◄───►│ Airflow Triggerer         │  │   │
│   │  │   Spark Worker (:8081)  │     │ Airflow Webserver(:8083│     │ (Failure Callbacks/Alert) │  │   │
│   │  └─────────────────────────┘     └────────────────────────┘     └───────────────────────────┘  │   │
│   └───────────────────────────────────────────────────┬────────────────────────────────────────────┘   │
│                                                       │                                                │
│                                                       ▼                                                │
│   ┌────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                   TẦNG PHỤC VỤ PHÂN TÍCH (SERVING)                             │   │
│   │  ┌────────────────────────┐                   ┌─────────────────────────────────────────────┐  │   │
│   │  │ Trino Coordinator      │◄──────────────────┤ Apache Superset BI (:8088)                  │  │   │
│   │  │ (:8082 - Iceberg Push) │                   │ (5 Dashboards, Cross-filtering, Charts)     │  │   │
│   │  └────────────────────────┘                   └─────────────────────────────────────────────┘  │   │
│   └───────────────────────────────────────────────────┬────────────────────────────────────────────┘   │
│                                                       │                                                │
│                                                       ▼                                                │
│   ┌────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                   TẦNG GIÁM SÁT & CẢNH BÁO                                     │   │
│   │  ┌─────────────────┐   ┌──────────────────┐   ┌─────────────────┐   ┌───────────────────────┐  │   │
│   │  │ Prometheus      │   │ Grafana Dashboards│  │ Exporters (x3)  │   │ MailHog SMTP Box      │  │   │
│   │  │ (:9090)         │◄──┤ (:3000)          │   │ (:9100/:9101)   │   │ (:8025 / :1025)       │  │   │
│   │  └─────────────────┘   └──────────────────┘   └─────────────────┘   └───────────────────────┘  │   │
│   └────────────────────────────────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Phân bổ 37 bảng Iceberg trong Lakehouse:
1. **Tầng Bronze (9 bảng):** Dữ liệu thu nhận nguyên bản từ CSV nguồn (`bronze.orders`, `bronze.order_items`, `bronze.order_payments`, `bronze.order_reviews`, `bronze.products`, `bronze.sellers`, `bronze.customers`, `bronze.geolocation`, `bronze.product_category_name_translation`).
2. **Tầng Silver (8 bảng):** Dữ liệu đã làm sạch, ép kiểu, khử trùng lặp và phân tích lỗi (`silver.orders`, `silver.order_items`, `silver.order_payments`, `silver.order_reviews`, `silver.products`, `silver.sellers`, `silver.customers`, `silver.geography`).
3. **Tầng Gold Star Schema (14 bảng):**
   - 4 Bảng Fact: `fact_orders`, `fact_order_items`, `fact_order_payments`, `fact_order_reviews`.
   - 4 Bảng Dimension: `dim_customer`, `dim_product`, `dim_seller`, `dim_date`, `dim_geography`.
   - 6 Bảng Data Marts: `mart_revenue_daily`, `mart_seller_monthly`, `mart_customer_retention`, `mart_delivery_performance`, `mart_product_category_growth`, `mart_order_lifecycle`.
4. **Tầng Kiểm soát & Cách ly (6 bảng):**
   - Tầng Kiểm soát: `control.pipeline_runs`, `control.ingestion_batches`, `control.transformation_targets`, `control.maintenance_results`.
   - Tầng Cách ly (Quarantine): `quarantine.orders`, `quarantine.geography`.
   - Tầng Kiểm định: `quality.quality_results`.

---

## 1.2. Chuẩn bị môi trường Ubuntu/Linux

Để vận hành toàn bộ hệ thống trên Ubuntu (20.04 LTS, 22.04 LTS hoặc 24.04 LTS), đảm bảo các gói công cụ sau đã được cài đặt:

```bash
# Cập nhật danh mục gói phần mềm
sudo apt-get update -y

# Cài đặt các tiện ích CLI cần thiết
sudo apt-get install -y curl jq tree git bsdmainutils python3 python3-pip

# Kiểm tra Docker Engine và Docker Compose (yêu cầu Docker Compose v2)
docker --version
docker compose version

# Đảm bảo người dùng hiện tại nằm trong nhóm docker (tránh phải gõ sudo)
sudo usermod -aG docker $USER
# (Nếu vừa thêm nhóm, hãy chạy `newgrp docker` hoặc đăng xuất rồi đăng nhập lại)
```

Kiểm tra cấu hình bộ nhớ ảo cho Elasticsearch / Trino:
```bash
# Kiểm tra tham số max_map_count (tối thiểu 262144)
sysctl vm.max_map_count

# Nếu nhỏ hơn, cấu hình tức thời:
sudo sysctl -w vm.max_map_count=262144
```

Di chuyển vào thư mục dự án `rebuild`:
```bash
cd /run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild

# Liệt kê các script vận hành
ls -la scripts/
```

---

## 1.3. Khởi động toàn cụm bằng `./scripts/up.sh`

Script [`scripts/up.sh`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/scripts/up.sh) tự động kết hợp đủ 5 file `docker-compose*.yml` với cờ `--wait` và `--wait-timeout 900`. Docker Compose sẽ tự động theo dõi toàn bộ trạng thái healthcheck của từng container và chỉ trả lại quyền điều khiển khi 100% dịch vụ đã chuyển sang trạng thái `healthy`.

```bash
# Chạy khởi động bình thường
./scripts/up.sh
```

Nếu muốn build lại image từ Dockerfile cục bộ:
```bash
./scripts/up.sh --build
```

Nếu máy có tốc độ ổ đĩa chậm, tăng thời hạn chờ (timeout):
```bash
./scripts/up.sh --timeout-seconds 1200
```

**Đầu ra kỳ vọng khi hoàn thành:**
```text
Dang khoi dong Olist Data Lakehouse (han cho 900 giay)...
[+] Running 13/13
 ✔ Container olist-lakehouse-minio-1             Healthy
 ✔ Container olist-lakehouse-postgres-1          Healthy
 ✔ Container olist-lakehouse-spark-master-1      Healthy
 ✔ Container olist-lakehouse-spark-worker-1      Healthy
 ✔ Container olist-lakehouse-trino-1             Healthy
 ✔ Container olist-lakehouse-airflow-scheduler-1 Healthy
 ✔ Container olist-lakehouse-airflow-webserver-1 Healthy
 ✔ Container olist-lakehouse-airflow-triggerer-1 Healthy
 ✔ Container olist-lakehouse-superset-1          Healthy
 ✔ Container olist-lakehouse-prometheus-1        Healthy
 ✔ Container olist-lakehouse-grafana-1           Healthy
 ✔ Container olist-lakehouse-mailhog-1           Healthy
 ✔ Container olist-lakehouse-pipeline-exporter-1 Healthy
```

---

## 1.4. Kiểm tra sức khỏe toàn diện bằng `./scripts/verify_stack.sh`

Sau khi khởi động, hãy chạy script [`scripts/verify_stack.sh`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/scripts/verify_stack.sh). Script này thực hiện 5 trạm kiểm tra nghiêm ngặt trong 60 giây:
1. Trạng thái hoạt động và healthcheck của 13 containers.
2. Kiểm tra mã phản hồi HTTP 200 của 7 Web UI endpoints.
3. Kiểm tra 4 DAG trong Airflow Scheduler và đảm bảo không có lỗi import DAG.
4. Chạy truy vấn SQL qua Trino đối soát số lượng đơn hàng và doanh thu tầng Gold.
5. Kiểm tra 10/10 scrape targets của Prometheus đang ở trạng thái `UP`.

```bash
./scripts/verify_stack.sh
```

**Đầu ra chuẩn kỳ vọng:**
```text
=== 1. TRANG THAI CONTAINER ===
  [PASS] olist-lakehouse-minio-1                      running healthy
  [PASS] olist-lakehouse-postgres-1                   running healthy
  [PASS] olist-lakehouse-spark-master-1               running healthy
  [PASS] olist-lakehouse-spark-worker-1               running healthy
  [PASS] olist-lakehouse-trino-1                      running healthy
  [PASS] olist-lakehouse-airflow-scheduler-1          running healthy
  [PASS] olist-lakehouse-airflow-webserver-1          running healthy
  [PASS] olist-lakehouse-airflow-triggerer-1          running healthy
  [PASS] olist-lakehouse-superset-1                   running healthy
  [PASS] olist-lakehouse-prometheus-1                 running healthy
  [PASS] olist-lakehouse-grafana-1                    running healthy
  [PASS] olist-lakehouse-mailhog-1                    running healthy
  [PASS] olist-lakehouse-pipeline-exporter-1          running healthy

=== 2. CONG WEB UI ===
  [PASS] Airflow                                      HTTP 200
  [PASS] Trino                                        HTTP 200
  [PASS] Superset                                     HTTP 200
  [PASS] MinIO                                        HTTP 200
  [PASS] Prometheus                                   HTTP 200
  [PASS] Grafana                                      HTTP 200
  [PASS] MailHog                                      HTTP 200

=== 3. AIRFLOW DAGS ===
  [PASS] olist_daily_pipeline                         da nap
  [PASS] olist_transformation                         da nap
  [PASS] olist_quality                                da nap
  [PASS] olist_maintenance                            da nap
  [PASS] Khong co loi import DAG                      

=== 4. DOI SOAT DU LIEU QUA TRINO ===
  [PASS] fact_orders: so don hang                     99441 (ky vong 99441)
  [PASS] fact_orders: doanh thu hang                  13591643.70 (ky vong 13591643.70)
  [PASS] fact_orders: cuoc van chuyen                 2251909.54 (ky vong 2251909.54)
  [PASS] fact_orders: tong thanh toan                 16008872.12 (ky vong 16008872.12)

=== 5. PROMETHEUS SCRAPE TARGETS ===
  [PASS] Scrape targets UP                            10/10

KET QUA: TAT CA KIEM TRA DEU PASS.
```

---

## 1.5. Danh mục 8 Cổng Web UI và thông tin đăng nhập mặc định

Bảng tham chiếu mở các giao diện Web UI trên trình duyệt:

| Giao diện dịch vụ | Cổng Host | URL truy cập | Tài khoản mặc định | Mật khẩu mặc định | Ghi chú vận hành |
|:---|:---:|:---|:---:|:---:|:---|
| **Airflow UI** | `8083` | [http://localhost:8083](http://localhost:8083) | `admin` | `admin` | Điều phối và kích hoạt DAGs |
| **Trino Coordinator** | `8082` | [http://localhost:8082](http://localhost:8082) | `trino` | *(không cần mật khẩu)* | Giám sát các câu truy vấn SQL đang chạy |
| **Apache Superset** | `8088` | [http://localhost:8088](http://localhost:8088) | `olist_admin` | `admin` | Khám phá 5 dashboard BI |
| **MinIO Console** | `9001` | [http://localhost:9001](http://localhost:9001) | `root` | `admin_root` | Quản lý S3 Buckets & Tệp Parquet |
| **Grafana Monitoring**| `3000` | [http://localhost:3000](http://localhost:3000) | `admin` | `admin` | Giám sát tài nguyên thời gian thực |
| **Prometheus Engine** | `9090` | [http://localhost:9090](http://localhost:9090) | *(công khai)* | *(không cần)* | Tra cứu metrics qua PromQL |
| **MailHog UI** | `8025` | [http://localhost:8025](http://localhost:8025) | *(công khai)* | *(không cần)* | Hộp thư xem email cảnh báo sự cố |
| **Spark Master UI** | `8084` | [http://localhost:8084](http://localhost:8084) | *(công khai)* | *(không cần)* | Xem trạng thái Spark Worker, Cores, Memory |

---

# PHẦN 2: THAM QUAN TẦNG LƯU TRỮ ĐỐI TƯỢNG (MINIO S3 OBJECT STORAGE)

Tầng lưu trữ đối tượng MinIO đóng vai trò là kho dữ liệu trung tâm (Data Lake Storage). Toàn bộ dữ liệu thô (Raw CSV) và dữ liệu các tầng Medallion (Parquet + Metadata Iceberg) đều nằm tại đây.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        CẤU TRÚC MINIO S3 STORAGE                       │
├────────────────────────────────────────────────────────────────────────┤
│  s3://raw/                                                             │
│    └── olist/                                                          │
│          ├── olist_orders_dataset.csv                                  │
│          ├── olist_order_items_dataset.csv                             │
│          └── ... (9 tệp CSV gốc)                                       │
│                                                                        │
│  s3://lakehouse/ (Warehouse của Apache Iceberg)                        │
│    ├── bronze/                                                         │
│    │     └── <tên_bảng>/                                               │
│    │           ├── metadata/ (v1.metadata.json, snap-*.avro)           │
│    │           └── data/ (*.parquet)                                   │
│    ├── silver/                                                         │
│    │     └── <tên_bảng>/                                               │
│    ├── gold/                                                           │
│    │     └── <tên_bảng>/                                               │
│    ├── control/                                                        │
│    └── quarantine/                                                     │
│                                                                        │
│  s3://checkpoints/                                                     │
│    └── spark/ (Dành cho Structured Streaming & checkpointing)          │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2.1. Cấu trúc Bucket S3: `raw`, `lakehouse`, `checkpoints`

Để khám phá MinIO từ dòng lệnh Ubuntu, ta có thể sử dụng tiện ích MinIO Client (`mc`) được tích hợp sẵn bên trong container `olist-lakehouse-minio-1`.

Cấu hình alias kết nối MinIO bên trong container:
```bash
docker exec -it olist-lakehouse-minio-1 mc alias set local http://localhost:9000 root admin_root
```

Liệt kê danh sách các Buckets hiện có:
```bash
docker exec -it olist-lakehouse-minio-1 mc ls local/
```

**Kết quả kỳ vọng:**
```text
[2026-11-06 00:00:00 UTC]     0B checkpoints/
[2026-11-06 00:00:00 UTC]     0B lakehouse/
[2026-11-06 00:00:00 UTC]     0B raw/
```

Kiểm tra dung lượng tổng thể của từng bucket:
```bash
docker exec -it olist-lakehouse-minio-1 mc du local/raw
docker exec -it olist-lakehouse-minio-1 mc du local/lakehouse
```

---

## 2.2. Khám phá 9 tệp CSV Olist nguồn trong `s3://raw/olist/`

Toàn bộ dữ liệu thô của bài toán thương mại điện tử Olist gồm 9 file CSV. Hãy kiểm tra danh sách và dung lượng chi tiết của từng file:

```bash
docker exec -it olist-lakehouse-minio-1 mc ls local/raw/olist/
```

**Kết quả chi tiết:**
```text
[2026-11-06 00:00:00 UTC]  8.6MiB olist_customers_dataset.csv
[2026-11-06 00:00:00 UTC]   59MiB olist_geolocation_dataset.csv
[2026-11-06 00:00:00 UTC]   15MiB olist_order_items_dataset.csv
[2026-11-06 00:00:00 UTC]  5.5MiB olist_order_payments_dataset.csv
[2026-11-06 00:00:00 UTC]   14MiB olist_order_reviews_dataset.csv
[2026-11-06 00:00:00 UTC]   17MiB olist_orders_dataset.csv
[2026-11-06 00:00:00 UTC]  2.3MiB olist_products_dataset.csv
[2026-11-06 00:00:00 UTC]  2.5KiB product_category_name_translation.csv
[2026-11-06 00:00:00 UTC]  170KiB olist_sellers_dataset.csv
```

Xem thử 5 dòng đầu tiên của file đơn hàng `olist_orders_dataset.csv`:
```bash
docker exec -it olist-lakehouse-minio-1 mc head -n 5 local/raw/olist/olist_orders_dataset.csv
```

**Dữ liệu hiển thị:**
```csv
order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date
e481f51cbdc54678b7cc49136f2d6af7,9ef432eb6251297304e76186b10a928d,delivered,2017-10-02 10:56:33,2017-10-02 11:07:15,2017-10-04 19:55:00,2017-10-10 21:25:13,2017-10-18 00:00:00
53cdb2fc8bc7dce0b60272928df32032,b0830fb4747a6c6d20dea0b8c802d77f,delivered,2018-07-24 20:41:37,2018-07-26 03:24:27,2018-07-26 14:31:00,2018-08-07 15:27:45,2018-08-13 00:00:00
47770eb9100c2d0c44946d9cf07ec65d,41ce2a54c0b03bf3443c3d931a367089,delivered,2018-08-08 08:38:49,2018-08-08 08:55:23,2018-08-08 13:50:00,2018-08-17 18:06:29,2018-09-04 00:00:00
949d5b44dbf5de918fe9c16f97b45f8a,f88197465ea7920adcdbec7375364d82,delivered,2017-11-18 19:28:06,2017-11-18 19:45:59,2017-11-22 13:39:59,2017-12-02 00:28:42,2017-12-15 00:00:00
```

---

## 2.3. Khám phá kho lưu trữ Apache Iceberg: Metadata, Manifest và Parquet files

Điểm khác biệt cốt lõi giữa **Data Lake thông thường (Hive-style)** và **Data Lakehouse (Apache Iceberg)** là cách quản lý tệp dữ liệu:
* Hive dựa vào cấu trúc thư mục đĩa để phân vùng (`/year=2018/month=08/`).
* Iceberg quản lý trạng thái bảng thông qua **cây phân cấp Metadata (Metadata Tree)** gồm 3 tầng:
  1. **Table Metadata (`*.metadata.json`):** Chứa Schema hiện tại, Partition Spec, Snapshot Log.
  2. **Manifest List (`snap-*.avro`):** Chứa danh sách các Manifest Files ứng với mỗi Snapshot.
  3. **Manifest File (`*-m0.avro`):** Chứa danh sách cụ thể từng Data File (Parquet) cùng số liệu thống kê min/max, null count của từng cột để phục vụ cơ chế File-Skipping.

Khám phá cấu trúc thư mục của bảng `gold.fact_orders`:
```bash
docker exec -it olist-lakehouse-minio-1 mc ls local/lakehouse/gold/fact_orders/
```

Liệt kê thư mục metadata:
```bash
docker exec -it olist-lakehouse-minio-1 mc ls local/lakehouse/gold/fact_orders/metadata/
```

**Bạn sẽ thấy các tệp đại diện:**
```text
[2026-11-06 15:00:00 UTC]  15KiB 00001-a1b2c3d4-e5f6-7890.metadata.json
[2026-11-06 15:00:00 UTC]  6.2KiB snap-1234567890123456789-1-a1b2.avro
[2026-11-06 15:00:00 UTC]  8.4KiB a1b2c3d4-m0.avro
```

Liệt kê các tệp dữ liệu Parquet thực tế:
```bash
docker exec -it olist-lakehouse-minio-1 mc ls local/lakehouse/gold/fact_orders/data/
```

Kiểm tra một tệp JSON metadata để xem Schema và Snapshot ID:
```bash
# Đọc 30 dòng đầu của file metadata.json mới nhất
docker exec -it olist-lakehouse-minio-1 sh -c '
META_FILE=$(mc ls local/lakehouse/gold/fact_orders/metadata/ | grep "\.metadata\.json" | tail -n 1 | awk "{print \$NF}")
mc cat "local/lakehouse/gold/fact_orders/metadata/$META_FILE"
' | head -n 35
```

---

## 2.4. Trực tiếp thao tác qua MinIO Client (`mc`) và Web UI Console

Bạn có thể mở trình duyệt và truy cập vào [http://localhost:9001](http://localhost:9001) với:
* **Username:** `root`
* **Password:** `admin_root`

Tại đây bạn có thể:
1. Duyệt cấu trúc cây thư mục trong bucket `lakehouse`.
2. Tải về máy một tệp Parquet bất kỳ trong `gold/fact_orders/data/`.
3. Kiểm tra kích thước chính xác và thời gian ghi của từng snapshot.

---

# PHẦN 3: THAM QUAN METADATA CATALOG (POSTGRESQL & ICEBERG JDBC CATALOG)

Trong kiến trúc Lakehouse này, PostgreSQL đảm nhiệm vai trò trung tâm lưu trữ toàn bộ Catalog của Apache Iceberg (thay cho Hive Metastore cồng kềnh) cùng với database nội bộ của Airflow và Superset.

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                          CƠ SỞ DỮ LIỆU POSTGRESQL 16                             │
├──────────────────────┬──────────────────────┬────────────────────────────────────┤
│ Database Name        │ Owner / User         │ Mục đích sử dụng                   │
├──────────────────────┼──────────────────────┼────────────────────────────────────┤
│ `iceberg`            │ `iceberg`            │ JDBC Catalog lưu con trỏ bảng      │
│ `airflow`            │ `airflow`            │ Metadata trạng thái DAG/Task runs  │
│ `superset`           │ `superset`           │ Dashboards, Slices, BI User roles  │
└──────────────────────┴──────────────────────┴────────────────────────────────────┘
```

---

## 3.1. Vai trò của PostgreSQL trong hệ sinh thái Lakehouse

Khi một job PySpark hoặc một câu truy vấn Trino gọi bảng `lakehouse.gold.fact_orders`:
1. Engine kết nối tới PostgreSQL database `iceberg` qua giao thức JDBC (`jdbc:postgresql://postgres:5432/iceberg`).
2. Tra cứu bảng `iceberg_tables` để lấy chuỗi đường dẫn URI trỏ tới tệp `*.metadata.json` hiện hành trên MinIO S3 (`s3://lakehouse/gold/fact_orders/metadata/0000x.metadata.json`).
3. Đọc tệp metadata đó từ MinIO để biết vị trí các tệp Parquet cần quét.

> Nhờ cơ chế này, PostgreSQL **chỉ lưu trữ con trỏ metadata** (kích thước vài Kilobyte), toàn bộ dữ liệu nặng đều nằm trên MinIO S3.

---

## 3.2. Truy cập PostgreSQL bằng `psql` CLI

Để tương tác trực tiếp với PostgreSQL, sử dụng lệnh `docker exec`:

```bash
# Truy cập vào database `iceberg` bằng người dùng postgres
docker exec -it olist-lakehouse-postgres-1 psql -U postgres -d iceberg
```

Liệt kê các bảng trong database `iceberg`:
```sql
\dt
```

**Kết quả:**
```text
              List of relations
 Schema |            Name             | Type  |  Owner  
--------+-----------------------------+-------+---------
 public | iceberg_namespace_properties| table | iceberg
 public | iceberg_tables              | table | iceberg
(2 rows)
```

Thoát khỏi `psql` bằng lệnh:
```sql
\q
```

---

## 3.3. Bảng `iceberg_tables` & `iceberg_namespace_properties`

Kiểm tra toàn bộ 37 bảng Iceberg đã được đăng ký trong Catalog:

```bash
docker exec -it olist-lakehouse-postgres-1 psql -U postgres -d iceberg -c "
SELECT table_namespace, table_name, metadata_location 
FROM iceberg_tables 
ORDER BY table_namespace, table_name;
"
```

**Trích xuất một phần kết quả thực tế:**
```text
  table_namespace   |              table_name              |                               metadata_location                               
--------------------+--------------------------------------+-------------------------------------------------------------------------------
 bronze             | customers                            | s3://lakehouse/bronze/customers/metadata/00001-customers.metadata.json
 bronze             | orders                               | s3://lakehouse/bronze/orders/metadata/00001-orders.metadata.json
 control            | ingestion_batches                    | s3://lakehouse/control/ingestion_batches/metadata/00099-batches.metadata.json
 control            | pipeline_runs                        | s3://lakehouse/control/pipeline_runs/metadata/00136-runs.metadata.json
 gold               | dim_customer                         | s3://lakehouse/gold/dim_customer/metadata/00001-dim_cust.metadata.json
 gold               | fact_orders                          | s3://lakehouse/gold/fact_orders/metadata/00003-fact_orders.metadata.json
 quarantine         | orders                               | s3://lakehouse/quarantine/orders/metadata/00005-quar_orders.metadata.json
 silver             | orders                               | s3://lakehouse/silver/orders/metadata/00002-silver_orders.metadata.json
(37 rows)
```

Kiểm tra các Namespace (tương đương Schema/Database trong Data Lakehouse):
```bash
docker exec -it olist-lakehouse-postgres-1 psql -U postgres -d iceberg -c "
SELECT DISTINCT table_namespace FROM iceberg_tables ORDER BY table_namespace;
"
```

**Các Namespace chuẩn:**
* `bronze`
* `silver`
* `gold`
* `control`
* `quarantine`
* `quality`

---

## 3.4. Bảng kiểm toán nghiệp vụ nội bộ (`control.*`)

Tầng Control được xây dựng nhằm mục đích kiểm toán (Audit Trail) và theo dõi tính bất biến (Idempotency) của pipeline. Hãy kiểm tra bảng `control.pipeline_runs` và `control.transformation_targets`:

```bash
# Kiểm tra 5 lần chạy pipeline gần nhất qua Trino
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT run_id, pipeline_name, status, started_at, completed_at 
FROM lakehouse.control.pipeline_runs 
ORDER BY started_at DESC 
LIMIT 5;
"
```

---

## 3.5. Trực tiếp kiểm tra và truy vấn trạng thái run qua SQL

Kiểm tra các bảng mục tiêu đã được biến đổi trong đợt chạy gần nhất:
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT target_table, layer, status, rows_written, duration_seconds 
FROM lakehouse.control.transformation_targets 
ORDER BY completed_at DESC 
LIMIT 10;
"
```

---

# PHẦN 4: THAM QUAN ĐỘNG CƠ XỬ LÝ PHÂN TÁN (APACHE SPARK 3.5)

Apache Spark 3.5 là động cơ tính toán chủ lực thực hiện toàn bộ việc đọc CSV, chuẩn hóa kiểu dữ liệu, khử trùng lặp và biến đổi thành Star Schema.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              CẤU TRÚC PHÂN TÁN SPARK                                   │
├────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                        │
│   Spark Master (:7077)  ◄──── Kết nối Driver từ Airflow (Deploy mode: client)          │
│         │                                                                              │
│         ├── Executor 1 (Spark Worker - 2 Cores, 4 GiB RAM)                             │
│         │     ├── Task 1: Ingest Bronze                                                │
│         │     ├── Task 2: Broadcast Join dim_date (In-Memory Hash Join)                │
│         │     └── Task 3: Adaptive Coalesce Partitions (Gộp phân vùng sau Shuffle)     │
│         │                                                                              │
│         └── Ghi xuống MinIO S3 bằng S3A FileSystem & S3FileIO                          │
│                                                                                        │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4.1. Kiến trúc Spark Master & Worker trong Docker

Mở trình duyệt truy cập vào [http://localhost:8084](http://localhost:8084) để xem giao diện Spark Master Web UI:
* **URL Master:** `spark://spark-master:7077`
* **Workers:** 1 Worker (`olist-lakehouse-spark-worker-1`)
* **Cores:** 2 Cores
* **Memory:** 4.0 GiB (hoặc cấu hình theo biến môi trường)

Kiểm tra log của Spark Master và Worker từ dòng lệnh Ubuntu:
```bash
./scripts/logs.sh spark-master --tail 20
./scripts/logs.sh spark-worker --tail 20
```

---

## 4.2. Khám phá cấu hình `spark_session.py`

Mở file [`transformation/common/spark_session.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/transformation/common/spark_session.py#L56-L98) để quan sát 8 cấu hình Adaptive Query Execution (AQE) đã được tinh chỉnh:

```bash
sed -n '56,98p' transformation/common/spark_session.py
```

**Bản chất các cờ tối ưu này:**
1. `spark.sql.adaptive.enabled = true`: Cho phép Spark sửa đổi kế hoạch thực thi ngay trong lúc chạy dựa trên thống kê thực tế sau mỗi shuffle stage.
2. `spark.sql.adaptive.coalescePartitions.enabled = true`: Tự động gộp các partition nhỏ sau shuffle, chống lãng phí task rỗng.
3. `spark.sql.adaptive.advisoryPartitionSizeInBytes = 64MB`: Cỡ partition mục tiêu phù hợp với máy 2 cores / 4 GiB.
4. `spark.sql.adaptive.skewJoin.enabled = true`: Tự động chia nhỏ partition bị lệch (skew) khi gặp sự kiện Flash-Sale.
5. `spark.sql.shuffle.partitions = 32`: Nâng từ 4 lên 32 để AQE có đủ nguyên liệu co giãn 2 chiều.
6. `spark.sql.autoBroadcastJoinThreshold = 52428800`: Ngưỡng tự động broadcast 50 MB.

---

## 4.3. Chạy tương tác với PySpark Shell trên cụm

Bạn có thể mở một phiên PySpark tương tác trực tiếp bên trong container Spark Worker để chạy thử mã Python:

```bash
docker exec -it olist-lakehouse-spark-worker-1 /opt/spark/bin/pyspark \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=jdbc \
  --conf spark.sql.catalog.lakehouse.uri=jdbc:postgresql://postgres:5432/iceberg \
  --conf spark.sql.catalog.lakehouse.jdbc.user=iceberg \
  --conf spark.sql.catalog.lakehouse.jdbc.password=iceberg \
  --conf spark.sql.catalog.lakehouse.warehouse=s3://lakehouse/ \
  --conf spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.lakehouse.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.lakehouse.s3.path-style-access=true
```

Khi dấu nhắc `>>>` hiện lên, gõ thử các lệnh sau:

```python
# Kiểm tra danh sách bảng trong catalog
spark.sql("SHOW TABLES IN lakehouse.gold").show(truncate=False)

# Đọc thử 5 dòng từ bảng Fact
df = spark.table("lakehouse.gold.fact_orders")
df.select("order_key", "order_id", "total_order_value", "order_status").show(5)

# Thoát PySpark
exit()
```

---

## 4.4. Đọc dữ liệu Bronze, Silver, Gold qua DataFrame API

Mỗi tầng dữ liệu trong Medallion Architecture mang một sứ mệnh cụ thể:
* **Bronze:** Giữ nguyên vẹn kiểu dữ liệu chuỗi (String), lưu thêm metadata (`_ingestion_timestamp`, `_source_file`, `_batch_id`).
* **Silver:** Ép kiểu chặt chẽ (Timestamp, Long, Double), kiểm tra quy tắc làm sạch dữ liệu và bóc tách lỗi.
* **Gold:** Mô hình hóa Star Schema (Fact/Dimension), tính toán các độ đo doanh thu, cước phí.

Hãy chạy script Python kiểm tra lược đồ của 3 tầng:

```bash
docker exec -it olist-lakehouse-spark-worker-1 python3 -c '
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InspectSchemas") \
    .master("local[2]") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "jdbc") \
    .config("spark.sql.catalog.lakehouse.uri", "jdbc:postgresql://postgres:5432/iceberg") \
    .config("spark.sql.catalog.lakehouse.jdbc.user", "iceberg") \
    .config("spark.sql.catalog.lakehouse.jdbc.password", "iceberg") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3://lakehouse/") \
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
    .getOrCreate()

print("\n--- SCHEMA BRONZE ORDERS ---")
spark.table("lakehouse.bronze.orders").printSchema()

print("\n--- SCHEMA SILVER ORDERS ---")
spark.table("lakehouse.silver.orders").printSchema()

print("\n--- SCHEMA GOLD FACT ORDERS ---")
spark.table("lakehouse.gold.fact_orders").printSchema()
'
```

---

## 4.5. Thực hành các tính năng độc quyền của Apache Iceberg qua Spark SQL

### a. Kiểm tra lịch sử Snapshots (Snapshot History)
Mỗi lần có lệnh ghi hoặc cập nhật (`INSERT`, `MERGE INTO`, `REWRITE`), Iceberg tạo ra một snapshot mới có ID bất biến:

```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT snapshot_id, parent_id, committed_at, operation 
FROM lakehouse.gold.\"fact_orders\$snapshots\" 
ORDER BY committed_at DESC;
"
```

### b. Kiểm tra các Manifest Files
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT path, length, partition_spec_id, added_data_files_count, existing_data_files_count 
FROM lakehouse.gold.\"fact_orders\$manifests\";
"
```

### c. Kiểm tra danh sách các Data Files Parquet thực tế
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT file_path, file_format, record_count, file_size_in_bytes 
FROM lakehouse.gold.\"fact_orders\$files\";
"
```

### d. Trải nghiệm Time-Travel (Quay ngược thời gian truy vấn dữ liệu)
Giả sử bạn có snapshot ID cũ `7891234567890123456`, bạn có thể truy vấn bảng đúng tại trạng thái thời điểm đó:

```sql
-- Cú pháp Time-Travel trên Trino / Spark:
SELECT count(*) 
FROM lakehouse.gold.fact_orders FOR VERSION AS OF 7891234567890123456;
```

---

# PHẦN 5: THAM QUAN TẦNG TRUY VẤN TỐC ĐỘ CAO (TRINO COORDINATOR)

Trino (trước đây là PrestoSQL) là động cơ truy vấn SQL phân tán hiệu năng cực cao, đóng vai trò là cổng truy vấn tương tác cho Superset BI, chuyên viên phân tích và các câu truy vấn Ad-hoc.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        TẦNG TRUY VẤN TRINO                             │
├────────────────────────────────────────────────────────────────────────┤
│   Trino CLI / Superset BI (:8082 / :8080)                              │
│         │                                                              │
│         ▼                                                              │
│   Trino Coordinator (:8088 internal)                                   │
│         ├── Iceberg Connector                                          │
│         │     ├── Đọc Catalog PostgreSQL                               │
│         │     ├── Đọc Metadata từ MinIO                                │
│         │     └── Cắt tỉa phân vùng & Bỏ qua tệp (File Skipping)       │
│         └── Đọc trực tiếp Parquet từ MinIO                             │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 5.1. Khởi động và làm chủ Trino CLI

Cách nhanh nhất để tương tác với Trino là mở Trino CLI trực tiếp bên trong container:

```bash
docker exec -it olist-lakehouse-trino-1 trino
```

Khi dấu nhắc `trino>` hiện ra, bạn có thể thiết lập catalog và schema mặc định:
```sql
USE lakehouse.gold;
```

Kiểm tra phiên bản Trino đang chạy:
```sql
SELECT version();
```

---

## 5.2. Khám phá cấu hình Connector Iceberg trong Trino

Thoát ra shell Ubuntu bằng lệnh `quit;` và kiểm tra file cấu hình connector:

```bash
docker exec -it olist-lakehouse-trino-1 cat /etc/trino/catalog/lakehouse.properties
```

**Nội dung cấu hình chuẩn:**
```ini
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://postgres:5432/iceberg
iceberg.jdbc-catalog.connection-user=iceberg
iceberg.jdbc-catalog.connection-password=iceberg
iceberg.jdbc-catalog.default-warehouse-dir=s3://lakehouse/
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.aws-access-key=root
hive.s3.aws-secret-key=admin_root
hive.s3.ssl.enabled=false
```

---

## 5.3. Truy vấn dữ liệu tầng Bronze (9 bảng Raw Ingestion)

Đếm số dòng của toàn bộ 9 bảng Bronze:

```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 'bronze.orders' AS tbl, count(*) AS cnt FROM lakehouse.bronze.orders
UNION ALL SELECT 'bronze.order_items', count(*) FROM lakehouse.bronze.order_items
UNION ALL SELECT 'bronze.order_payments', count(*) FROM lakehouse.bronze.order_payments
UNION ALL SELECT 'bronze.order_reviews', count(*) FROM lakehouse.bronze.order_reviews
UNION ALL SELECT 'bronze.customers', count(*) FROM lakehouse.bronze.customers
UNION ALL SELECT 'bronze.products', count(*) FROM lakehouse.bronze.products
UNION ALL SELECT 'bronze.sellers', count(*) FROM lakehouse.bronze.sellers
UNION ALL SELECT 'bronze.geolocation', count(*) FROM lakehouse.bronze.geolocation
UNION ALL SELECT 'bronze.product_category_name_translation', count(*) FROM lakehouse.bronze.product_category_name_translation;
"
```

**Kết quả số dòng chuẩn:**
```text
                  tbl                  |   cnt   
---------------------------------------+---------
 bronze.orders                         |   99441 
 bronze.order_items                    |  112650 
 bronze.order_payments                 |  103886 
 bronze.order_reviews                  |   99224 
 bronze.customers                      |   99441 
 bronze.products                       |   32951 
 bronze.sellers                        |    3095 
 bronze.geolocation                    | 1000163 
 bronze.product_category_name_translation |      71 
```

---

## 5.4. Truy vấn dữ liệu tầng Silver (8 bảng Chuẩn hóa & Khử trùng lặp)

Quan sát sự khác biệt giữa `bronze.orders` và `silver.orders`:
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    (SELECT count(*) FROM lakehouse.bronze.orders) AS bronze_count,
    (SELECT count(*) FROM lakehouse.silver.orders) AS silver_count,
    (SELECT count(*) FROM lakehouse.quarantine.orders) AS quarantine_count;
"
```

**Kết quả:**
```text
 bronze_count | silver_count | quarantine_count 
--------------+--------------+------------------
        99441 |        99244 |              197 
```
> **Nhận xét kiến trúc:** $99.244 + 197 = 99.441$. Không có một dòng dữ liệu nào bị rơi rớt; 197 đơn hàng nghịch lý thời gian đã được bóc tách sang tầng Quarantine!

---

## 5.5. Truy vấn dữ liệu tầng Gold Star Schema: Facts & Dimensions

### a. Đối soát doanh thu tổng thể
Chạy câu đối soát vàng kiểm tra 4 chỉ số tài chính trọng yếu:
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    count(*) AS tong_so_don,
    sum(item_revenue) AS tong_tien_hang,
    sum(freight_total) AS tong_tien_cuoc,
    sum(payment_total) AS tong_thanh_toan
FROM lakehouse.gold.fact_orders;
"
```

**Bảng đối soát doanh thu từng cent:**
* Tổng số đơn: **99.441**
* Tổng tiền hàng: **13.591.643,70 R$**
* Tổng cước vận chuyển: **2.251.909,54 R$**
* Tổng thanh toán thực tế: **16.008.872,12 R$**

### b. Truy vấn Doanh thu theo Bang khách hàng (JOIN Fact & Dimension)
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    c.customer_state,
    count(f.order_key) AS so_don,
    round(sum(f.total_order_value), 2) AS doanh_thu_real
FROM lakehouse.gold.fact_orders f
JOIN lakehouse.gold.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_state
ORDER BY doanh_thu_real DESC
LIMIT 5;
"
```

**Kết quả:**
```text
 customer_state | so_don | doanh_thu_real 
----------------+--------+----------------
 SP             |  41746 |     5998240.23 
 RJ             |  12852 |     2144379.69 
 MG             |  11635 |     1872202.32 
 RS             |   5466 |      890783.64 
 PR             |   5045 |      811160.74 
```

---

## 5.6. Truy vấn 4 Semantic Views phục vụ BI

Tầng ngữ nghĩa (Semantic Views) giúp trừu tượng hóa các phép JOIN phức tạp thành các View thân thiện cho Data Analyst:

```bash
# 1. Xem View Hiệu quả Kinh doanh (Sales Performance)
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT order_date, total_orders, gross_revenue, avg_order_value 
FROM lakehouse.semantic.vw_sales_performance 
ORDER BY order_date DESC 
LIMIT 5;
"

# 2. Xem View Phễu Vòng đời Đơn hàng (Order Funnel)
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT funnel_stage, stage_order, order_count, conversion_rate_pct 
FROM lakehouse.semantic.vw_order_funnel 
ORDER BY stage_order;
"
```

---

## 5.7. Benchmark hiệu năng truy vấn Trino và File-Skipping

Hãy thử nghiệm câu lệnh `EXPLAIN ANALYZE` để xem Trino tận dụng metadata Iceberg cắt bớt bao nhiêu tệp Parquet khi truy vấn có bộ lọc ngày:

```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
EXPLAIN ANALYZE 
SELECT count(*), sum(total_order_value) 
FROM lakehouse.gold.fact_orders 
WHERE purchase_date_key BETWEEN 20180101 AND 20180131;
"
```

Trong phần kế hoạch thực thi (Execution Plan), bạn sẽ thấy chỉ số `Input: xxx rows` và số lượng splits quét chỉ bằng một phần nhỏ tổng số tệp dữ liệu nhờ cơ chế min/max stats của Iceberg Manifest.

---

# PHẦN 6: THAM QUAN TẦNG ĐIỀU PHỐI QUY TRÌNH (APACHE AIRFLOW)

Apache Airflow 2.10 là trái tim điều phối toàn bộ chu trình xử lý dữ liệu tự động, quản lý phụ thuộc giữa các tác vụ (DAG), cơ chế thử lại (Retry) và kích hoạt cảnh báo sự cố.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        KIẾN TRÚC AIRFLOW DAGS                          │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│   olist_daily_pipeline (Chạy hàng ngày @daily)                         │
│     ├── 1. validate_parameters                                         │
│     ├── 2. ingest_bronze_layer                                         │
│     ├── 3. [8 tasks song song] silver_orders, silver_items, ...        │
│     ├── 4. build_gold_closure (Dựng facts & marts)                     │
│     ├── 5. run_quality_checks (Kiểm định 198 quy tắc GX)               │
│     └── 6. complete_pipeline                                           │
│                                                                        │
│   olist_maintenance (Bảo trì Iceberg: dry-run, rewrite, expire, orphan) │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 6.1. Tổng quan giao diện Airflow Web UI

Mở trình duyệt truy cập vào [http://localhost:8083](http://localhost:8083):
* **Username:** `admin`
* **Password:** `admin`

Giao diện chính liệt kê 4 DAGs:
1. `olist_daily_pipeline`: Pipeline tổng lực xử lý lô dữ liệu hàng ngày.
2. `olist_transformation`: Pipeline chạy riêng việc chuyển đổi Silver và Gold.
3. `olist_quality`: Pipeline chạy riêng 198 bài kiểm định chất lượng.
4. `olist_maintenance`: Pipeline bảo trì nén tệp và dọn dẹp Iceberg metadata.

---

## 6.2. Mổ xẻ 4 DAG nghiệp vụ cốt lõi

Kiểm tra mã nguồn của DAG chính [`orchestration/dags/olist_daily_pipeline.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/orchestration/dags/olist_daily_pipeline.py):

```bash
head -n 60 orchestration/dags/olist_daily_pipeline.py
```

**Các đặc tính thiết kế quan trọng:**
* Sử dụng `build_spark_submit_task` chuẩn hóa gọi SparkSubmitOperator.
* Tham số `pool="lakehouse_writes"` giúp kiểm soát số lượng tiến trình ghi đồng thời vào Iceberg.
* Cấu hình `on_failure_callback=send_pipeline_failure_alert` tự động gửi email cứu hộ khi có bất kỳ task nào gặp sự cố.

---

## 6.3. Làm chủ Airflow CLI từ dòng lệnh Ubuntu

Bạn có thể điều khiển toàn bộ Airflow mà không cần bấm chuột trên Web UI:

### a. Liệt kê danh sách DAGs
```bash
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow dags list
```

### b. Kiểm tra các lỗi Import DAG (nếu có)
```bash
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow dags list-import-errors
```

### c. Xem danh sách các Task trong DAG `olist_daily_pipeline`
```bash
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow tasks list olist_daily_pipeline --tree
```

### d. Bỏ tạm dừng (Unpause) hoặc Tạm dừng (Pause) DAG
```bash
# Bỏ tạm dừng để DAG sẵn sàng chạy
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow dags unpause olist_daily_pipeline

# Tạm dừng DAG
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow dags pause olist_daily_pipeline
```

### e. Kích hoạt thủ công một lần chạy (Trigger DAG Run)
```bash
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_daily_pipeline \
  --conf '{"mode": "full", "dataset_path": "data/raw"}'
```

Theo dõi trạng thái lần chạy:
```bash
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow dags list-runs -d olist_daily_pipeline --state running
```

---

## 6.4. Cơ chế điều tiết ghi: Airflow Pool `lakehouse_writes`

Iceberg sử dụng cơ chế Khóa lạc quan (OCC). Nếu nhiều tiến trình cùng commit vào một bảng tại một mili-giây, giao dịch commit sau sẽ bị xung đột. Airflow giải quyết việc này bằng khái niệm `Pool`:

Kiểm tra cấu hình Pool trong Airflow:
```bash
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow pools list
```

**Kết quả:**
```text
Pool               Slots   Description
default_pool       128     Default pool
lakehouse_writes   8       Dieu tiet cac job ghi vao Apache Iceberg
```

*(Lưu ý: Như đã phân tích trong báo cáo thẩm định, ở môi trường Production với Spark Worker 2 nhân, pool này nên được hạ xuống 2-3 slots để giảm tải RAM cho Scheduler).*


---

# PHẦN 7: THAM QUAN CƠ CHẾ KIỂM ĐỊNH CHẤT LƯỢNG (GREAT EXPECTATIONS & QUARANTINE)

Chất lượng dữ liệu (Data Quality) là xương sống tạo nên sự tin cậy của Data Lakehouse. Thay vì chỉ kiểm tra thủ công, hệ thống tích hợp **198 bài kiểm định tự động theo hợp đồng dữ liệu (Data Contracts)** sử dụng thư viện Great Expectations kết hợp với **Mô hình Cách ly Dữ liệu (Medallion Quarantine Pattern)**.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        CƠ CHẾ PHÒNG VỆ VÀ CÁCH LY DỮ LIỆU                              │
├────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                        │
│   Dữ liệu Thô (Bronze)                                                                 │
│          │                                                                             │
│          ▼                                                                             │
│   Bộ lọc Quy tắc Nghiệp vụ (Silver Ingestion)                                          │
│          │                                                                             │
│          ├──► Vi phạm thứ tự thời gian / Định dạng hỏng ──► [ QUARANTINE ZONE ]        │
│          │    (Carrier < Purchase, Delivery < Carrier)      (quarantine.orders - 197)  │
│          │                                                                             │
│          └──► Dữ liệu Hợp lệ ──► [ SILVER CLEAN ZONE ] (silver.orders - 99.244)        │
│                                       │                                                │
│                                       ▼                                                │
│                                 [ GOLD ZONE ] (Star Schema Facts/Dims)                 │
│                                       │                                                │
│                                       ▼                                                │
│                     [ CỔNG KIỂM ĐỊNH GREAT EXPECTATIONS ]                              │
│                     (198 Checks: Null, Uniqueness, Types, FK)                          │
│                                       │                                                │
│                    ┌──────────────────┴──────────────────┐                             │
│                    ▼                                     ▼                             │
│             Mức độ: WARN                          Mức độ: ERROR                        │
│          (Ghi log, cho đi tiếp)              (Chặn pipeline, gửi email)                │
│                                                                                        │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 7.1. Triết lý Data Contracts trong Medallion Architecture

Data Contract là cam kết giữa đội ngũ kỹ sư dữ liệu và các bên tiêu thụ dữ liệu (BI, Analytics, ML). Mọi bảng trong Lakehouse đều có một "bản hợp đồng" quy định rõ:
1. Danh sách các cột bắt buộc phải có mặt.
2. Kiểu dữ liệu chính xác (ví dụ: `price` phải là `double`, không thể là `string`).
3. Ràng buộc tính không rỗng (Non-nullability) trên các khóa định danh.
4. Ràng buộc duy nhất (Uniqueness) trên các khóa chính.
5. Ràng buộc miền giá trị hợp lệ (Value Set) và mối quan hệ khóa ngoại (Referential Integrity).

Kiểm tra định nghĩa hợp đồng trong mã nguồn:
```bash
head -n 50 transformation/quality/contracts.py
```

---

## 7.2. 198 phép kiểm định chất lượng tự động

Module [`transformation/quality/job.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/transformation/quality/job.py) thực thi 198 quy tắc kiểm tra trải rộng khắp 3 tầng:
* **Bronze (18 checks):** Kiểm tra cấu trúc tệp nguồn, số lượng cột tối thiểu và tính nguyên vẹn khi nạp.
* **Silver (72 checks):** Kiểm tra tính duy nhất của khóa tự nhiên, định dạng ngày hợp chuẩn ISO-8601, mã bưu chính zip code hợp lệ.
* **Gold (108 checks):** Kiểm tra toàn vẹn khóa ngoại giữa Fact và Dimension, kiểm tra doanh thu không âm, kiểm tra tỷ lệ bản ghi mồ côi (Unknown Member).

Chạy thử nghiệm kiểm định chất lượng độc lập bằng lệnh:
```bash
docker exec -it olist-lakehouse-spark-worker-1 python3 /opt/airflow/transformation/quality/job.py \
  --run-id "tour-manual-dq-test" \
  --layer "gold"
```

---

## 7.3. Phân biệt cấp độ nghiêm trọng: WARN vs ERROR

Hệ thống phân cấp lỗi rành mạch:
* **Mức `WARN` (Cảnh báo):** Lỗi không làm gián đoạn tính toàn vẹn hệ thống (ví dụ: tỷ lệ đơn hàng thiếu ngày giao hàng do khách mới đặt, hoặc một vài đánh giá không có bình luận). Pipeline vẫn tiếp tục chạy, nhưng ghi log và hiển thị trên dashboard giám sát.
* **Mức `ERROR` (Chí mạng):** Lỗi vi phạm nghiêm trọng (ví dụ: khóa chính bị trùng lặp, doanh thu âm, schema bị mất cột cốt lõi). Task `run_quality_checks` sẽ lập tức thoát với mã lỗi $\neq 0$, dừng pipeline và kích hoạt email khẩn cấp tới Admin.

---

## 7.4. Khám phá tầng cách ly (Quarantine Layer)

Khi phát hiện bản ghi lỗi ở tầng Silver, hệ thống **không xóa bỏ** (làm mất dấu kiểm toán) và **không để lọt vào Gold** (làm bẩn kho báo cáo), mà đưa vào tầng Quarantine.

Kiểm tra số dòng trong các bảng Quarantine qua Trino:
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 'quarantine.orders' AS tbl, count(*) AS cnt FROM lakehouse.quarantine.orders
UNION ALL
SELECT 'quarantine.geography', count(*) FROM lakehouse.quarantine.geography;
"
```

**Kết quả:**
```text
         tbl          | cnt 
----------------------+-----
 quarantine.orders    | 197 
 quarantine.geography |   0 
```

---

## 7.5. Phân tích 197 đơn hàng lỗi thời gian trong `quarantine.orders`

Hãy xem thử các bản ghi bị cách ly và đọc lý do lỗi được ghi nhận trong cột `_error_codes` và `_error_reasons`:

```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    order_id, 
    order_status, 
    order_purchase_timestamp, 
    order_delivered_carrier_date,
    _error_codes
FROM lakehouse.quarantine.orders 
LIMIT 5;
"
```

**Dữ liệu thực tế cho thấy:**
```text
             order_id             | order_status | order_purchase_timestamp | order_delivered_carrier_date |         _error_codes          
----------------------------------+--------------+--------------------------+------------------------------+-------------------------------
 2d93d3950e487772d1d2332d6b7b6a7f | delivered    | 2017-03-29 14:10:00      | 2017-03-27 10:00:00          | [CARRIER_BEFORE_PURCHASE]     
 b86674afceda611042469fa5d17338e5 | delivered    | 2018-01-20 15:30:00      | 2018-01-18 11:20:00          | [CARRIER_BEFORE_PURCHASE]     
 d7703058a946b96f0119e793910c85c0 | delivered    | 2017-11-25 10:15:00      | 2017-11-20 09:00:00          | [CARRIER_BEFORE_PURCHASE]     
```
> **Phân tích:** Ngày giao cho đơn vị vận chuyển (`carrier_date`) lại diễn ra **trước** cả ngày khách bấm mua hàng (`purchase_date`)! Đây là lỗi logic thời gian của hệ thống quản lý kho đối tác, và cơ chế phòng vệ của Lakehouse đã bắt chính xác 100%.

---

## 7.6. Truy vấn lịch sử kiểm định từ bảng `quality.quality_results`

Toàn bộ kết quả của 198 phép kiểm tra mỗi lần chạy đều được ghi vào bảng Iceberg `quality.quality_results`:

```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    check_name, 
    table_name, 
    severity, 
    status, 
    observed_value, 
    checked_at 
FROM lakehouse.quality.quality_results 
ORDER BY checked_at DESC 
LIMIT 10;
"
```

---

# PHẦN 8: THAM QUAN HỆ THỐNG CẢNH BÁO SỰ CỐ (SMTP & MAILHOG)

Hệ thống cảnh báo được thiết kế đa năng: Trong môi trường phát triển (Local/Staging), email được bắt bởi **MailHog Sandbox** (không làm phiền người dùng thật). Khi chuyển sang Production, hệ thống tự động nhận diện và gửi qua **Gmail SMTP thật (hoặc SendGrid/SES)** bằng giao thức mã hóa STARTTLS/SSL.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        KIẾN TRÚC CẢNH BÁO EMAIL                        │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│   Airflow Task FAILED / Great Expectations ERROR                       │
│         │                                                              │
│         ▼                                                              │
│   Module: infrastructure/alerts/email_alert.py                         │
│         │                                                              │
│         ├── Kiểm tra biến ALERT_SMTP_HOST trong .env                   │
│         │                                                              │
│         ├── [LOCAL/DEV]: mailhog:1025 ──► MailHog Web UI (:8025)       │
│         │                                                              │
│         └── [PRODUCTION]: smtp.gmail.com:587 ──► Hộp thư Admin thật    │
│                           (STARTTLS / App Password)                    │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 8.1. Cơ chế cảnh báo lỗi tự động `email_alert.py`

Khám phá module [`infrastructure/alerts/email_alert.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/infrastructure/alerts/email_alert.py):
* Được viết ngắn gọn, dùng thư viện chuẩn `smtplib` và `email.mime`.
* Tự động tạo thẻ HTML Card có màu sắc trực quan (Đỏ cho `CRITICAL`, Vàng cho `WARNING`, Xanh lá cho `INFO`).
* Tự động trích xuất 1.000 ký tự cuối của Exception Log (nơi chứa nguyên nhân gốc thực sự của lỗi `spark-submit`).

Xem cấu hình SMTP trong file `.env`:
```bash
grep -E "ALERT_SMTP|ALERT_ADMIN" .env.example
```

---

## 8.2. Mở giao diện Hộp thư sự cố MailHog

Trong môi trường Docker local, MailHog đóng vai trò như một SMTP Server giả lập bắt mọi email gửi ra mà không cần kết nối Internet ra ngoài:
* Mở trình duyệt truy cập: [http://localhost:8025](http://localhost:8025)
* Giao diện hộp thư đến (Inbox) sẽ hiển thị toàn bộ các email cảnh báo đã phát sinh trong đợt thử nghiệm.

---

## 8.3. Thử nghiệm bắn email cảnh báo độc lập qua CLI

Bạn có thể chạy thử trực tiếp module cảnh báo mà không cần chờ pipeline lỗi:

```bash
docker exec -it olist-lakehouse-airflow-scheduler-1 python3 /opt/airflow/infrastructure/alerts/email_alert.py
```

**Thông báo xuất ra màn hình:**
```text
Dang gui email thu nghiem toi MailHog (mailhog:1025)...
[SUCCESS] Da gui thanh cong email thu nghiem vao MailHog!
Kiem tra hop thu tai: http://localhost:8025
```

Mở [http://localhost:8025](http://localhost:8025), bạn sẽ thấy ngay một bức thư mới với tiêu đề:
`[TEST] Olist Data Lakehouse: Thu nghiem kenh canh bao email` kèm nội dung định dạng HTML chuyên nghiệp!

---

## 8.4. Thử nghiệm kích hoạt lỗi thật trong pipeline để nhận email

Hãy thử nghiệm kích hoạt một lỗi thật để xem Airflow tự động kích hoạt callback:
1. Mở Airflow UI [http://localhost:8083](http://localhost:8083).
2. Tìm DAG `olist_maintenance`.
3. Bấm nút **Trigger DAG w/ config** (hình tam giác góc phải) và nhập cấu hình bảng không tồn tại:
   ```json
   {"table": "bang_khong_hop_le_de_test_alert"}
   ```
4. Bấm **Trigger**.
5. Task `maintenance_dry_run` sẽ thử lại 2 lần và chuyển sang trạng thái `FAILED`.
6. Mở MailHog [http://localhost:8025](http://localhost:8025), bạn sẽ nhận được email:
   `[CRITICAL] Olist Lakehouse Alert: Task maintenance_dry_run trong DAG olist_maintenance đã THẤT BẠI`.

---

# PHẦN 9: THAM QUAN TẦNG TRỰC QUAN HÓA & PHÂN TÍCH KINH DOANH (APACHE SUPERSET)

Apache Superset 4.0 là nền tảng Business Intelligence mã nguồn mở hàng đầu, kết nối trực tiếp với Trino để truy vấn dữ liệu từ tầng Gold và Semantic Views, cung cấp bức tranh toàn cảnh về hoạt động kinh doanh của sàn Olist.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        5 DASHBOARDS TRÊN SUPERSET                      │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  1. Executive Sales Overview (Doanh thu 16.0M R$, Xu hướng tháng)      │
│  2. Order Lifecycle Funnel (Phễu 5 giai đoạn: Đặt -> Duyệt -> Giao)    │
│  3. Customer Analytics & Geo (Bản đồ bang, Khách hàng thân thiết)      │
│  4. Seller Logistics Performance (Top người bán, Thời gian giao hàng)  │
│  5. Customer Reviews & Sentiment (Điểm số 1-5 sao, phân tích cảm xúc)  │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 9.1. Đăng nhập Superset Web UI và kết nối Database

* Truy cập: [http://localhost:8088](http://localhost:8088)
* **Username:** `olist_admin`
* **Password:** `admin`

Kiểm tra chuỗi kết nối Database tới Trino trong Superset:
* Vào menu **Settings (bánh răng)** $\rightarrow$ **Database Connections**.
* Bạn sẽ thấy kết nối tên `Trino Olist Lakehouse`.
* Chuỗi kết nối SQLAlchemy chuẩn:
  `trino://trino@trino:8080/lakehouse`

---

## 9.2. Tham quan Dashboard 1: Tổng quan Doanh thu & Đơn hàng

* Vào menu **Dashboards** $\rightarrow$ chọn **`[Olist] 1. Executive Sales Overview`**.
* **Các chỉ số chính (Big Number KPI):**
  - Tổng doanh thu thanh toán: **16.008.872,12 R$**
  - Tổng số đơn hàng: **99.441 đơn**
  - Giá trị trung bình mỗi đơn (AOV): **~160,99 R$**
* **Biểu đồ xu hướng (Time-series Area Chart):** Doanh thu tăng trưởng mạnh mẽ từ cuối 2016 đến đỉnh cao vào sự kiện Black Friday tháng 11/2017 và duy trì ổn định năm 2018.

---

## 9.3. Tham quan Dashboard 2: Phễu Vòng đời Đơn hàng (Order Lifecycle Funnel)

* Chọn dashboard **`[Olist] 2. Order Lifecycle Funnel`**.
* Đây là biểu đồ phễu kinh doanh (Funnel Analysis) mô hình hóa 5 giai đoạn sống của đơn hàng:
  1. **Đặt hàng (Created):** 99.441 đơn (100%)
  2. **Duyệt thanh toán (Approved):** 99.281 đơn (99,8%)
  3. **Bàn giao vận chuyển (Shipped):** 96.478 đơn (97,0%)
  4. **Giao thành công (Delivered):** 96.470 đơn (97,0%)
  5. **Để lại đánh giá (Reviewed):** 95.820 đơn (96,3%)
* Bộ lọc toàn cục (Cross-filtering) cho phép bạn lọc theo Bang (`customer_state`) hoặc Năm (`purchase_year`) để xem phễu biến động theo từng vùng miền.

---

## 9.4. Tham quan Dashboard 3: Phân tích Khách hàng & Địa lý

* Chọn dashboard **`[Olist] 3. Customer Analytics & Geo Insights`**.
* **Bản đồ nhiệt địa lý (Choropleth Map):**
  - Bang São Paulo (`SP`) chiếm ưu thế tuyệt đối với hơn 41.746 đơn hàng (chiếm trên 41% toàn sàn).
  - Tiếp theo là Rio de Janeiro (`RJ`) với 12.852 đơn và Minas Gerais (`MG`) với 11.635 đơn.
* **Tỷ lệ khách hàng quay lại (Repeat Purchase Rate):** Đa phần khách hàng Olist là người mua một lần (`customer_unique_id` tương đương với số đơn), phản ánh bài toán Olist cần đẩy mạnh các chương trình khách hàng thân thiết.

---

## 9.5. Tham quan Dashboard 4: Vận hành Giao hàng & Người bán

* Chọn dashboard **`[Olist] 4. Seller Logistics Performance`**.
* **Thời gian giao hàng trung bình:** 12,5 ngày từ lúc đặt hàng đến khi nhận hàng.
* **Tỷ lệ giao hàng trễ hẹn:** ~8% tổng số đơn hàng bị giao sau ngày ước tính (`is_late_delivery = true`).
* **Top 10 Người bán có doanh thu cao nhất:** Tập trung ở ngành hàng Máy tính, Đồ gia dụng và Sức khỏe sắc đẹp.

---

## 9.6. Tham quan Dashboard 5: Đánh giá Đơn hàng & Sentiment

* Chọn dashboard **`[Olist] 5. Customer Reviews & Sentiment`**.
* **Phân bổ điểm đánh giá (Rating Distribution):**
  - 5 sao: chiếm ~57%
  - 4 sao: chiếm ~19%
  - 1 sao: chiếm ~11% (chủ yếu liên quan đến đơn hàng giao trễ hoặc sản phẩm không đúng mô tả).
* Mối tương quan chặt chẽ: Các đơn hàng bị giao trễ có điểm số trung bình giảm mạnh từ 4,2 sao xuống chỉ còn 1,8 sao!

---

# PHẦN 10: THAM QUAN TẦNG GIÁM SÁT THỜI GIAN THỰC (PROMETHEUS & GRAFANA)

Khả năng quan sát (Observability) giúp đội ngũ vận hành nắm bắt trạng thái sức khỏe của Lakehouse theo từng giây, phát hiện sớm các hiện tượng tràn RAM, rò rỉ dữ liệu bẩn và nghẽn hàng đợi.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        TẦNG QUAN SÁT THỜI GIAN THỰC                    │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│   Node Exporter (:9100) ────┐                                          │
│   Pipeline Exporter (:9101) ───► Prometheus (:9090) ──► Grafana (:3000)│
│   Blackbox Exporter (:9115) ─┘                                         │
│                                                                        │
│   Metrics trọng yếu:                                                   │
│     - olist_pipeline_run_duration_seconds                              │
│     - olist_table_row_count                                            │
│     - olist_quarantine_rows_total (Đổi màu đỏ khi > 5%)                │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 10.1. Khám phá 10/10 Prometheus Scrape Targets

Mở trình duyệt truy cập: [http://localhost:9090/targets](http://localhost:9090/targets).
Bạn sẽ thấy toàn bộ 10 mục tiêu giám sát đang ở trạng thái xanh `UP`:
1. `node-exporter`: Giám sát CPU/RAM máy host.
2. `pipeline-exporter`: Đọc metadata bảng Iceberg và kiểm toán.
3. `blackbox-http`: Giám sát uptime của Airflow, Trino, Superset, MinIO.
4. `spark-master` & `spark-worker`: Giám sát executors.
5. `minio`: Giám sát lưu lượng S3 và số lượng request.
6. `prometheus`: Tự giám sát chính nó.

---

## 10.2. Các câu lệnh PromQL trọng yếu giám sát Lakehouse

Tại giao diện [http://localhost:9090/graph](http://localhost:9090/graph), bạn có thể thử thực thi các câu lệnh PromQL sau:

### a. Xem số dòng dữ liệu hiện tại của các bảng Iceberg
```promql
olist_table_row_count{layer="gold"}
```

### b. Kiểm tra tỷ lệ dữ liệu cách ly (Quarantine Rate %)
```promql
(sum(olist_quarantine_rows_total) / sum(olist_table_row_count{layer="bronze"})) * 100
```

### c. Đo thời gian thực thi của task chậm nhất
```promql
topk(5, olist_task_duration_seconds)
```

---

## 10.3. Tham quan Dashboard Grafana `olist-pipeline`

* Truy cập: [http://localhost:3000](http://localhost:3000)
* **Username:** `admin`
* **Password:** `admin`
* Mở dashboard **`Olist Data Lakehouse - Pipeline Observability`**.
* **9 Panel thông tin gồm:**
  1. *Pipeline Status Gauge:* Trạng thái thành công của lần chạy gần nhất.
  2. *Pipeline Duration Trend:* Biểu đồ đường xu hướng thời gian chạy theo ngày.
  3. *Table Row Counts by Layer:* Cột chồng so sánh quy mô Bronze vs Silver vs Gold.
  4. *Quarantine Ingestion Rate (%):* Đồng hồ đo tỷ lệ bản ghi lỗi (có ngưỡng đổi màu: Vàng khi > 2%, Đỏ khi > 5%).
  5. *Spark Worker CPU & Memory Utilization:* Tải thực tế của Worker.
  6. *Trino Query Latency:* Độ trễ truy vấn trung bình (P95, P99).
  7. *Airflow Pool Slot Allocation:* Số lượng slot đang sử dụng của pool ghi.
  8. *Data Quality Check Results:* Tỷ lệ pass của 198 phép kiểm tra.
  9. *Alert Events Timeline:* Lịch sử các lần bắn email cảnh báo.

---

## 10.4. Tham quan Dashboard Grafana `docker-monitoring`

* Mở dashboard **`Docker Infrastructure Monitoring`**.
* Theo dõi mức tiêu thụ RAM chi tiết của từng container trong số 13 container:
  - `airflow-scheduler` (container ngốn RAM nhất do JVM Driver)
  - `trino` (thường tiêu thụ quanh 2,5 GiB RAM)
  - `spark-worker` (ổn định ở mức 1,5 - 2,6 GiB RAM)

---

# PHẦN 11: THỰC HÀNH CÁC KỊCH BẢN NÂNG CAO (CHAOS TESTING & BENCHMARK)

Phần này giúp bạn tự tay thử nghiệm sức chịu tải và cơ chế tự vệ của Lakehouse trước các kịch bản thực tế khắc nghiệt nhất.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        6 BỘ DỮ LIỆU KIỂM THỬ NÂNG CAO                  │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  1. 01_offpeak: 500 đơn giờ thấp điểm (đo chi phí cố định)             │
│  2. 02_flash_sale: 40.000 đơn dồn trong 2h (thử tải đỉnh, chống OOM)   │
│  3. 03_catchup: 7.544 đơn nạp bù 1 tháng (thử nghiệm MERGE INTO)       │
│  4. 04_dirty_metadata: 2.000 đơn (1.388 lỗi ngày và chữ trong cột tiền)│
│  5. 05_integrity_violations: 2.000 đơn (lỗi khóa ngoại, ép dừng Gold)  │
│  6. 06_duplicates: 2.600 đơn (xung đột trạng thái, khử trùng lặp)      │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 11.1. Chạy một bài đo Benchmark đơn lẻ bằng Python

Để chạy bài đo tải nhẹ `01_offpeak` (500 đơn hàng):

```bash
python3 scripts/run_benchmark.py \
  --dataset data/synthetic/01_offpeak \
  --label 01_offpeak \
  --mode incremental
```

Quá trình chạy gồm 6 bước tự động:
1. Đo trạng thái bảng và dung lượng Parquet TRƯỚC khi chạy.
2. Chép dữ liệu vào `s3://raw/olist/`.
3. Bật bộ theo dõi tài nguyên `docker stats` lấy mẫu mỗi 3 giây.
4. Kích hoạt Airflow DAG và chờ kết thúc.
5. Đo trạng thái SAU khi chạy để tính phần chênh lệch.
6. Kết xuất kết quả ra file JSON `docs/reports/benchmarks/01_offpeak.json`.

---

## 11.2. Thực hành tiêm lỗi Dữ liệu bẩn `04_dirty_metadata`

Bộ dữ liệu này chứa 1.388 bản ghi bị cố ý sửa sai định dạng ngày (ví dụ: `2018/31/12`) và chèn ký tự chữ vào cột số tiền (ví dụ: `abc120.50`):

```bash
python3 scripts/run_benchmark.py \
  --dataset data/synthetic/04_dirty_metadata \
  --label 04_dirty_metadata \
  --mode incremental
```

**Quan sát kết quả tự vệ:**
* Pipeline vẫn chạy **SUCCESS**.
* 1.388 bản ghi bẩn bị bóc tách và đẩy toàn bộ vào `quarantine.orders`.
* Bảng `silver.orders` và `gold.fact_orders` hoàn toàn không bị nhiễm một dòng dữ liệu bẩn nào!

---

## 11.3. Thực hành tiêm lỗi Vi phạm khóa `05_integrity_violations`

Bộ dữ liệu này chứa các đơn hàng trỏ tới mã khách hàng không tồn tại (`customer_id` bị sửa thành mã rác):

```bash
python3 scripts/run_benchmark.py \
  --dataset data/synthetic/05_integrity_violations \
  --label 05_integrity_violations \
  --mode incremental
```

**Quan sát kết quả:**
* Cổng kiểm định Great Expectations phát hiện vi phạm khóa ngoại ở mức `ERROR`.
* Pipeline lập tức chuyển sang trạng thái **`FAILED`** để ngăn chặn việc lan truyền dữ liệu lỗi.
* Email cảnh báo `[CRITICAL]` được bắn ngay lập tức vào MailHog!

---

## 11.4. Thực hành Nén tệp tự động (Iceberg Compaction)

Sau nhiều lần nạp tăng dần, số lượng tệp Parquet nhỏ sẽ tăng lên. Hãy chạy DAG `olist_maintenance` để thực hiện nén tệp:

```bash
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_maintenance \
  --conf '{"table": "lakehouse.control.pipeline_runs"}'
```

Theo dõi log nén tệp:
```bash
./scripts/logs.sh airflow-scheduler --tail 50
```

Kiểm tra số lượng tệp Parquet sau khi nén:
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT count(*) AS so_tep_parquet_hien_tai 
FROM lakehouse.control.\"pipeline_runs\$files\";
"
```

---

## 11.5. Phục hồi Baseline nguyên bản và đối soát doanh thu từng cent

Sau khi đã thử nghiệm các kịch bản tải và lỗi, hãy chạy bài phục hồi dữ liệu gốc để đưa hệ thống về trạng thái chuẩn đã được nghiệm thu:

```bash
python3 scripts/run_benchmark.py \
  --dataset data/raw \
  --label 07_baseline_restore \
  --mode full
```

Sau khi hoàn tất, chạy lại script đối soát sức khỏe:
```bash
./scripts/verify_stack.sh
```

Khẳng định 4 con số tài chính vàng khớp từng cent:
* Số lượng đơn: **99.441**
* Doanh thu hàng: **13.591.643,70 R$**
* Cước vận chuyển: **2.251.909,54 R$**
* Tổng thanh toán: **16.008.872,12 R$**

---

# PHẦN 12: BẢNG TRA CỨU COMMAND TOÀN TẬP (CHEATSHEET & TROUBLESHOOTING)

## 12.1. Cheatsheet Docker & Compose

```bash
# Bật toàn bộ cụm và chờ chuyển xanh
./scripts/up.sh

# Bật và build lại image
./scripts/up.sh --build

# Xem danh sách container và trạng thái healthcheck
docker compose ps

# Xem tài nguyên CPU/RAM theo thời gian thực
docker stats

# Dừng cụm an toàn (bảo toàn volume)
./scripts/down.sh

# Dừng cụm và XÓA SẠCH volume (cần xác nhận 'XOA')
./scripts/down.sh -v

# Xem log bám đuôi của một container
./scripts/logs.sh <tên-dịch-vụ> -f
```

---

## 12.2. Cheatsheet Airflow CLI

```bash
# Liệt kê DAGs
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow dags list

# Kiểm tra lỗi import DAG
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow dags list-import-errors

# Trigger DAG chạy thủ công
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow dags trigger <tên-dag>

# Xem danh sách task trong DAG
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow tasks list <tên-dag>

# Xóa trạng thái để chạy lại một task cụ thể
docker exec -it olist-lakehouse-airflow-scheduler-1 airflow tasks clear -t <task_id> -d <dag_id> --yes
```

---

## 12.3. Cheatsheet Trino SQL

```bash
# Mở Trino CLI tương tác
docker exec -it olist-lakehouse-trino-1 trino

# Chạy một câu lệnh SQL từ bên ngoài terminal
docker exec -it olist-lakehouse-trino-1 trino --execute "<câu-lệnh-sql>"

# Xuất kết quả ra file CSV
docker exec -it olist-lakehouse-trino-1 trino --execute "<sql>" --output-format CSV > output.csv

# Xem danh sách schemas (namespaces)
SHOW SCHEMAS FROM lakehouse;

# Xem danh sách bảng trong một schema
SHOW TABLES FROM lakehouse.gold;

# Xem thông tin metadata của bảng Iceberg
SELECT * FROM lakehouse.gold."fact_orders$snapshots";
SELECT * FROM lakehouse.gold."fact_orders$files";
SELECT * FROM lakehouse.gold."fact_orders$manifests";
```

---

## 12.4. Bảng chẩn đoán và khắc phục sự cố (Troubleshooting Matrix)

| Hiện tượng sự cố | Nguyên nhân gốc | Cách khắc phục ngay lập tức |
|:---|:---|:---|
| **Airflow Scheduler bị sập đột ngột (OOM)** | 8 task Silver chạy song song ở `client` mode chiếm hơn 6 GiB RAM host | Chạy: `./scripts/down.sh`<br>Hạ số slot pool `lakehouse_writes` từ 8 xuống 2 trong Airflow UI rồi bật lại. |
| **Trino báo `Access Denied` hoặc không thấy bảng mới** | Metadata Catalog chưa kịp đồng bộ từ PostgreSQL | Chạy lệnh trong Trino CLI:<br>`CALL lakehouse.system.flush_metadata_cache();` |
| **PostgreSQL báo lỗi `password authentication failed` khi dựng lại** | Docker Volume cũ vẫn giữ mật khẩu cũ, không đồng bộ với `.env` | Chạy lệnh SQL đồng bộ:<br>`ALTER ROLE airflow WITH PASSWORD 'airflow';` |
| **Không nhận được email cảnh báo** | `ALERT_SMTP_HOST` trong `.env` chưa đúng hoặc cổng bị chặn | Kiểm tra MailHog tại [http://localhost:8025](http://localhost:8025). Nếu dùng Gmail, kiểm tra Google App Password 16 ký tự. |
| **Một task Spark bị treo (Stuck) không kết thúc** | Executor thiếu RAM hoặc phân vùng dữ liệu bị Skew | Mở Spark Master UI [http://localhost:8084](http://localhost:8084), kiểm tra Stage đang chạy và kill task nếu cần. |

### Hướng dẫn chi tiết từng bước xử lý 5 tình huống sự cố trên:

#### a. Quy trình xử lý khi Airflow Scheduler bị OOM Crash
```bash
# Bước 1: Xác nhận Scheduler đã chết
docker ps -a --filter "name=airflow-scheduler"

# Bước 2: Xem log lỗi OOM của Linux kernel
dmesg -T | grep -E -i "killed process|oom_reaper|airflow" | tail -n 10

# Bước 3: Vào database PostgreSQL của Airflow hạ ngay slot của pool
docker exec -it olist-lakehouse-postgres-1 psql -U airflow -d airflow -c "
UPDATE slot_pool SET slots = 2 WHERE pool = 'lakehouse_writes';
"

# Bước 4: Khởi động lại container Scheduler
docker compose -f docker-compose.yml -f docker-compose.orchestration.yml restart airflow-scheduler
```

#### b. Quy trình làm mới bộ nhớ đệm Metadata của Trino (Flush Cache)
```bash
# Khi Spark vừa ghi bảng mới mà Trino báo lỗi không tìm thấy bảng hoặc schema cũ:
docker exec -it olist-lakehouse-trino-1 trino --execute "
CALL lakehouse.system.flush_metadata_cache();
"
# Kiểm tra lại danh sách bảng
docker exec -it olist-lakehouse-trino-1 trino --execute "SHOW TABLES FROM lakehouse.gold;"
```

#### c. Quy trình đồng bộ lại mật khẩu PostgreSQL khi tái sử dụng Volume
```bash
# Đổi mật khẩu cho user 'airflow'
docker exec -it olist-lakehouse-postgres-1 psql -U postgres -c "
ALTER ROLE airflow WITH PASSWORD 'airflow';
"

# Đổi mật khẩu cho user 'iceberg'
docker exec -it olist-lakehouse-postgres-1 psql -U postgres -c "
ALTER ROLE iceberg WITH PASSWORD 'iceberg';
"

# Khởi động lại các container phụ thuộc
docker compose restart airflow-scheduler airflow-webserver
```

---

# PHẦN 13: BỘ 5 BÀI LAB THỰC HÀNH TỰ DO (HANDS-ON LABS & EXPERIMENTS)

Phần này cung cấp 5 bài thực hành độc lập có kịch bản chi tiết để bạn tự tay "vọc vạch", thử nghiệm sâu vào từng thành phần của hệ sinh thái Lakehouse.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        DANH MỤC 5 BÀI LAB THỰC HÀNH NÂNG CAO                           │
├────┬──────────────────────────────────────────┬────────────────────────────────────────┤
│Lab │ Tên bài thực hành                        │ Trọng tâm kỹ thuật                     │
├────┼──────────────────────────────────────────┼────────────────────────────────────────┤
│ 1  │ Bóc tách SQL của 10 Chart Superset       │ Phân tích câu lệnh SQL chạy ngầm của BI│
│ 2  │ Viết Ad-hoc PySpark Script phân tán      │ Trích xuất Top Seller & Category       │
│ 3  │ Diễn tập Khôi phục Dữ liệu (Rollback)    │ Xóa nhầm dữ liệu & Rollback Iceberg    │
│ 4  │ Tự tạo Semantic View mới trên Trino      │ Tối ưu hóa truy vấn cho Marketing      │
│ 5  │ Tự thêm quy tắc Data Contract mới        │ Kiểm định chất lượng và kích hoạt alert│
└────┴──────────────────────────────────────────┴────────────────────────────────────────┘
```

---

## 13.1. Lab 1: Bóc tách câu lệnh SQL của 10 Biểu đồ trọng yếu trên Superset

Superset trực quan hóa dữ liệu bằng cách gửi các câu lệnh SQL tới Trino Coordinator. Dưới đây là 10 câu lệnh SQL chuẩn đại diện cho 10 biểu đồ kinh doanh cốt lõi mà bạn có thể chạy trực tiếp trong Trino CLI để đối soát với biểu đồ trên Web UI:

### Chart 1: Doanh thu theo tháng (Monthly Revenue Trend)
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    date_trunc('month', d.full_date) AS thang,
    round(sum(f.total_order_value), 2) AS doanh_thu,
    count(distinct f.order_key) AS so_don
FROM lakehouse.gold.fact_orders f
JOIN lakehouse.gold.dim_date d ON f.purchase_date_key = d.date_key
GROUP BY 1
ORDER BY 1;
"
```

### Chart 2: Top 10 Ngành hàng có doanh thu cao nhất (Top Categories)
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    coalesce(p.product_category_name_english, 'Chua phan loai') AS nganh_hang,
    round(sum(i.price), 2) AS doanh_thu_hang,
    count(i.order_item_id) AS so_luong_ban
FROM lakehouse.gold.fact_order_items i
JOIN lakehouse.gold.dim_product p ON i.product_key = p.product_key
GROUP BY 1
ORDER BY doanh_thu_hang DESC
LIMIT 10;
"
```

### Chart 3: Phân bổ Phương thức Thanh toán (Payment Types)
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    payment_type,
    count(*) AS so_giao_dich,
    round(sum(payment_value), 2) AS tong_tien,
    round(avg(payment_installments), 1) AS so_ky_tra_gop_tb
FROM lakehouse.gold.fact_order_payments
GROUP BY payment_type
ORDER BY tong_tien DESC;
"
```

### Chart 4: Tỷ lệ Giao hàng đúng hạn vs Trễ hạn theo từng Bang
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    c.customer_state,
    count(*) AS tong_don,
    count(CASE WHEN f.is_late_delivery = true THEN 1 END) AS so_don_tre,
    round(count(CASE WHEN f.is_late_delivery = true THEN 1 END) * 100.0 / count(*), 2) AS ty_le_tre_pct
FROM lakehouse.gold.fact_orders f
JOIN lakehouse.gold.dim_customer c ON f.customer_key = c.customer_key
WHERE f.order_status = 'delivered'
GROUP BY c.customer_state
HAVING count(*) > 500
ORDER BY ty_le_tre_pct DESC;
"
```

### Chart 5: Phân tích Điểm đánh giá Đơn hàng theo Thời gian giao hàng
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    CASE 
        WHEN f.delivery_duration_days <= 7 THEN '1. Nhanh (<= 7 ngay)'
        WHEN f.delivery_duration_days <= 14 THEN '2. Binh thuong (8-14 ngay)'
        WHEN f.delivery_duration_days <= 21 THEN '3. Hoi cham (15-21 ngay)'
        ELSE '4. Rat cham (> 21 ngay)'
    END AS nhom_thoi_gian_giao,
    count(distinct f.order_key) AS so_don,
    round(avg(r.review_score), 2) AS diem_danh_gia_tb
FROM lakehouse.gold.fact_orders f
JOIN lakehouse.gold.fact_order_reviews r ON f.order_key = r.order_key
WHERE f.order_status = 'delivered' AND f.delivery_duration_days IS NOT NULL
GROUP BY 1
ORDER BY 1;
"
```

### Chart 6: Phễu Vòng đời Đơn hàng (Order Funnel 5 Stages)
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    funnel_stage,
    stage_order,
    order_count,
    conversion_rate_pct,
    dropoff_rate_pct
FROM lakehouse.semantic.vw_order_funnel
ORDER BY stage_order;
"
```

### Chart 7: Top 10 Người bán có Doanh số Lớn nhất
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    s.seller_id,
    s.seller_city,
    s.seller_state,
    round(sum(i.price), 2) AS tong_doanh_so,
    count(distinct i.order_id) AS so_don_ban
FROM lakehouse.gold.fact_order_items i
JOIN lakehouse.gold.dim_seller s ON i.seller_key = s.seller_key
GROUP BY 1, 2, 3
ORDER BY tong_doanh_so DESC
LIMIT 10;
"
```

### Chart 8: Phân tích Giá trị Giỏ hàng trung bình theo Ngày trong tuần
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    d.day_name_english AS thu_trong_tuan,
    d.day_of_week AS ma_thu,
    count(f.order_key) AS tong_don,
    round(avg(f.total_order_value), 2) AS gia_tri_gio_hang_tb
FROM lakehouse.gold.fact_orders f
JOIN lakehouse.gold.dim_date d ON f.purchase_date_key = d.date_key
GROUP BY d.day_name_english, d.day_of_week
ORDER BY d.day_of_week;
"
```

### Chart 9: Ma trận Tương quan Trọng lượng hàng hóa và Cước vận chuyển
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    CASE 
        WHEN p.product_weight_g < 500 THEN '1. Sieu nhe (< 500g)'
        WHEN p.product_weight_g < 2000 THEN '2. Nhe (500g - 2kg)'
        WHEN p.product_weight_g < 5000 THEN '3. Trung binh (2kg - 5kg)'
        ELSE '4. Nang (> 5kg)'
    END AS phan_khuc_khoi_luong,
    count(*) AS so_luot_dat,
    round(avg(i.freight_value), 2) AS cuoc_phi_trung_binh
FROM lakehouse.gold.fact_order_items i
JOIN lakehouse.gold.dim_product p ON i.product_key = p.product_key
WHERE p.product_weight_g IS NOT NULL
GROUP BY 1
ORDER BY 1;
"
```

### Chart 10: Tỷ lệ Khách hàng mua lặp lại (Repeat Purchase Cohort)
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
WITH customer_orders AS (
    SELECT 
        c.customer_unique_id,
        count(distinct f.order_key) AS order_count
    FROM lakehouse.gold.fact_orders f
    JOIN lakehouse.gold.dim_customer c ON f.customer_key = c.customer_key
    GROUP BY c.customer_unique_id
)
SELECT 
    CASE 
        WHEN order_count = 1 THEN '1. Mua 1 lan'
        WHEN order_count = 2 THEN '2. Mua 2 lan'
        ELSE '3. Mua >= 3 lan (Khach VIP)'
    END AS loai_khach_hang,
    count(*) AS so_luong_khach,
    round(count(*) * 100.0 / sum(count(*)) OVER (), 2) AS ty_le_pct
FROM customer_orders
GROUP BY 1
ORDER BY 1;
"
```

---

## 13.2. Lab 2: Viết Ad-hoc PySpark Script phân tán trích xuất dữ liệu

Hãy thử viết một script PySpark hoàn chỉnh độc lập, chạy trên Spark Worker để tính toán danh sách Top 5 Người bán có điểm đánh giá trung bình cao nhất (yêu cầu bán tối thiểu 50 đơn) và lưu kết quả ra một bảng tạm trong Iceberg:

Tạo script thực hành `scratch_pyspark_lab.py`:
```bash
cat << 'EOF' > scratch_pyspark_lab.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Khởi tạo SparkSession kết nối Iceberg
spark = SparkSession.builder \
    .appName("HandsOnLab2_TopSellers") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "jdbc") \
    .config("spark.sql.catalog.lakehouse.uri", "jdbc:postgresql://postgres:5432/iceberg") \
    .config("spark.sql.catalog.lakehouse.jdbc.user", "iceberg") \
    .config("spark.sql.catalog.lakehouse.jdbc.password", "iceberg") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3://lakehouse/") \
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
    .getOrCreate()

print("\n>>> DANG DOC DU LIEU TU LAKEHOUSE...")
fact_items = spark.table("lakehouse.gold.fact_order_items")
fact_reviews = spark.table("lakehouse.gold.fact_order_reviews")
dim_seller = spark.table("lakehouse.gold.dim_seller")

# 2. Thực hiện phép JOIN phân tán
print(">>> DANG TINH TOAN JOIN PHAN TAN...")
seller_perf = fact_items.join(fact_reviews, "order_key") \
    .join(dim_seller, "seller_key") \
    .groupBy("seller_id", "seller_city", "seller_state") \
    .agg(
        F.count("order_item_key").alias("total_items_sold"),
        F.round(F.sum("price"), 2).alias("total_revenue"),
        F.round(F.avg("review_score"), 2).alias("avg_rating")
    ) \
    .filter(F.col("total_items_sold") >= 50) \
    .orderBy(F.col("avg_rating").desc(), F.col("total_revenue").desc())

# 3. In kết quả Top 5 ra terminal
print("\n=== TOP 5 NGUOI BAN XUAT SAC NHAT (>= 50 items) ===")
seller_perf.show(5, truncate=False)

spark.stop()
EOF
```

Chạy script bên trong container Spark Worker:
```bash
docker cp scratch_pyspark_lab.py olist-lakehouse-spark-worker-1:/tmp/
docker exec -it olist-lakehouse-spark-worker-1 python3 /tmp/scratch_pyspark_lab.py
rm -f scratch_pyspark_lab.py
```

---

## 13.3. Lab 3: Diễn tập Khôi phục Dữ liệu (Disaster Recovery Drill) bằng Iceberg Snapshot Rollback

Đây là bài thực hành nâng cao mô phỏng tình huống thảm họa: Một kỹ sư lỡ tay chạy câu lệnh cập nhật đè làm sai lệch dữ liệu bảng `fact_orders`, và người trực vận hành dùng khả năng **Time-Travel Rollback** của Apache Iceberg để cứu dữ liệu về đúng snapshot ban đầu trong 30 giây mà không cần restore backup MinIO!

### Bước 1: Ghi nhận Snapshot ID hiện tại và tổng doanh thu ban đầu
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    snapshot_id AS snapshot_goc, 
    committed_at, 
    summary['total-records'] AS tong_dong
FROM lakehouse.gold.\"fact_orders\$snapshots\" 
ORDER BY committed_at DESC 
LIMIT 1;
"
```
*(Ghi nhớ `snapshot_goc`, ví dụ: `1234567890123456789`)*.

### Bước 2: Giả lập sự cố làm sai lệch dữ liệu (Cố ý chạy UPDATE đè)
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
UPDATE lakehouse.gold.fact_orders 
SET total_order_value = 0.00 
WHERE purchase_date_key >= 20180101;
"
```

Kiểm tra thấy doanh thu bị sai lệch ngay lập tức:
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT sum(total_order_value) AS doanh_thu_bi_pha_hong 
FROM lakehouse.gold.fact_orders;
"
```
*(Con số bị giảm hẳn so với mốc 16.008.872,12 R$ ban đầu!)*

### Bước 3: Thực hiện Rollback tức thì về Snapshot gốc
Sử dụng thủ tục hệ thống của Iceberg:
```bash
# Thay thế SNAPSHOT_ID_GOC bang ID thuc te vua lay o Buoc 1
SNAPSHOT_ID_GOC=$(docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT snapshot_id FROM lakehouse.gold.\"fact_orders\$snapshots\" ORDER BY committed_at ASC LIMIT 1;
" | tr -d ' "\r\n')

echo "Dang rollback ve snapshot: ${SNAPSHOT_ID_GOC}..."

docker exec -it olist-lakehouse-trino-1 trino --execute "
CALL lakehouse.system.rollback_to_snapshot('gold', 'fact_orders', ${SNAPSHOT_ID_GOC});
"
```

### Bước 4: Khẳng định dữ liệu đã phục hồi nguyên vẹn 100%
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT 
    count(*) AS so_don_sau_rollback,
    sum(payment_total) AS tong_thanh_toan_sau_rollback
FROM lakehouse.gold.fact_orders;
"
```
**Kết quả hiển thị chuẩn:**
* Số đơn: **99.441**
* Tổng thanh toán: **16.008.872,12 R$**
> 🎉 **Thảm họa đã được khắc phục hoàn toàn trong 15 giây mà không mất một byte dữ liệu nào!**

---

## 13.4. Lab 4: Tự tạo Semantic View mới trên Trino cho phân tích Marketing

Tạo một Semantic View tổng hợp hiệu quả kinh doanh theo Bang người bán kết hợp với Bang khách hàng (`vw_interstate_trade`):

```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
CREATE OR REPLACE VIEW lakehouse.semantic.vw_interstate_trade AS
SELECT 
    s.seller_state AS bang_ban,
    c.customer_state AS bang_mua,
    CASE WHEN s.seller_state = c.customer_state THEN 'Noi bang (Intra-state)' ELSE 'Lien bang (Inter-state)' END AS loai_thuong_mai,
    count(distinct f.order_key) AS so_don,
    round(sum(i.price), 2) AS tong_tien_hang,
    round(avg(i.freight_value), 2) AS cuoc_phi_tb
FROM lakehouse.gold.fact_orders f
JOIN lakehouse.gold.fact_order_items i ON f.order_key = i.order_key
JOIN lakehouse.gold.dim_seller s ON i.seller_key = s.seller_key
JOIN lakehouse.gold.dim_customer c ON f.customer_key = c.customer_key
GROUP BY s.seller_state, c.customer_state;
"
```

Truy vấn thử View vừa tạo:
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT * FROM lakehouse.semantic.vw_interstate_trade 
ORDER BY tong_tien_hang DESC 
LIMIT 5;
"
```

---

## 13.5. Lab 5: Tự cấu hình một quy tắc Data Contract mới và chạy kiểm chứng

Hãy thử mở file cấu hình hợp đồng dữ liệu [`transformation/quality/contracts.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/transformation/quality/contracts.py) để xem cách khai báo các bài kiểm tra:

```bash
grep -n -A 10 "fact_orders" transformation/quality/contracts.py | head -n 25
```

Thực thi kiểm tra chất lượng riêng trên bảng `fact_orders`:
```bash
docker exec -it olist-lakehouse-spark-worker-1 python3 /opt/airflow/transformation/quality/job.py \
  --run-id "lab5-test-run" \
  --layer "gold" \
  --table "fact_orders"
```

Xem kết quả vừa ghi vào bảng kiểm định:
```bash
docker exec -it olist-lakehouse-trino-1 trino --execute "
SELECT check_name, status, observed_value 
FROM lakehouse.quality.quality_results 
WHERE run_id = 'lab5-test-run'
LIMIT 5;
"
```

---

## 13.6. Lời kết và các bước tiếp theo

Bạn vừa hoàn thành một chuyến tham quan chuyên sâu qua toàn bộ các ngóc ngách kỹ thuật của hệ thống **Olist Data Lakehouse** và thực hành trọn vẹn 5 bài Lab chuyên sâu.

Bằng việc tự tay thực thi các câu lệnh trên, bạn đã trực tiếp kiểm chứng:
1. Tính toàn vẹn của mô hình Medallion và sự mạnh mẽ của Apache Iceberg.
2. Khả năng chịu tải và chống sập của động cơ PySpark với cấu hình AQE và Broadcast Join.
3. Độ tin cậy của 198 bài kiểm định chất lượng và cơ chế cách ly lỗi Quarantine.
4. Khả năng giám sát toàn diện thời gian thực qua Prometheus, Grafana và Superset.
5. Quy trình phục hồi dữ liệu tức thì (Disaster Recovery Time-Travel Rollback) chuẩn doanh nghiệp.

Để tiếp tục chuẩn bị cho lộ trình nâng cấp hệ thống đạt chuẩn Enterprise Production, bạn có thể tham khảo thêm tài liệu phân tích chuyên sâu tại:
👉 [**`docs/reports/DANH_GIA_HIEN_TRANG_VA_LO_TRINH_PRODUCTION.md`**](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/docs/reports/DANH_GIA_HIEN_TRANG_VA_LO_TRINH_PRODUCTION.md)

