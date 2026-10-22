# HƯỚNG DẪN NHANH DÀNH CHO AGENT / KỸ SƯ KẾ THỪA DỰ ÁN
## Quick Onboarding & Handover Briefing (Đọc Tài Liệu Này Đầu Tiên!)

> **Chào bạn!** Bạn vừa nhận bàn giao toàn bộ mã nguồn của hệ thống **Olist Modern Data Lakehouse** (`Big_Data/rebuild`).  
> Tài liệu này được thiết kế để bạn **nắm bắt toàn bộ bức tranh kiến trúc, hiện trạng kỹ thuật và nhiệm vụ cụ thể chỉ trong 5 phút**.

---

## 1. Bức Tranh Dự Án (Executive Summary)

* **Bản chất hệ thống:** Đây là một nền tảng **Modern Data Lakehouse chuẩn công nghiệp** xử lý dữ liệu thương mại điện tử Olist (Kaggle), vận hành theo kiến trúc huy chương **Medallion Architecture (Bronze → Silver → Gold)**.
* **Định dạng lưu trữ:** Bảng mở **Apache Iceberg** lưu trữ trên nền **MinIO Object Storage (S3)**, quản lý metadata qua **PostgreSQL JDBC Catalog**.
* **Công nghệ cốt lõi:**
  * **Xử lý phân tán:** Apache Spark 3.5 (PySpark).
  * **Điều phối tự động:** Apache Airflow 2.11 (4 DAGs).
  * **Phân tích & Phục vụ SQL:** Trino Query Engine (4 Semantic Views bảo mật).
  * **Báo cáo kinh doanh (BI):** Apache Superset (5 Dashboards).
  * **Kiểm định chất lượng:** Great Expectations (198 checks).
  * **Giám sát thời gian thực:** Prometheus (9/9 targets UP) & Grafana (3 Dashboards).
* **Trạng thái hiện tại:** Hệ thống đã được **Rebuild và Verify 100%**, toàn bộ 4 DAGs đều đã chạy thành công (`SUCCESS`), doanh thu đối soát khớp tuyệt đối từng cent: **`$16,008,872.12`** (không mất một đồng nào từ dữ liệu thô sang tầng Gold).
* **Tình trạng máy chủ:** Hiện tại toàn bộ container đang được tạm dừng (`down`) để giải phóng RAM cho máy tính. Bạn có thể bật lại bất cứ lúc nào chỉ với 1 câu lệnh (xem mục 5).

---

## 2. Vai Trò & Tôn Chỉ Lập Trình Bắt Buộc

Bạn đóng vai trò là **Senior Data / DataOps Engineer** nhận bàn giao từ **Tech Lead (Sếp)**. Bạn bắt buộc phải ghi nhớ 3 nguyên tắc thép:

1. **Triệt để bài trừ Over-Engineering (KISS Principle):** Dự án này từng bị một đội ngũ cũ làm phức tạp hóa quá mức (tự viết CLI parser tiếng Việt, viết cơ chế HMAC lock 2,500 dòng làm sập Airflow). Nhóm hiện tại đã dọn sạch. Bạn **tuyệt đối không được tự chế framework riêng**, giữ code đơn giản, thanh thoát và dùng thư viện chuẩn.
2. **Comment tiếng Việt chi tiết:** Mọi hàm, class, module bạn viết phải có chú thích tiếng Việt chuẩn mực: *Đầu vào là gì? Logic làm gì? Đầu ra là gì? Vì sao chọn giải pháp này?*
3. **Kỷ luật thứ tự công việc (Quy trình 4 Giai đoạn):**
   * **Giai đoạn 1:** Tập trung làm cho xong **5 tasks kỹ thuật hiện tại** (đây là khối lượng công việc chính, rất lớn và quan trọng).
   * **Giai đoạn 2:** Chạy bài test **Benchmark hiệu năng** trên cụm 12 container với các dataset vừa tạo.
   * **Giai đoạn 3:** Rà soát và cập nhật lại toàn bộ tài liệu trong thư mục `docs/`.
   * **Giai đoạn 4:** Lập Báo cáo nghiệm thu chi tiết, tự nghiên cứu đề xuất **5 đến 10 ý tưởng nâng cấp siêu chi tiết** và **trình Sếp duyệt**. Cấm tự ý code ý tưởng mới khi Sếp chưa phê duyệt!

---

## 3. Bản Đồ Điều Hướng Tài Liệu (Bắt Buộc Phải Đọc)

Hệ thống tài liệu đã được chuẩn bị đầy đủ trong thư mục `docs/`. Bạn cần đọc theo thứ tự ưu tiên sau:

| Thứ tự | Tên File Tài Liệu | Vai Trò & Điều Bạn Cần Nắm |
|:---:|:---|:---|
| 🥇 **1** | [**`docs/AGENT_HANDOFF_TASKS.md`**](docs/AGENT_HANDOFF_TASKS.md) | **VĂN BẢN CHỈ THỊ CHÍNH THỨC TỪ TECH LEAD.** Đọc kỹ để nắm rõ quy trình 4 giai đoạn, các bài học từ sự cố quá khứ và tiêu chuẩn làm việc. |
| 🥈 **2** | [**`docs/RECOMMEND_UPDATES.md`**](docs/RECOMMEND_UPDATES.md) | **Phụ lục đặc tả kỹ thuật chi tiết:** Mô tả cấu trúc 6 bộ dataset (tải/tăng dần & chaos test), code mẫu SMTP alert, cờ cấu hình Spark AQE. |
| 🥉 **3** | [**`docs/Y_TUONG_NANG_CAP_DU_AN.md`**](docs/Y_TUONG_NANG_CAP_DU_AN.md) | **Đề bài & Tiêu chuẩn ý tưởng:** Đọc để hiểu tiêu chuẩn "siêu chi tiết" (6 mục bắt buộc) và 5 trục tư duy để tự động não đề xuất 5-10 ý tưởng trình Sếp. |
| 4 | [**`docs/SO_SANH_BAN_CU_VA_MOI.md`**](docs/SO_SANH_BAN_CU_VA_MOI.md) | So sánh bản cũ over-engineered vs. bản mới để hiểu lý do vì sao hệ thống được thiết kế như hiện tại. |
| 5 | [**`docs/data_model/star_schema.md`**](docs/data_model/star_schema.md) | Mô hình Star Schema chuẩn Kimball đã được Sếp trực tiếp sửa lỗi chart Mermaid (bỏ fact-to-fact, tách Marts lineage). |
| 6 | [**`README.md`**](README.md) & [**`NHAT_KY_REBUILD_LAKEHOUSE.md`**](NHAT_KY_REBUILD_LAKEHOUSE.md) | Bản đồ danh mục cổng dịch vụ và toàn văn nhật ký 12 chặng rebuild chi tiết từ con số 0. |

---

## 4. Checklist 5 Tasks Cần Làm Ngay (Giai Đoạn 1)

Cậu hãy mở IDE và lần lượt thực hiện 5 task sau theo thứ tự:

- [ ] **Task 1 (Tinh giản Maintenance):** Viết lại module [`transformation/maintenance/job.py`](transformation/maintenance/job.py) từ 2,580 dòng về khoảng **~200 dòng**, bỏ toàn bộ HMAC token và lease lock, gọi trực tiếp 3 thủ tục Iceberg: `rewrite_data_files` (nén 128MB), `expire_snapshots`, `remove_orphan_files`. Đảm bảo DAG `olist_maintenance` chạy thành công.
- [ ] **Task 2 (Sinh 3 Dataset Test Tải & Tăng Dần):** Viết script Python đọc từ dữ liệu thật `data/raw/` để sinh:
  1. *Dataset 1 (Giờ thấp điểm):* 500 đơn hàng, test micro-batch và nạp tăng dần không bị trùng lặp.
  2. *Dataset 2 (Giờ săn sale Flash-Sale):* 30,000 - 50,000 đơn hàng dồn dập trong 2 giờ, test tải nặng xem Spark Worker có bị OOM không và Airflow pool có điều tiết tốt không.
  3. *Dataset 3 (Dồn tích Catch-up):* Toàn bộ 1 tháng đơn hàng có trạng thái cập nhật hỗn hợp, test cơ chế `MERGE INTO` (Upsert).
- [ ] **Task 3 (Sinh 3 Dataset Test Chịu Lỗi - Chaos):** Viết script Python tiêm lỗi:
  1. *Dataset 4 (Metadata bẩn):* Thêm cột lạ, ngày tháng sai định dạng, chuỗi text trong cột giá.
  2. *Dataset 5 (Vi phạm khóa & Quarantine):* Null `order_id`, khóa ngoại mồ côi, đơn giá âm. Đảm bảo toàn bộ bị bóc tách vào `lakehouse.quarantine.*`.
  3. *Dataset 6 (Bản ghi trùng lặp):* Cùng khóa chính nhưng khác trạng thái và timestamp. Đảm bảo Silver deduplicate chuẩn xác.
- [ ] **Task 4 (Cảnh Báo Email SMTP):** Tạo module `infrastructure/alerts/email_alert.py` (< 60 dòng) dùng `smtplib`, kết nối vào `failure_callback` trong `orchestration/dags/common.py` để tự động gửi email cho Admin khi có task Airflow bị FAILED hoặc Great Expectations phát hiện kiểm định `ERROR`.
- [ ] **Task 5 (Tối Ưu Spark & Dashboard):** Thêm gợi ý `broadcast()` cho các bảng chiều nhỏ trong `gold/job.py`; bật Spark Adaptive Query Execution (AQE); bổ sung biểu đồ phễu kinh doanh (Funnel) trên Superset và panel Quarantine Rate (%) trên Grafana.

---

## 5. Cheatsheet Lệnh Vận Hành Nhanh (Dành Cho Kỹ Sư)

### 5.1. Nạp Images & Bật/Tắt Hệ Thống (12 Containers)
```bash
# 1. Nạp sẵn toàn bộ Docker Images đã đóng gói (tránh mất thời gian build lại):
docker load -i olist-lakehouse-images.tar

# 2. Khởi động toàn bộ 12 container dịch vụ
docker compose --project-name olist-lakehouse \
  -f docker-compose.yml \
  -f docker-compose.query.yml \
  -f docker-compose.orchestration.yml \
  -f docker-compose.bi.yml \
  -f docker-compose.monitoring.yml \
up -d

# 3. Tắt toàn bộ container để giải phóng RAM (bảo toàn nguyên vẹn 100% dữ liệu)
docker compose --project-name olist-lakehouse \
  -f docker-compose.yml \
  -f docker-compose.query.yml \
  -f docker-compose.orchestration.yml \
  -f docker-compose.bi.yml \
  -f docker-compose.monitoring.yml \
down --remove-orphans
```

### 5.2. Các Cổng Truy Cập Web UI
* **Airflow UI:** `http://localhost:8083` (`admin` / `admin`)
* **Trino UI:** `http://localhost:8082` (user `trino`)
* **Apache Superset BI:** `http://localhost:8088` (`olist_admin` / `admin`)
* **MinIO Console:** `http://localhost:9001` (`root` / `admin_root`)
* **Prometheus:** `http://localhost:9090` (Targets: `http://localhost:9090/targets`)
* **Grafana:** `http://localhost:3000` (`admin` / `admin`)
* **Spark Master UI:** `http://localhost:8084`

### 5.3. Kích Hoạt DAGs Bằng Lệnh Airflow CLI
```bash
# Chạy toàn bộ luồng Ingestion -> Silver -> Gold -> Quality
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_daily_pipeline

# Chạy riêng kiểm định chất lượng (198 checks)
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_quality

# Chạy bảo trì Iceberg nén tệp & dọn snapshot
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_maintenance
```

### 5.4. Lệnh Trino Đối Soát Doanh Thu
```bash
docker exec -i olist-lakehouse-trino-1 trino --execute \
  "SELECT count(*) total_orders, sum(item_revenue) item_rev, sum(freight_value) freight, sum(payment_total) payment FROM lakehouse.gold.fact_orders;"
# Kết quả kỳ vọng: 99441 | 13591643.70 | 2417228.42 | 16008872.12
```

---

> [!TIP]
> Hãy mở file [**`docs/AGENT_HANDOFF_TASKS.md`**](docs/AGENT_HANDOFF_TASKS.md) ngay bây giờ để đọc chi tiết chỉ đạo từ Tech Lead và bắt tay vào Giai đoạn 1! Chúc bạn làm việc hiệu quả và thành công!
