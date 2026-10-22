# Hướng Dẫn Bàn Giao Nhiệm Vụ & Quy Chuẩn Kỹ Thuật Dành Cho Agent Kế Thừa

> **Tài liệu chuyển giao kỹ thuật (Agent Handoff Directive) dành riêng cho AI Agent hoặc Kỹ sư kế thừa tiếp nhận dự án Olist Modern Data Lakehouse. Vui lòng đọc kỹ toàn bộ tài liệu này trước khi bắt tay vào chỉnh sửa mã nguồn.**

---

## 1. Tinh Thần Cốt Lõi & Nguyên Tắc Lập Trình (Core Principles)

Dự án này đã trải qua quá trình tái cấu trúc toàn diện để khắc phục các sai lầm về thiết kế của phiên bản cũ. Vì vậy, người kế thừa **bắt buộc phải tuân thủ nghiêm ngặt 3 nguyên tắc sau**:

1. **Triệt để bài trừ sự phức tạp thừa thãi (Anti-Over-Engineering & KISS Principle):**
   * Giữ mọi thứ đơn giản nhất có thể (*Keep It Simple, Stupid*).
   * **TUYỆT ĐỐI KHÔNG** tự chế lại những thứ mà hạ tầng và thư viện chuẩn đã hỗ trợ rất tốt (ví dụ: không tự viết thư viện CLI parser riêng, không tự chế state-machine token bảo mật HMAC phức tạp cho các tác vụ nội bộ đơn giản).
   * Không viết mã dự phòng cho những trường hợp viễn tưởng không có trong yêu cầu (*YAGNI - You Aren't Gonna Need It*).
2. **Mã nguồn phải có chú thích (Comment) chi tiết, dễ hiểu bằng tiếng Việt:**
   * Mọi hàm (function), lớp (class) và đoạn xử lý logic nghiệp vụ phức tạp đều phải có docstring và comment tiếng Việt chuẩn kỹ thuật.
   * Chú thích phải giải thích rõ: **Đầu vào là gì? Xử lý logic gì? Đầu ra là gì? Vì sao lại lựa chọn cách giải quyết này?**
3. **Quy định bắt buộc về Báo Cáo sau khi hoàn thành công việc:**
   * Sau khi hoàn tất một hoặc nhiều nhiệm vụ, bạn **BẮT BUỘC** phải viết một tài liệu báo cáo chi tiết định dạng Markdown (`.md`) lưu tại `docs/reports/BAO_CAO_HOAN_THANH_<TEN_TASK>.md` để người dùng đọc hiểu tường tận những gì bạn đã làm. *(Xem chi tiết cấu trúc báo cáo ở Mục 5).*

---

## 2. Bức Tranh Hiện Trạng Hệ Thống (Current Baseline)

Trước khi thực hiện bất kỳ thay đổi nào, bạn cần nắm rõ trạng thái đã được xác minh 100% của hệ thống:
* **Hạ tầng:** 12 containers Docker đang hoạt động ổn định, phân tầng rõ ràng (MinIO, PostgreSQL, Spark Master/Worker, Trino, Airflow Webserver/Scheduler, Superset, Prometheus, Grafana, Exporters).
* **4 Airflow DAGs cốt lõi:**
  1. `olist_daily_pipeline`: Master pipeline điều phối toàn bộ chu trình nạp, biến đổi và kiểm định chất lượng.
  2. `olist_transformation`: Biến đổi Spark qua 3 tầng Medallion (Bronze 9 bảng $\rightarrow$ Silver 8 bảng + Quarantine $\rightarrow$ Gold 14 bảng Star Schema & Marts).
  3. `olist_quality`: Cổng kiểm định chất lượng tự động hóa với 198 bài kiểm tra Great Expectations.
  4. `olist_maintenance`: Tối ưu hóa bảng Iceberg (Compaction nén tệp Parquet 128MB, Expire snapshots, Dọn dẹp orphan files).
* **Chỉ số kinh doanh chuẩn mực (Conservation of Revenue):**
  * 99,441 đơn hàng.
  * Tiền hàng: `$13,591,643.70` + Tiền cước: `$2,417,228.42` = Tổng thanh toán: **`$16,008,872.12`** *(khớp tuyệt đối giữa Facts, Marts và Semantic Views)*.
* **Giám sát thời gian thực:** Prometheus quét 9/9 targets thành công; Grafana có sẵn 3 dashboards.

---

## 3. Các Sự Cố Đã Gặp Phải (Known Issues & Root Causes - Cần Ghi Nhớ)

Trong quá trình bảo trì và phát triển, hãy luôn ghi nhớ các bài học sau để không bao giờ lặp lại:

### Sự cố 1: Lệch pha Jinja Template giữa các Task trong DAG Bảo trì
* **Bản chất lỗi:** Task khảo sát (`maintenance_dry_run`) ghi nhận run ID tiêu thụ dựa trên `--approval-run-id auto`. Nhưng task nén file (`rewrite_data_files`) dùng template `{{ ts_nodash }}` sinh ra `20261022T101113-maintenance-rewrite`. Cơ chế kiểm tra chuỗi quá khắt khe đã ném lỗi `ValueError: Run tiêu thụ của báo cáo dry run không khớp yêu cầu`.
* **Cách khắc phục chuẩn:** Luôn đồng bộ một biểu thức mẫu dùng chung `run_base = "{% if params.run_id == 'auto' %}{{ ts_nodash }}{% else %}{{ params.run_id }}{% endif %}"` cho mọi tham số liên quan trong DAG.

### Sự cố 2: Nguy cơ đệ quy vô hạn khi retry do cờ `os.O_EXCL`
* **Bản chất lỗi:** Khi mở file ghi báo cáo dry-run, việc dùng cờ độc quyền `os.O_EXCL` khiến lệnh thất bại khi người vận hành Clear/Retry task trong Airflow. Đoạn mã cũ bắt lỗi `FileExistsError` rồi lại gọi đệ quy chính nó với cùng tham số đích, dẫn đến tràn ngăn xếp (`RecursionError`).
* **Cách khắc phục chuẩn:** Sử dụng cờ ghi đè an toàn `os.O_TRUNC` để đảm bảo tính bất biến (Idempotency).

### Sự cố 3: Nút thắt cổ chai tính Hash trên Spark Driver
* **Bản chất lỗi:** Tải toàn bộ bảng địa lý lớn vào bộ nhớ Python Driver để tính băm SHA-256 từng dòng làm cạn kiệt RAM và treo ứng dụng.
* **Cách khắc phục chuẩn:** Luôn đẩy việc tính toán và phân nhóm xuống Spark SQL phân tán trên cụm Worker.

---

## 4. Danh Mục Nhiệm Vụ Cần Triển Khai (Prioritized Actionable Tasks)

Agent kế thừa hãy lần lượt thực hiện các nhiệm vụ sau theo mức độ ưu tiên từ cao xuống thấp:

### Nhiệm Vụ 1: Tinh Giản Toàn Diện Module Bảo Trì [`transformation/maintenance/job.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/transformation/maintenance/job.py)
* **Hiện trạng:** File dài 2,580 dòng, chứa hàng ngàn dòng code token HMAC, state machine phức tạp hóa quá mức.
* **Yêu cầu:**
  * Viết lại một phiên bản tinh gọn khoảng **~200 - 300 dòng**.
  * Loại bỏ hoàn toàn lớp vỏ bọc HMAC token, nonce validation và advisory locks dư thừa (vì Airflow đã đảm nhận việc khóa tài nguyên bằng Pool `lakehouse_writes`).
  * Thực thi trực tiếp 3 thủ tục chuẩn của Apache Iceberg:
    1. Nén tệp: `CALL system.rewrite_data_files(...)`
    2. Cắt tỉa snapshot: `CALL system.expire_snapshots(...)`
    3. Dọn file mồ côi: `CALL system.remove_orphan_files(...)`
  * Ghi kết quả kiểm toán vào bảng `lakehouse.control.maintenance_results`.
  * Đảm bảo DAG `olist_maintenance` chạy thành công mượt mà sau khi tinh giản.

### Nhiệm Vụ 2: Xây Dựng Bộ Script Sinh 3 Tập Dữ Liệu Test Tải & Xử Lý Tăng Dần
* **Hiện trạng:** Hệ thống đang chạy tốt với dữ liệu gốc một lần, cần bộ script tự động hóa sinh dữ liệu test tăng dần và test tải.
* **Yêu cầu:** Viết script Python (ví dụ: `scripts/generate_incremental_datasets.py`) đọc từ dữ liệu thật Olist Kaggle trong `data/raw/` để sinh ra:
  1. **Dataset 1 (Giờ thấp điểm):** Lấy ~500 đơn hàng, đổi timestamp sang ngày mới, mô phỏng micro-batch.
  2. **Dataset 2 (Giờ săn sale đột biến):** Lấy ~30,000 đơn hàng, dồn timestamp vào 1 khung giờ duy nhất (Flash-sale), tăng đột biến số lượng sản phẩm hot để test khả năng chịu tải của Spark Worker.
  3. **Dataset 3 (Dồn tích lớn sau sự cố):** Lấy dữ liệu 1 tháng hoàn chỉnh với nhiều trạng thái cập nhật hỗn hợp (`created` $\rightarrow$ `delivered`), kiểm tra cơ chế `MERGE INTO` (Upsert).

### Nhiệm Vụ 3: Xây Dựng Bộ Script Sinh 3 Tập Dữ Liệu Test Khả Năng Chịu Lỗi (Chaos Testing)
* **Hiện trạng:** Cần chứng minh tính mạnh mẽ của cơ chế kiểm soát chất lượng và Medallion Quarantine Pattern khi gặp dữ liệu dị thường.
* **Yêu cầu:** Viết script Python (ví dụ: `scripts/generate_chaos_datasets.py`) sinh ra:
  1. **Dataset 4 (Metadata bẩn & Schema Drift):** Thêm cột lạ, làm sai định dạng ngày tháng, đưa chuỗi văn bản vào cột số tiền.
  2. **Dataset 5 (Vi phạm Khóa & Dữ liệu Cách Ly):** Null order_id, order_items mồ côi, đơn giá âm. Đảm bảo toàn bộ rơi vào `lakehouse.quarantine.*`.
  3. **Dataset 6 (Bản ghi trùng lặp & Xung đột trạng thái):** Bản ghi trùng khóa chính nhưng khác trạng thái và timestamp. Đảm bảo tầng Silver deduplicate chính xác.

### Nhiệm Vụ 4: Tích Hợp Cảnh Báo Sự Cố Tự Động Qua Email (SMTP Alerting)
* **Hiện trạng:** Khi task thất bại, Airflow mới chỉ ghi log vào file NDJSON cục bộ.
* **Yêu cầu:**
  * Tạo module `infrastructure/alerts/email_alert.py` (dưới 60 dòng) sử dụng thư viện chuẩn `smtplib`.
  * Tích hợp vào hàm `failure_callback` trong `orchestration/dags/common.py` để tự động gửi email cho Admin khi có task bị FAILED.
  * Tích hợp gửi email khi Great Expectations phát hiện kiểm định mức `ERROR`.
  * Cung cấp biến môi trường cấu hình trong `.env` (`ALERT_SMTP_HOST`, `ALERT_SMTP_PORT`, `ALERT_ADMIN_EMAIL`...).

### Nhiệm Vụ 5: Nâng Cấp Bảng Điều Khiển Superset & Grafana
* **Hiện trạng:** Bố cục biểu đồ còn phân tán, người dùng chưa thấy rõ bức tranh kinh doanh và vận hành.
* **Yêu cầu:**
  * **Superset:** Xây dựng biểu đồ Phễu Chuyển Đổi Kinh Doanh (Funnel: Created $\rightarrow$ Approved $\rightarrow$ Delivered $\rightarrow$ Reviewed), đồng bộ bộ lọc toàn cục (Cross-filtering theo ngày, bang, danh mục).
  * **Grafana:** Bổ sung panel hiển thị tỷ lệ cách ly (Quarantine Rate %), cảnh báo đổi màu theo ngưỡng; bổ sung biểu đồ theo dõi xu hướng thời gian chạy của pipeline.

---

## 5. Quy Chuẩn Viết Báo Cáo Sau Khi Hoàn Thành Nhiệm Vụ

Sau khi bạn hoàn thành một hoặc nhiều nhiệm vụ trên, bạn **phải lập một file báo cáo Markdown** (ví dụ: `docs/reports/BAO_CAO_HOAN_THANH_<TEN_NHIEM_VU>.md`) tuân thủ cấu trúc chuẩn sau:

```markdown
# Báo Cáo Hoàn Thành Nhiệm Vụ: [Tên Nhiệm Vụ]

**Ngày thực hiện:** [YYYY-MM-DD]
**Người thực hiện:** [Tên Agent / Kỹ Sư]
**Trạng thái kiểm thử:** 100% Hoàn thành & Đã kiểm chứng

---

## 1. Tóm Tắt Kết Quả (Executive Summary)
- Mô tả ngắn gọn (3-5 câu) mục tiêu đã đạt được và giá trị mang lại cho hệ thống.

## 2. Vấn Đề Trước Khi Xử Lý (Problem Statement)
- Phân tích chi tiết hiện trạng trước khi sửa, tại sao cần phải thay đổi hoặc tối ưu hóa.

## 3. Phương Án & Thiết Kế Kỹ Thuật (Architecture & Solution)
- Giải thích phương pháp tiếp cận theo cách bình dị, trực quan, dễ hiểu bằng tiếng Việt.
- Nêu rõ vì sao giải pháp này tuân thủ nguyên tắc đơn giản (KISS) và không gây over-engineering.

## 4. Chi Tiết Các File & Đoạn Mã Đã Thay Đổi (Code Changes)
- Liệt kê danh sách các file đã sửa hoặc tạo mới kèm đường dẫn tương đối.
- Trích dẫn các đoạn code quan trọng và giải thích ý nghĩa từng đoạn logic.

## 5. Bằng Chứng Kiểm Thử Đầu-Cuối (Verification Proofs)
- Câu lệnh cụ thể đã thực thi trên terminal / container.
- Trích xuất logs kết quả thành công thực tế (SUCCESS / PASS).
- Kết quả đối soát dữ liệu hoặc ảnh chụp/truy vấn chứng minh hệ thống hoạt động chuẩn xác 100%.

## 6. Đề Xuất Cho Bước Tiếp Theo (Next Steps)
- Các lưu ý cần chú ý cho đội ngũ vận hành hoặc các tính năng có thể mở rộng tiếp theo.
```
