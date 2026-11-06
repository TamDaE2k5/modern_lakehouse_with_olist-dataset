# Đề Xuất Nâng Cấp Kỹ Thuật & Hoàn Thiện Hệ Thống (Recommend Updates)

> **Tài liệu tổng hợp các hạng mục cải tiến kỹ thuật, kịch bản tạo 6 bộ dữ liệu mẫu (synthetic datasets), cơ chế cảnh báo qua Email (SMTP Alerting), chiến lược tối ưu hóa Spark và nâng cấp bảng điều khiển BI/Monitoring cho dự án Olist Data Lakehouse.**

---

> [!NOTE]
> ## TRẠNG THÁI THỰC HIỆN — ĐÃ HOÀN THÀNH TOÀN BỘ
>
> Tài liệu này là **bản đặc tả yêu cầu** được viết trước khi bắt tay làm. Dưới đây là đối chiếu
> giữa từng hạng mục đã đặc tả và kết quả thực tế, kèm nơi tìm bằng chứng:
>
> | # | Hạng mục đã đặc tả | Trạng thái | Bằng chứng |
> |:-:|:---|:---:|:---|
> | 1 | Kiểm thử xử lý tăng dần & mô phỏng CDC | ✅ | 3 bộ dữ liệu trong `data/synthetic/01..03`, số liệu đo trong `docs/reports/benchmarks/` |
> | 2 | Kiểm tra nhất quán dữ liệu & cô lập snapshot | ✅ | Trino đọc trong lúc Spark ghi suốt các bài benchmark, không xảy ra đọc bẩn |
> | 3 | Tối ưu hiệu năng Spark Job | ✅ | `F.broadcast()` tại 4 điểm tra cứu chiều; AQE đầy đủ trong `transformation/common/spark_session.py` |
> | 4 | 6 bộ dữ liệu mẫu | ✅ | `scripts/generate_load_datasets.py` + `scripts/generate_chaos_datasets.py` |
> | 5 | Cảnh báo sự cố qua Email SMTP | ✅ | `infrastructure/alerts/email_alert.py` + container MailHog, đã kiểm chứng gửi thật |
> | 6 | Hoàn thiện Dashboards Superset & Grafana | ✅ | Biểu đồ phễu + bộ lọc đồng bộ; gauge Quarantine Rate đổi màu theo ngưỡng |
> | 7 | Loại bỏ over-engineering trong `maintenance/job.py` | ✅ | 2.569 → 627 dòng (383 dòng code thực), DAG chạy 5/5 task SUCCESS |
>
> **Hai điểm thực tế khác với đặc tả ban đầu — và lý do:**
> * **Mục 4.2 đề xuất SMTP Gmail.** Thực tế dùng container **MailHog** làm SMTP giả lập, để không
>   phải đưa mật khẩu ứng dụng Gmail vào file `.env`, đồng thời có Web UI xem lại thư làm bằng chứng
>   nghiệm thu. Chuyển sang Gmail thật chỉ cần đổi các biến `ALERT_SMTP_*`, không sửa một dòng code.
> * **Mục 7 đề xuất rút về ~200 dòng.** Kết quả thực tế là **627 dòng tổng / 383 dòng code thực**.
>   Phần chênh lệch là chú thích tiếng Việt chi tiết theo đúng yêu cầu của Tech Lead (118 dòng
>   docstring + 41 dòng comment + 85 dòng trống), cộng với danh sách trắng 31 bảng được phép bảo trì
>   viết tường minh — đây là ranh giới an toàn chống xóa nhầm dữ liệu, cố ý không rút gọn.
>
> Báo cáo nghiệm thu đầy đủ: [`docs/reports/BAO_CAO_HOAN_THANH_TOAN_DIEN_LAKEHOUSE.md`](reports/BAO_CAO_HOAN_THANH_TOAN_DIEN_LAKEHOUSE.md)

---

## 1. Danh Mục Các Vấn Đề Cần Hoàn Thiện Tiếp Theo

Dựa trên quá trình kiểm định thực tế và các ghi chú trong [NHAT_KY_REBUILD_LAKEHOUSE.md](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/NHAT_KY_REBUILD_LAKEHOUSE.md#L1415-L1436), hệ thống đã đạt mức ổn định 100% về mặt chức năng và logic nghiệp vụ nhưng vẫn còn các không gian tối ưu hóa kỹ thuật quan trọng sau:

1. **Kiểm thử Xử lý Tăng Dần (Incremental Processing & CDC Simulation):** Hiện tại pipeline chủ yếu được xác minh ở chế độ nạp toàn bộ (`mode="full"`). Cần thiết kế kịch bản và tập dữ liệu để kiểm thử chế độ nạp gia tăng (`mode="incremental"`), kiểm tra cơ chế `MERGE INTO` của Apache Iceberg và đảm bảo tính bất biến (Idempotency).
2. **Kiểm tra Tính Nhất Quán Dữ Liệu (Data Consistency & Snapshot Isolation):** Đánh giá việc đọc/ghi đồng thời giữa Spark Ingestion và Trino Query Engine thông qua cơ chế MVCC của Apache Iceberg.
3. **Tối ưu Hóa Hiệu Năng Spark Job:** Áp dụng Broadcast Join, Partition Pruning, điều chỉnh Adaptive Query Execution (AQE) và cân đối lại kích thước file ghi nén.
4. **Xây dựng 6 Bộ Dữ Liệu Mẫu (Synthetic Datasets):** Gồm 3 bộ thử nghiệm tải/tăng dần và 3 bộ thử nghiệm khả năng chịu lỗi (fault-tolerance & chaos testing).
5. **Cơ chế Cảnh báo Sự cố qua Email (SMTP Alerting):** Tự động gửi email thông báo cho Quản trị viên (Admin) ngay khi có task Airflow bị lỗi hoặc dữ liệu vi phạm ngưỡng kiểm định Great Expectations.
6. **Hoàn thiện Bộ Dashboards Superset & Grafana:** Tái cấu trúc lại luồng hiển thị số liệu kinh doanh và panel kỹ thuật để người dùng dễ nắm bắt nhất.
7. **Loại bỏ Hoàn Toàn Mã Nguồn Over-engineering Tồn Đọng:** Viết lại module `transformation/maintenance/job.py` từ ~2,400 dòng xuống ~200 dòng gọi trực tiếp Iceberg procedures.

---

## 2. Kế Hoạch Xây Dựng 3 Tập Dữ Liệu Test Tải & Xử Lý Tăng Dần

Để kiểm thử khả năng co giãn và khả năng xử lý gia tăng mà không cần dữ liệu giả lập phi thực tế, chúng ta sẽ **lấy chính tập dữ liệu thật Olist Kaggle hiện có và biến tấu có chủ đích**:

```
                                  ┌──► Dataset 1: Giờ Thấp Điểm (Off-Peak / Micro-batch)
Tập Dữ Liệu Gốc Olist CSVs ──────┼──► Dataset 2: Khung Giờ Săn Sale (Peak Traffic / Surge)
(99,441 đơn hàng thực tế)         └──► Dataset 3: Dồn Tích Khối Lượng Lớn (Catch-up Batch)
```

### 2.1. Dataset 1: Khung Giờ Thấp Điểm (Off-Peak / Low-Volume Micro-batch)
* **Ý nghĩa thực tế:** Mô phỏng các khung giờ đêm muộn (02:00 - 05:00 sáng) hoặc ngày bình thường, khi lưu lượng giao dịch diễn ra rải rác và số lượng đơn hàng rất ít.
* **Cách thức tạo từ dữ liệu thật:**
  * Lấy ngẫu nhiên khoảng **500 - 1,000 đơn hàng** từ `orders.csv` và các dòng tương ứng trong `order_items.csv`, `order_payments.csv`.
  * Điều chỉnh `order_purchase_timestamp` thành ngày kế tiếp (ví dụ: `2026-10-23 03:xx:xx`).
  * Gán nhãn batch mới: `batch_id = "batch_incremental_offpeak"`.
* **Mục tiêu kiểm thử:**
  * Kiểm tra xem Spark và Airflow khi xử lý tập dữ liệu nhỏ có bị overhead quá mức không (small-file problem).
  * Kiểm tra tính đúng đắn của logic `mode="incremental"`: Các đơn hàng mới nạp vào bảng Iceberg mà không làm nhân đôi (duplicate) hay ghi đè mất dữ liệu cũ.

### 2.2. Dataset 2: Khung Giờ Săn Sale Đột Biến (Flash-Sale Traffic Surge)
* **Ý nghĩa thực tế:** Mô phỏng các đợt "Flash-Sale / Siêu Sale 11/11, 12/12" với lưu lượng đơn hàng tăng đột biến gấp **10 - 50 lần** bình thường trong một khoảng thời gian ngắn (12:00 - 14:00).
* **Cách thức tạo từ dữ liệu thật:**
  * Trích xuất khoảng **30,000 - 50,000 đơn hàng** từ dữ liệu gốc.
  * Ép toàn bộ mốc thời gian mua hàng (`order_purchase_timestamp`) tập trung dồn dập vào cùng 1 khung giờ duy nhất (ví dụ: `2026-10-23 12:00:00` đến `2026-10-23 13:59:59`).
  * Nhân bản ngẫu nhiên các mặt hàng bán chạy (Hot deals) có `product_id` thuộc top danh mục phổ biến (Bed Bath Table, Health & Beauty).
* **Mục tiêu kiểm thử:**
  * **Test khả năng chịu tải (Stress testing):** Xem Spark Worker có bị tràn bộ nhớ (Out-Of-Memory) hay executor bị mất kết nối (Heartbeat timeout) khi shuffle dữ liệu dồn nén hay không.
  * **Kiểm tra cơ chế Điều tiết Ghi (Write Throttling):** Đảm bảo Airflow Pool `lakehouse_writes` giữ vững slot giới hạn, không để quá nhiều job Spark đồng thời làm nghẽn MinIO S3 hoặc xung đột ghi (CommitConflictException) trên Iceberg catalog.

### 2.3. Dataset 3: Dồn Tích Sau Gián Đoạn (Catch-up / Backlog Batch)
* **Ý nghĩa thực tế:** Mô phỏng tình huống hệ thống gặp sự cố mạng hoặc downtime kéo dài 3-5 ngày, sau đó toàn bộ dữ liệu dồn ứ được nạp bù cùng một lúc.
* **Cách thức tạo từ dữ liệu thật:**
  * Tạo một batch chứa toàn bộ đơn hàng của 1 tháng hoàn chỉnh (~10,000 - 15,000 đơn hàng) với nhiều trạng thái hỗn hợp: vừa có đơn mới tạo (`created`), vừa có đơn cập nhật từ hôm trước sang đã thanh toán (`invoiced`) hoặc đã giao (`delivered`).
* **Mục tiêu kiểm thử:**
  * Kiểm tra cơ chế **Upsert (Cập nhật hoặc Chèn mới)**: Các đơn hàng cũ phải được cập nhật trạng thái mới nhất mà không tạo ra bản ghi thừa.
  * Kiểm tra thời gian bắt kịp (Catch-up lag) và sự ổn định của hệ thống giám sát Prometheus.

---

## 3. Kế Hoạch Xây Dựng 3 Tập Dữ Liệu Test Khả Năng Chịu Lỗi (Chaos Testing)

Hệ thống Data Lakehouse sản xuất bắt buộc phải có cơ chế phòng vệ tự động trước dữ liệu bẩn và metadata biến dạng:

```
Tập Dữ Liệu Bẩn ──► Spark Silver Transformation ──┬──► Dữ liệu Hợp lệ ──► lakehouse.silver.*
                                                  └──► Dữ liệu Lỗi     ──► lakehouse.quarantine.*
```

### 3.1. Dataset 4: Metadata Bẩn & Biến Dạng Cấu Trúc (Schema Drift & Dirty Metadata)
* **Các lỗi chủ động tiêm vào dữ liệu (Injected Faults):**
  * Thêm các cột metadata lạ không khai báo trong schema (ví dụ: `promotional_code_internal`, `hack_tracking_id`).
  * Sai lệch định dạng ngày tháng: Trộn lẫn ISO timestamp chuẩn (`2026-10-23T10:15:00`) với định dạng không hợp lệ (`23/10/26 10h15m`, `NULL`, chuỗi rác `"N/A"`, `"invalid_date"`).
  * Thay đổi kiểu dữ liệu cột số: Cột `price` hoặc `freight_value` bị tiêm chuỗi văn bản (`"free_shipping"`, `"1.250,50R$"`).
* **Mục tiêu kiểm thử:**
  * Đảm bảo Spark Schema Evolution hoặc Schema Enforcement xử lý đúng quy tắc: Không làm sập pipeline (crash) mà chuyển đổi kiểu an toàn hoặc gán `NULL` có kiểm soát.

### 3.2. Dataset 5: Vi Phạm Toàn Vẹn Khóa & Dữ Liệu Cách Ly (Integrity & Quarantine Violations)
* **Các lỗi chủ động tiêm vào dữ liệu:**
  * `order_id` hoặc `customer_id` bị để trống (`NULL` hoặc chuỗi rỗng `""`).
  * Khóa ngoại mồ côi (Orphan Foreign Keys): Bản ghi trong `order_items` trỏ đến một `order_id` không hề tồn tại trong bảng `orders`.
  * Giá trị logic phi lý: Đơn giá âm (`price = -50.0`), số kỳ trả góp âm (`payment_installments = -2`), tổng thanh toán bằng 0 nhưng trạng thái đơn hàng là `"delivered"`.
* **Mục tiêu kiểm thử:**
  * Kiểm tra tính nghiêm ngặt của mô hình **Medallion Quarantine Pattern**: Toàn bộ các bản ghi sai phạm này bắt buộc phải bị bóc tách đưa vào các bảng cách ly `lakehouse.quarantine.orders`, `lakehouse.quarantine.order_items`, không được phép lọt vào tầng Silver hay Gold.
  * Bộ quy tắc Great Expectations phải bắt được vi phạm ở mức `ERROR` hoặc `WARN`.

### 3.3. Dataset 6: Bản Ghi Trùng Lặp & Xung Đột Trạng Thái (Duplicates & Conflicting Updates)
* **Các lỗi chủ động tiêm vào dữ liệu:**
  * Trùng lặp hoàn toàn (Exact Duplicates): Nhân đôi 1,000 dòng dữ liệu giống hệt nhau về mọi trường.
  * Trùng khóa chính nhưng khác nội dung (Primary Key Collision): Hai dòng có cùng `order_id` nhưng một dòng ghi trạng thái `shipped`, một dòng ghi trạng thái `canceled` với timestamp chênh nhau chỉ vài giây.
* **Mục tiêu kiểm thử:**
  * Đánh giá thuật toán chống trùng lặp (Deduplication) ở tầng Silver: Sử dụng `row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC)` để chỉ giữ lại bản ghi mới nhất và chuẩn xác nhất.

---

## 4. Tích Hợp Cảnh Báo Sự Cố Qua Email (SMTP Alerting)

### 4.1. Kiến Trúc Cảnh Báo Đơn Giản & Hiệu Quả
Thay vì triển khai các hệ thống thông báo quá phức tạp, chúng ta tích hợp cảnh báo Email trực tiếp qua giao thức chuẩn **SMTP**:

```
Airflow Task Thất bại (failure_callback) ──┐
                                          ├──► Python SMTP Alert Script ──► Email Quản Trị Viên (Admin)
Great Expectations phát hiện ERROR ───────┘
```

### 4.2. Kịch bản Thực thi Cụ thể: Module `infrastructure/alerts/email_alert.py`
Chỉ cần một script Python gọn nhẹ (dưới 60 dòng), sử dụng thư viện tiêu chuẩn `smtplib` và `email`:

```python
"""Module phát cảnh báo sự cố qua giao thức SMTP tiêu chuẩn."""
from email.message import EmailMessage
import os
import smtplib

def send_alert_email(subject: str, body_text: str, severity: str = "ERROR") -> None:
    smtp_host = os.environ.get("ALERT_SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("ALERT_SMTP_PORT", 587))
    smtp_user = os.environ.get("ALERT_SMTP_USER", "admin@lakehouse.local")
    smtp_password = os.environ.get("ALERT_SMTP_PASSWORD", "")
    admin_recipient = os.environ.get("ALERT_ADMIN_EMAIL", "admin@company.com")

    msg = EmailMessage()
    msg["Subject"] = f"[{severity}] Olist Lakehouse Alert: {subject}"
    msg["From"] = smtp_user
    msg["To"] = admin_recipient
    msg.set_content(body_text)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.starttls()
            if smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)
    except Exception as exc:
        # Ghi log an toàn, không làm gián đoạn luồng chính của pipeline
        print(f"Không thể gửi email cảnh báo: {exc}")
```

### 4.3. Tích hợp Vào Airflow Callback
Trong [`rebuild/orchestration/dags/common.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/orchestration/dags/common.py#L351-L355), tại hàm `failure_callback`, chỉ cần thêm 2 dòng để tự động gửi thông báo khi có task thất bại:
```python
def failure_callback(context: Mapping[str, object]) -> None:
    destination = Path(os.environ.get("AIRFLOW_FAILURE_LOG", "/opt/airflow/logs/failures.ndjson"))
    write_failure_event(context, destination)
    
    # Gửi email cảnh báo tức thì cho Quản trị viên
    from infrastructure.alerts.email_alert import send_alert_email
    task_id = context.get("task_instance").task_id
    dag_id = context.get("dag").dag_id
    send_alert_email(
        subject=f"Task {task_id} trong DAG {dag_id} thất bại!",
        body_text=f"Chi tiết: DAG={dag_id}, Task={task_id}, RunID={context.get('run_id')}",
        severity="CRITICAL"
    )
```

---

## 5. Tối Ưu Hóa Hiệu Năng Spark Jobs

Dựa trên cấu hình hiện tại (Worker 2 Cores, 4 GiB RAM), cần áp dụng các kỹ thuật sau:
1. **Broadcast Join cho các Bảng Chiều Nhỏ:**
   * Trong job Gold [`transformation/gold/job.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/transformation/gold/job.py), các bảng chiều nhỏ (`dim_date`, `dim_product`, `dim_seller`) cần được bọc gợi ý `broadcast(dim_df)` khi join với bảng sự kiện lớn `fact_orders` và `fact_order_items`. Điều này loại bỏ hoàn toàn giai đoạn Shuffle nặng nề qua mạng.
2. **Kích hoạt & Tinh chỉnh Adaptive Query Execution (AQE):**
   * Bật các tham số AQE:
     ```text
     spark.sql.adaptive.enabled = true
     spark.sql.adaptive.coalescePartitions.enabled = true
     spark.sql.adaptive.skewJoin.enabled = true
     ```
   * Tự động gộp các phân vùng nhỏ sau bước shuffle, ngăn ngừa việc sinh ra hàng trăm tác vụ Spark xử lý các partition rỗng.
3. **Partition Pruning theo Ngày:**
   * Các truy vấn trên `mart_daily_revenue` và `fact_orders` cần tận dụng partition `order_purchase_date` để Trino và Spark chỉ quét đúng các tệp Parquet của khoảng thời gian truy vấn, giảm 80% dung lượng I/O từ MinIO S3.

---

## 6. Nâng Cấp & Chuẩn Hóa Dashboards Superset & Grafana

### 6.1. Tối ưu Hóa Apache Superset BI
* **Bổ sung Phễu Chuyển Đổi Kinh Doanh (E-Commerce Funnel Analysis):**
  * Thiết kế biểu đồ phễu trực quan: `Đơn đã đặt (created)` $\rightarrow$ `Đã duyệt thanh toán (approved)` $\rightarrow$ `Đã giao bên vận chuyển (carrier)` $\rightarrow$ `Giao hàng thành công (delivered)` $\rightarrow$ `Khách hàng đánh giá (reviewed)`.
* **Đồng bộ Hóa Bộ Lọc Toàn Cục (Cross-Dashboard Filtering):**
  * Thống nhất các bộ lọc: Khoảng ngày mua hàng (`purchase_date`), Bang/Thành phố của khách hàng (`customer_state`), Danh mục sản phẩm (`product_category_name`). Khi người dùng chọn lọc ở góc trên dashboard, tất cả biểu đồ con phải tự động lọc đồng bộ.
* **Biểu đồ Cảnh báo Tỷ lệ Trả lời Đánh giá Xấu (Negative Review Tracker):**
  * Theo dõi các đơn hàng bị đánh giá 1 sao hoặc 2 sao theo từng nhóm nhà bán lẻ (`seller_id`) để đội ngũ CSKH can thiệp kịp thời.

### 6.2. Hoàn thiện Grafana Observability Dashboards
* **Bổ sung Panel "Tỷ lệ Dữ liệu Cách Ly" (Quarantine Rate %):**
  * Hiển thị tỷ lệ phần trăm giữa số bản ghi bị cách ly chia cho tổng số bản ghi nạp vào. Nếu tỷ lệ này vượt quá 5%, đổi màu panel từ Xanh $\rightarrow$ Vàng $\rightarrow$ Đỏ.
* **Theo dõi Thời gian Thực thi Pipeline (Pipeline Duration Trend):**
  * Vẽ biểu đồ đường theo dõi thời gian thực thi của từng DAG theo ngày. Giúp phát hiện sớm hiện tượng suy giảm hiệu năng (Performance Degradation) khi khối lượng dữ liệu phình to.

---

## 7. Kế Hoạch Tinh Giản Module Bảo Trì `job.py` (Khử Over-Engineering)

Hiện tại file [`transformation/maintenance/job.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/transformation/maintenance/job.py) dài tới 2,580 dòng với hệ thống cấp phát lease token HMAC rất phức tạp. Kế hoạch tinh gọn sẽ tái cấu trúc thành một script thanh thoát ~200 dòng:
* Loại bỏ toàn bộ HMAC verification, nonce validation và file descriptor locks.
* Tập trung vào nghiệp vụ cốt lõi thông qua 3 lệnh SQL Iceberg tiêu chuẩn:
  1. `CALL system.rewrite_data_files(table => 'gold.fact_orders', options => map('target-file-size-bytes','134217728'))`
  2. `CALL system.expire_snapshots(table => 'gold.fact_orders', older_than => TIMESTAMP '<threshold>')`
  3. `CALL system.remove_orphan_files(table => 'gold.fact_orders', older_than => TIMESTAMP '<threshold>')`
* Ghi trực tiếp kết quả số lượng file nén và dung lượng thu hồi vào bảng `lakehouse.control.maintenance_results`.
