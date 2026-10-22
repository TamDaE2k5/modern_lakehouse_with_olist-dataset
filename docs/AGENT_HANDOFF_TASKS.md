# CHỈ THỊ CÔNG VIỆC TỪ TECH LEAD DÀNH CHO SENIOR DATA ENGINEER
## Kế Thừa, Hoàn Thiện Hệ Thống & Đề Xuất Nâng Cấp Olist Data Lakehouse

> **Người giao việc (Sếp / Tech Lead):** Trưởng nhóm Dữ liệu & Kiến trúc sư Nền tảng  
> **Người nhận nhiệm vụ:** Senior Data / DataOps Engineer (Agent Kế Thừa)  
> **Dự án:** Olist Modern Data Lakehouse Platform (`Big_Data/rebuild`)  
> **Mục tiêu tối thượng:** Ổn định – Tinh gọn – Kiểm chứng Benchmark – Chuẩn hóa Tài liệu – Đề xuất Ý tưởng chờ duyệt.

---

## 1. Thông Điệp & Tinh Thần Chỉ Đạo Từ Sếp

Chào cậu, tôi bàn giao lại toàn bộ hệ thống **Olist Modern Data Lakehouse** này cho cậu tiếp quản và hoàn thiện nốt các hạng mục còn dang dở. 

Tôi và team vừa trải qua một đợt tái cấu trúc (rebuild) cực kỳ căng thẳng để kéo toàn bộ dự án ra khỏi vũng lầy **over-engineering** của đội ngũ cũ (họ tự chế bộ phân tích cú pháp CLI tiếng Việt, viết cơ chế HMAC lease lock phức tạp dài tới 2,500 dòng làm sập Airflow, tính hash SHA256 từng dòng trên Spark Driver gây nghẽn RAM). Đến thời điểm hiện tại, hạ tầng 12 container, 4 DAGs và định luật bảo toàn doanh thu `$16,008,872.12` đã chạy thông suốt và được xác minh 100%.

Tuy nhiên, khối lượng công việc tồn đọng để hệ thống đạt chuẩn xuất sắc (Production-Grade) vẫn còn rất lớn. Do đó, **tôi yêu cầu cậu tuyệt đối tuân thủ 4 nguyên tắc sau**:

1. **TẬP TRUNG HOÀN THIỆN CÁC TASK HIỆN TẠI TRƯỚC TIÊN:** Tuyệt đối không được vội vàng "vẽ voi vẽ chuột" hay tự ý code các tính năng mới cao siêu. Hoàn thiện các task và fix các issue hiện tại của dự án đã là một khối lượng công việc rất lớn và thử thách rồi!
2. **TRIỆT ĐỂ BÀI TRỪ OVER-ENGINEERING (KISS PRINCIPLE):** Code đơn giản, chuẩn mực, dùng thư viện tiêu chuẩn (Python standard library, PySpark SQL, Airflow built-ins). Không tự chế framework riêng, không viết code dự phòng cho những thứ viễn tưởng.
3. **CODE PHẢI CÓ COMMENT TIẾNG VIỆT CHI TIẾT:** Mọi module, hàm, class cậu viết phải có docstring và chú thích rõ ràng: *Đầu vào là gì? Xử lý logic gì? Đầu ra là gì? Vì sao lại chọn cách làm này?*
4. **KHÔNG TỰ Ý TRIỂN KHAI Ý TƯỞNG KHI CHƯA ĐƯỢC DUYỆT:** Sau khi làm xong hết các task, cậu phải chạy benchmark, sửa lại tài liệu và viết báo cáo đề xuất từ **5 đến 10 ý tưởng nâng cấp siêu chi tiết**. Cậu phải **trình cho tôi (Sếp) duyệt trước**, tôi đồng ý phương án nào thì cậu mới được bắt tay vào làm tiếp!

---

## 2. Quy Trình 4 Giai Đoạn Cậu Bắt Buộc Phải Thực Hiện

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ GIAI ĐOẠN 1: HOÀN THIỆN TOÀN BỘ CÁC TASK KỸ THUẬT & FIX ISSUES TỒN ĐỌNG        │
│ 1. Tinh giản job.py (2,580 -> ~200 dòng)    2. Sinh 3 dataset test tải/tăng dần │
│ 3. Sinh 3 dataset test chịu lỗi (Chaos)     4. Tích hợp Email SMTP Alerting     │
│ 5. Tối ưu Spark AQE & Superset/Grafana                                          │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ GIAI ĐOẠN 2: CHẠY BÀI KIỂM THỬ BENCHMARK & ĐO LƯỜNG TẢI HỆ THỐNG                │
│ - Chạy test tải với 3 bộ dataset (Off-peak, Flash-sale surge, Catchup)          │
│ - Đo lường: Throughput, độ trễ DAGs, mức RAM/CPU, số lượng file nén Parquet     │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ GIAI ĐOẠN 3: RÀ SOÁT, CHUẨN HÓA & CẬP NHẬT TOÀN BỘ HỆ THỐNG TÀI LIỆU (DOCS)    │
│ - Sửa các tài liệu cũ/demo trong docs/ (architecture, data_flow, runbook...)    │
│ - SỬA LỖI MERMAID CHART trong data_model (loại bỏ fact-to-fact, tách Marts)     │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ GIAI ĐOẠN 4: VIẾT BÁO CÁO NGHIỆM THU & ĐỀ XUẤT 5 - 10 Ý TƯỞNG (CHỜ SẾP DUYỆT)   │
│ - Lập file báo cáo Markdown chi tiết kết quả benchmark và bằng chứng kiểm thử   │
│ - Đề xuất 5 - 10 ý tưởng chiến lược siêu chi tiết -> ĐỆ TRÌNH SẾP DUYỆT         │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Chi Tiết Giai Đoạn 1: Danh Mục 5 Tasks Cần Làm Ngay

### Task 1: Tinh Giản Module Bảo Trì [`transformation/maintenance/job.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/transformation/maintenance/job.py)
* **Vấn đề:** File hiện tại dài 2,580 dòng, ôm đồm cơ chế tạo file `dry-run.json`, ký HMAC token bí mật và claim lease lock quá mức cồng kềnh. Đây là nguyên nhân vừa gây ra lỗi lệch pha Jinja template làm treo Airflow task `rewrite_data_files`.
* **Yêu cầu cậu làm:**
  * Viết lại một phiên bản tinh gọn chỉ khoảng **~200 - 300 dòng**.
  * Cắt bỏ toàn bộ logic HMAC verification, nonce validation và advisory locks (vì Airflow đã có Pool `lakehouse_writes` lo việc điều tiết tài nguyên).
  * Sử dụng trực tiếp 3 thủ tục chuẩn của Apache Iceberg:
    1. `CALL system.rewrite_data_files(table => '<target>', options => map('target-file-size-bytes','134217728'))`
    2. `CALL system.expire_snapshots(table => '<target>', older_than => TIMESTAMP '<threshold>')`
    3. `CALL system.remove_orphan_files(table => '<target>', older_than => TIMESTAMP '<threshold>')`
  * Ghi kết quả vào bảng audit `lakehouse.control.maintenance_results`.
  * Kiểm thử đảm bảo DAG `olist_maintenance` chạy 5/5 tasks xanh biếc.

### Task 2: Viết Script Sinh 3 Tập Dataset Test Tải & Xử Lý Tăng Dần
* **Vấn đề:** Cần kiểm thử khả năng xử lý tăng dần (`mode="incremental"`) và khả năng chịu tải của hệ thống trên các kịch bản lưu lượng khác nhau mà không dùng data ngẫu nhiên vô nghĩa.
* **Yêu cầu cậu làm:** Viết script Python (ví dụ: `scripts/generate_load_datasets.py`) đọc từ dữ liệu thật Olist trong `data/raw/` và sinh ra:
  * **Dataset 1 (Giờ thấp điểm / Micro-batch):** Trích ~500 đơn hàng, đổi timestamp sang ngày mới. Kiểm tra xem Spark xử lý lô nhỏ có bị overhead tệp nhỏ không và kiểm tra nạp gia tăng không làm trùng dữ liệu.
  * **Dataset 2 (Giờ săn sale đột biến / Flash-sale Surge):** Trích 30,000 - 50,000 đơn hàng, dồn toàn bộ timestamp vào cùng khung giờ vàng (12:00 - 14:00), nhân bản các sản phẩm hot. Dùng để test tải (stress test) xem Spark Worker có bị OOM không và Airflow pool `lakehouse_writes` có điều tiết ghi tốt không.
  * **Dataset 3 (Dồn tích lớn sau sự cố / Catch-up):** Toàn bộ dữ liệu 1 tháng với trạng thái cập nhật hỗn hợp (`created` $\rightarrow$ `delivered`). Kiểm tra cơ chế `MERGE INTO` (Upsert) của Iceberg.

### Task 3: Viết Script Sinh 3 Tập Dataset Test Chịu Lỗi (Chaos Testing)
* **Vấn đề:** Cần chứng minh tính mạnh mẽ của cơ chế kiểm soát chất lượng và Medallion Quarantine Pattern.
* **Yêu cầu cậu làm:** Viết script Python (ví dụ: `scripts/generate_chaos_datasets.py`) tiêm lỗi có chủ đích vào dữ liệu:
  * **Dataset 4 (Metadata bẩn & Schema Drift):** Thêm trường dữ liệu lạ, sai lệch định dạng ISO ngày tháng, đưa chuỗi chữ vào cột số tiền. Đảm bảo pipeline không crash.
  * **Dataset 5 (Vi phạm Khóa & Quarantine):** Null `order_id`, khóa ngoại mồ côi (order_item trỏ tới order không tồn tại), đơn giá âm. Đảm bảo toàn bộ bản ghi lỗi này bị đẩy vào bảng cách ly `lakehouse.quarantine.*`.
  * **Dataset 6 (Bản ghi trùng lặp & Xung đột trạng thái):** Cùng `order_id` nhưng khác trạng thái và timestamp. Đảm bảo tầng Silver deduplicate chỉ giữ lại bản ghi chuẩn xác nhất.

### Task 4: Tích Hợp Cảnh Báo Sự Cố Tự Động Qua Email (SMTP Alerting)
* **Vấn đề:** Khi có sự cố, hệ thống mới chỉ ghi log ra file NDJSON trong container, Admin không biết ngay để xử lý.
* **Yêu cầu cậu làm:**
  * Viết module `infrastructure/alerts/email_alert.py` ngắn gọn (< 60 dòng) dùng `smtplib` chuẩn.
  * Kết nối vào `failure_callback` trong [`common.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/orchestration/dags/common.py#L351-L355) để tự động bắn email khi Airflow task bị FAILED.
  * Bắn email khi Great Expectations phát hiện kiểm định mức `ERROR`.
  * Khai báo các tham số linh hoạt trong `.env` (`ALERT_SMTP_HOST`, `ALERT_ADMIN_EMAIL`...).

### Task 5: Tối Ưu Spark Jobs & Tinh Chỉnh Dashboard Superset/Grafana
* **Yêu cầu cậu làm:**
  * **Spark:** Thêm gợi ý `broadcast()` cho các bảng chiều nhỏ (`dim_product`, `dim_seller`) khi join với bảng Fact lớn trong [`gold/job.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/transformation/gold/job.py); kích hoạt cấu hình Adaptive Query Execution (AQE).
  * **Superset:** Thiết kế lại biểu đồ phễu kinh doanh (Funnel Analysis: Đặt hàng $\rightarrow$ Thanh toán $\rightarrow$ Giao hàng $\rightarrow$ Đánh giá); đồng bộ hóa bộ lọc toàn cục (Cross-filtering theo ngày, bang).
  * **Grafana:** Bổ sung panel "Tỷ lệ Dữ liệu Cách Ly" (Quarantine Rate %) đổi màu theo ngưỡng cảnh báo (> 5% đổi màu đỏ); bổ sung biểu đồ theo dõi xu hướng thời gian chạy của pipeline.

---

## 4. Chi Tiết Giai Đoạn 2 & 3: Kiểm Thử Benchmark & Rà Soát Sửa Tài Liệu

### 4.1. Quy Trình Kiểm Thử Benchmark (Performance Benchmark SOP)
Sau khi viết xong các script sinh dataset ở Task 2 & Task 3, cậu phải tiến hành chạy benchmark thực tế trên cụm 12 container và ghi nhận các thông số:
1. **Throughput (Dòng/Giây):** Tốc độ nạp và biến đổi dữ liệu trên từng tầng Bronze, Silver, Gold.
2. **Execution Latency:** Thời gian hoàn thành từng task trong DAG khi chạy bình thường vs. khi gặp đợt Flash-Sale tải nặng.
3. **Mức sử dụng tài nguyên:** RAM và CPU đỉnh (Peak memory/CPU) của Spark Worker và Trino.
4. **Hiệu quả nén file:** Số lượng file Parquet trước và sau khi chạy job maintenance nén file 128MB.

### 4.2. Rà Soát & Chuẩn Hóa Lại Toàn Bộ Tài Liệu trong `docs/`
Vì sau khi cậu hoàn thiện các task, cấu hình và mã nguồn thực tế sẽ thay đổi, nên cậu **phải cập nhật lại toàn bộ tài liệu cho chuẩn xác**:
* **LỖI NGHIÊM TRỌNG TRONG CHART MERMAID CẦN SỬA NGAY:**
  * Tôi vừa kiểm tra file [`rebuild/docs/data_model/star_schema.md`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/docs/data_model/star_schema.md) và thấy sơ đồ Mermaid ERD cũ bị **SAI BẢN CHẤT MÔ HÌNH KIMBALL**: Biểu đồ cũ vẽ Fact nối trực tiếp với Fact (`FACT_ORDERS ||--o{ FACT_ORDER_ITEMS`, `FACT_ORDERS ||--o{ FACT_PAYMENTS`), rồi lại vẽ Fact nối với Mart trong ERD!
  * **Quy chuẩn đúng:** Trong Star Schema, Fact KHÔNG BAO GIỜ nối với Fact. Chúng liên kết qua **Conformed Dimensions** (`DIM_DATE`, `DIM_CUSTOMER`) và **Degenerate Dimension** (`order_id`). Còn Marts là bảng tổng hợp (Materialized Summary), phải vẽ ở sơ đồ Data Lineage riêng chứ không được nhét vào ERD quan hệ khóa ngoại.
  * Tôi đã trực tiếp sửa lại mẫu chuẩn mực cho file `star_schema.md`. Nhiệm vụ của cậu là phải rà soát các file tài liệu còn lại:
    * `docs/architecture/architecture.md`
    * `docs/architecture/data_flow.md`
    * `docs/operations/runbook.md`
    * `docs/data_model/column_dictionary.md`
    Xem chỗ nào mô tả chưa đúng với code thực tế của 12 container Linux thì sửa lại ngay!

---

## 5. Chi Tiết Giai Đoạn 4: Viết Báo Cáo Nghiệm Thu & 5-10 Ý Tưởng Đệ Trình Sếp

Sau khi hoàn thành 100% Giai đoạn 1, 2 và 3, cậu **không được tự ý làm thêm bất kỳ tính năng nào khác**, mà phải lập một bộ hồ sơ báo cáo nghiệm thu gồm:

### 5.1. Báo Cáo Nghiệm Thu Hoàn Thành Công Việc
Lập file Markdown: `docs/reports/BAO_CAO_HOAN_THANH_TOAN_DIEN_LAKEHOUSE.md` gồm các mục:
1. **Tóm tắt kết quả đạt được:** Tinh giản job.py thành công bao nhiêu dòng, kết quả test 6 bộ dataset, kết quả gửi email cảnh báo thực tế.
2. **Bằng chứng đối soát doanh thu:** Câu lệnh Trino SQL chứng minh doanh thu vẫn giữ chuẩn `$16,008,872.12`.
3. **Bảng kết quả Benchmark hiệu năng:** Các chỉ số đo lường tải thực tế.
4. **Danh sách các tài liệu đã rà soát và sửa đổi:** Liệt kê các file trong `docs/` đã cập nhật.

### 5.2. Đề Xuất 5 Đến 10 Ý Tưởng Nâng Cấp Siêu Chi Tiết (Chờ Sếp Duyệt)
Cậu hãy đọc kỹ tài liệu nghiên cứu khung mà tôi đã chuẩn bị sẵn tại [**`rebuild/docs/Y_TUONG_NANG_CAP_DU_AN.md`**](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/docs/Y_TUONG_NANG_CAP_DU_AN.md). Từ đó, phát triển thành danh sách **5 đến 10 ý tưởng nâng cấp chiến lược siêu chi tiết**, mỗi ý tưởng phải trình bày rõ:
* *Tên ý tưởng & Mục tiêu kinh doanh.*
* *Kiến trúc kỹ thuật đề xuất (kèm sơ đồ ASCII / Mermaid).*
* *Kế hoạch triển khai công nghệ cụ thể.*
* *Phân tích Lợi ích (Pros) vs. Rủi ro / Chi phí (Cons).*
* *Ước tính thời gian và tài nguyên cần thiết.*

> [!CAUTION]
> **LỆNH TỪ SẾP:** Cậu chỉ dừng lại ở việc soạn thảo đề xuất ý tưởng trong báo cáo. Cậu **TUYỆT ĐỐI KHÔNG ĐƯỢC PHÉP TỰ Ý TRIỂN KHAI** bất kỳ ý tưởng nào trước khi tôi họp xem xét và ký duyệt phương án! Tập trung làm thật chuẩn chỉnh các task hiện tại cho tôi trước đã!

Chúc cậu hoàn thành xuất sắc nhiệm vụ! Có bất kỳ vướng mắc kiến trúc nào thì báo cáo lại cho tôi ngay.
