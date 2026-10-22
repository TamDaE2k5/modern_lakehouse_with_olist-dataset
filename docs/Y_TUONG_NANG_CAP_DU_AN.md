# Đề Án Chiến Lược & Ý Tưởng Nâng Cấp Hệ Thống (Strategic Roadmap)

> **Tài liệu nghiên cứu chuyên sâu phác thảo các định hướng kiến trúc, lộ trình mở rộng quy mô và các đề xuất nâng cấp chiến lược cho hệ sinh thái Olist Modern Data Lakehouse. Tài liệu được thiết kế chi tiết để Lãnh đạo dự án và Kỹ sư trưởng có đầy đủ luận cứ đưa ra quyết định đầu tư công nghệ.**

---

## 1. Tầm Nhìn Kiến Trúc Tổng Thể (Vision Statement)

Hệ sinh thái hiện tại đã hoàn thành xuất sắc vai trò của một **Modern Data Lakehouse chuẩn công nghiệp vận hành theo mô hình Batch (Medallion Architecture)** trên môi trường kiểm thử cục bộ. Tuy nhiên, để đáp ứng sự phát triển mạnh mẽ của hoạt động thương mại điện tử trong kỷ nguyên số, hệ thống cần tiến hóa từ một nền tảng lưu trữ/phân tích truyền thống sang một **Nền tảng Dữ liệu Doanh nghiệp Thông minh (Intelligent Enterprise Data Platform)**:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        TƯƠNG LAI: INTELLIGENT ENTERPRISE DATA PLATFORM                 │
│                                                                                        │
│   [Real-time Streaming]     [dbt Semantic Models]      [Data Governance & Lineage]     │
│   (Kafka + Spark Stream)    (Declarative SQL Marts)    (DataHub + PII Data Masking)    │
│            │                          │                             │                  │
│            ▼                          ▼                             ▼                  │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │                     LAKEHOUSE STORAGE: APACHE ICEBERG ACID                      │   │
│   │                 (Tiered Storage: Hot / Warm / Cold on Cloud S3)                │   │
│   └────────────────────────────────────────────────────────────────────────────────┘   │
│            │                          │                             │                  │
│            ▼                          ▼                             ▼                  │
│   [Trino Real-time BI]      [MLOps & Feature Store]    [FinOps & Auto Compaction]      │
│   (Sub-second Queries)      (Feast + Churn/CLV Models) (Z-Order Clustering Optimizer)  │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

Dưới đây là **6 ý tưởng nâng cấp chiến lược** được phân tích chi tiết về bối cảnh, kiến trúc, kế hoạch thực thi, ưu/nhược điểm và ma trận ra quyết định.

---

## 2. Chi Tiết 6 Ý Tưởng Nâng Cấp Chiến Lược

---

### Ý TƯỞNG 1: Chuyển Dịch Sang Kiến Trúc Real-Time Streaming Lakehouse
*(Real-time Event Streaming với Apache Kafka & Spark Structured Streaming / Apache Flink)*

#### 1. Bối cảnh & Điểm nghẽn hiện tại
Hệ thống hiện tại xử lý theo lô định kỳ (Batch T+1 hoặc chạy theo lịch vài giờ một lần). Trong môi trường kinh doanh thương mại điện tử thực tế:
* Cảnh báo gian lận thẻ thanh toán (`order_payments`) nếu chờ tới ngày hôm sau mới phát hiện thì kẻ gian đã nhận hàng và tẩu tán.
* Theo dõi doanh thu và lưu lượng tồn kho trong các chiến dịch Siêu Sale (Flash-Sale) đòi hỏi số liệu cập nhật từng giây (Sub-minute Latency) để đội ngũ Vận hành kịp thời can thiệp tiếp thị hoặc điều phối kho.

#### 2. Kiến trúc Giải pháp Đề xuất
Xây dựng kiến trúc **Kappa Architecture**, hợp nhất luồng xử lý thời gian thực vào thẳng bảng Apache Iceberg:

```
Máy chủ Ứng dụng Web ──► Apache Kafka ──► Spark Structured Streaming ──► Apache Iceberg
(Orders / Payments)     (Event Topics)    (Watermark + Micro-batch)       (Bronze/Silver Table)
                                                                                  │
                                                                                  ▼
                                                                        Trino Query Engine
                                                                        (Near Real-time BI)
```

#### 3. Kế hoạch Triển khai Kỹ thuật
* **Lớp Event Ingestion:**
  * Bổ sung container `kafka` và `zookeeper` (hoặc Kafka KRaft) vào Compose.
  * Định nghĩa các Topics: `olist.events.orders`, `olist.events.payments`, `olist.events.reviews`.
* **Lớp Stream Processing:**
  * Xây dựng PySpark Streaming Job: Đọc từ Kafka, parse định dạng JSON/Avro, lọc dữ liệu bẩn theo thời gian thực.
  * Tận dụng tính năng **Streaming Sink của Apache Iceberg**: Iceberg hỗ trợ ghi append và upsert liên tục với đảm bảo giao dịch ACID, tự động tạo snapshot nhỏ sau mỗi micro-batch (10 - 30 giây).
  * Cấu hình **Watermarking** để xử lý các sự kiện đến trễ (Late-arriving events) lên tới 1 giờ.
* **Cơ chế nén file tự động (Compaction on Streaming):**
  * Do streaming liên tục tạo ra nhiều file Parquet nhỏ, bố trí một Airflow DAG phụ trợ chạy định kỳ 15 phút gọi `rewrite_data_files` để gộp file mà không gây xung đột với luồng ghi (nhờ cơ chế Optimistic Concurrency Control của Iceberg).

#### 4. Đánh giá Lợi ích & Thách thức
* **Lợi ích:** Độ trễ dữ liệu giảm từ 24 giờ xuống chỉ còn 15 - 30 giây; cho phép phát hiện gian lận và cảnh báo tồn kho tức thì.
* **Thách thức:** Tốn tài nguyên RAM/CPU hơn do Spark Streaming phải chạy liên tục (daemon process); cần quản lý chặt chẽ kích thước metadata của Iceberg.

---

### Ý TƯỞNG 2: Chuẩn Hóa Tầng Biến Đổi Dữ Liệu Với dbt-core
*(Declarative Modeling & Data Lineage với dbt-trino)*

#### 1. Bối cảnh & Điểm nghẽn hiện tại
Hiện tại, toàn bộ logic biến đổi tầng Gold và xây dựng Data Marts nằm trong file Python [`transformation/gold/job.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/transformation/gold/job.py). Nhược điểm:
* Chỉ có Kỹ sư Dữ liệu (Data Engineers) biết viết Spark code mới tham gia phát triển được. Các Chuyên viên Phân tích Dữ liệu (Data Analysts / Analytics Engineers) thành thạo SQL bị cô lập.
* Thiếu tài liệu tự động hóa về nguồn gốc dữ liệu (Data Lineage) và kiểm thử hợp đồng dữ liệu chuẩn SQL (Schema tests).

#### 2. Kiến trúc Giải pháp Đề xuất
Tách biệt rõ ràng: **Spark phụ trách Heavy Ingestion (Bronze $\rightarrow$ Silver)**; **dbt + Trino phụ trách Tầng Phân tích & Mô hình hóa (Silver $\rightarrow$ Gold)**:

```
┌──────────────────────────────────────┐      ┌──────────────────────────────────────────────┐
│           APACHE SPARK               │      │                  DBT-TRINO                   │
│  CSV ──► Bronze ──► Silver + Quaran  │ ──►  │  Silver ──► Dim / Fact ──► Business Marts    │
│  (Heavy parsing, cleaning, scaling)  │      │  (Declarative SQL, Automated Tests, Lineage) │
└──────────────────────────────────────┘      └──────────────────────────────────────────────┘
```

#### 3. Kế hoạch Triển khai Kỹ thuật
* Tích hợp gói `dbt-trino` vào môi trường điều phối.
* Chuyển đổi 14 bảng Gold thành các file `.sql` chuẩn dbt models:
  * `models/gold/dimensions/dim_customer.sql`
  * `models/gold/facts/fact_orders.sql`
  * `models/gold/marts/mart_daily_revenue.sql`
* Tận dụng các tính năng cao cấp của dbt:
  * **Generic Tests:** Tự động kiểm tra `unique`, `not_null`, `relationships` khóa ngoại ngay trong file `schema.yml`.
  * **Documentation & Lineage Graph:** Chạy lệnh `dbt docs generate` để tự động sinh website tương tác trực quan hóa cây phả hệ dữ liệu từ bảng nguồn đến bảng đích.
  * **Incremental Models:** dbt tự động sinh mã SQL `MERGE INTO` để nạp dữ liệu tăng dần mà không cần viết lệnh thủ công.

#### 4. Đánh giá Lợi ích & Thách thức
* **Lợi ích:** Dễ mở rộng, chuẩn hóa quy trình phân tích dữ liệu theo tiêu chuẩn quốc tế; cho phép đội ngũ phân tích kinh doanh tự xây dựng báo cáo; tự động sinh tài liệu 100%.
* **Thách thức:** Cần cấu hình tài nguyên cho Trino cluster đủ mạnh để gánh vác khối lượng tính toán ETL của tầng Gold.

---

### Ý TƯỞNG 3: Xây Dựng Nền Tảng Quản Trị Dữ Liệu Tập Trung (Data Governance)
*(Metadata Management, Data Catalog & Data Masking với DataHub hoặc Apache Atlas)*

#### 1. Bối cảnh & Điểm nghẽn hiện tại
Hệ thống hiện quản lý 31 bảng dữ liệu phân tầng nhưng chưa có cơ chế quản trị tập trung:
* Chưa có giao diện tìm kiếm tập trung (Data Discovery) cho người dùng nội bộ biết trong Lakehouse đang có những bảng gì, cột nào, ý nghĩa kinh doanh là gì.
* Dữ liệu nhạy cảm (PII - Personally Identifiable Information) như tọa độ địa lý, mã bưu điện, mã khách hàng chưa được gắn nhãn (Tagging) và kiểm soát quyền truy cập chi tiết.

#### 2. Kiến trúc Giải pháp Đề xuất
Tích hợp **DataHub** (nền tảng Data Catalog mã nguồn mở hiện đại do LinkedIn phát triển) để tự động thu thập metadata từ toàn bộ hệ sinh thái:

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│   Iceberg   │   │    Trino    │   │   Airflow   │
│   Catalog   │   │ Query Logs  │   │  DAG Runs   │
└──────┬──────┘   └──────┬──────┘   └──────┬──────┘
       │                 │                 │
       └───────────┬─────┴─────────────────┘
                   ▼
         DataHub Ingestion Pipeline
                   │
                   ▼
       ┌───────────────────────┐
       │     DATAHUB UI        │
       │ - Search & Discovery  │
       │ - End-to-End Lineage  │
       │ - PII Data Governance │
       └───────────────────────┘
```

#### 3. Kế hoạch Triển khai Kỹ thuật
* Cấu hình DataHub CLI ingestion định kỳ:
  * Quét metadata từ PostgreSQL Iceberg Catalog.
  * Quét lịch sử chạy và task lineage từ Airflow.
  * Phân tích query logs từ Trino để vẽ Column-level Lineage (Cột A của bảng Mart được tính từ Cột B của Fact và Cột C của Dimension nào).
* Triển khai chính sách **Phân loại & Che mờ Dữ liệu (Data Masking / Anonymization)**:
  * Gắn nhãn `PII.Confidential` cho các cột vị trí địa lý và định danh khách hàng.
  * Cấu hình phân quyền Trino System Access Control: Chỉ nhóm Quản trị viên mới xem được dữ liệu thật; nhóm Thực tập sinh hoặc Phân tích kinh doanh chỉ nhìn thấy chuỗi băm (Hashed/Masked).

#### 4. Đánh giá Lợi ích & Thách thức
* **Lợi ích:** Tuân thủ các quy định bảo vệ dữ liệu người dùng (GDPR / LGPD Brazil); tăng năng suất làm việc của đội ngũ Data Analytics; phát hiện sớm các thay đổi schema bất thường.
* **Thách thức:** Cần duy trì thêm dịch vụ DataHub (yêu cầu Elasticsearch/OpenSearch và Neo4j/MySQL), đòi hỏi bổ sung ít nhất 2 - 4 GiB RAM trên máy chủ.

---

### Ý TƯỞNG 4: Tích Hợp Nền Tảng Học Máy & Trí Tuệ Nhân Tạo (AI / MLOps)
*(Feature Store với Feast & Dự Đoán Kinh Doanh với MLflow)*

#### 1. Bối cảnh & Giá trị Kinh doanh
Dữ liệu thương mại điện tử Olist là "mỏ vàng" để phát triển các mô hình Trí tuệ Nhân tạo phục vụ trực tiếp cho tăng trưởng doanh thu:
1. **Mô hình Dự đoán Khách hàng Rời Bỏ (Customer Churn Prediction):** Xác định những khách hàng có nguy cơ không quay lại mua sắm sau 30/60 ngày để gửi mã giảm giá kích cầu.
2. **Hệ Thống Gợi Ý Sản Phẩm (Cross-sell / Recommendation Engine):** Dựa trên bảng cầu nối `bridge_review_products` và lịch sử mua sắm để gợi ý sản phẩm phù hợp khi khách đang lướt giỏ hàng.
3. **Mô hình Dự Báo Độ Trễ Vận Chuyển (Delivery Delay Prediction):** Dựa trên thông tin bang của người bán, bang của người mua và điều kiện địa lý để cảnh báo thời gian giao hàng dự kiến chính xác hơn cam kết của bưu điện.

#### 2. Kiến trúc Giải pháp Đề xuất
Tích hợp **Feature Store (Feast)** và **Vòng đời Mô hình (MLflow)** lên trên nền tảng tầng Gold:

```
┌─────────────────────────────────┐
│     LAKEHOUSE GOLD MARTS        │
│ (mart_seller, mart_customer)    │
└────────────────┬────────────────┘
                 │
                 ▼
     ┌───────────────────────┐
     │  FEAST FEATURE STORE  │ ── Offline Store: Trino/Iceberg (Training Data)
     │  (Thư viện Đặc trưng) │ ── Online Store: Redis Cache (Real-time Inference)
     └───────────┬───────────┘
                 │
                 ▼
     ┌───────────────────────┐
     │    MLFLOW TRACKING    │
     │  (XGBoost / LightGBM) │
     └───────────┬───────────┘
                 │
                 ▼
     FastAPI Serving Service (REST API trả về kết quả dự đoán thời gian thực)
```

#### 3. Kế hoạch Triển khai Kỹ thuật
* **Xây dựng Feature Views:**
  * Đặc trưng khách hàng: Tần suất mua hàng (Recency, Frequency, Monetary - RFM scores), số tiền trung bình mỗi đơn, điểm đánh giá trung bình.
  * Đặc trưng nhà bán hàng: Tỷ lệ giao hàng trễ, số lượng danh mục sản phẩm kinh doanh.
* **Huấn luyện & Đóng gói:**
  * Sử dụng thư viện `scikit-learn` và `xgboost` để huấn luyện mô hình.
  * Đăng ký phiên bản mô hình (Model Versioning) lên MLflow Model Registry.
  * Đóng gói dịch vụ dự đoán qua API FastAPI (ví dụ: `POST /predict-churn`, `GET /recommend-products/{customer_id}`).

#### 4. Đánh giá Lợi ích & Thách thức
* **Lợi ích:** Tạo ra giá trị kinh tế trực tiếp cho doanh nghiệp từ dữ liệu có sẵn; nâng tầm dự án từ kỹ thuật thuần túy sang ứng dụng AI thực chiến.
* **Thách thức:** Đòi hỏi kiến thức chuyên sâu về Machine Learning và kỹ năng tối ưu hóa độ trễ API.

---

### Ý TƯỞNG 5: Bản Thiết Kế Di Chuyển Lên Đám Mây Chuẩn Cloud-Native
*(Cloud Migration Blueprint: AWS / GCP Serverless Architecture)*

#### 1. Bối cảnh & Lý do Cần Di Chuyển
Môi trường Docker Compose cục bộ rất lý tưởng cho giai đoạn nghiên cứu, phát triển và chứng minh tính khả thi (PoC). Tuy nhiên, khi doanh nghiệp mở rộng quy mô dữ liệu từ Gigabytes lên hàng chục Terabytes với hàng ngàn người dùng truy vấn đồng thời, việc tự duy trì hạ tầng máy chủ cục bộ sẽ phát sinh chi phí bảo trì phần cứng và rủi ro gián đoạn dịch vụ.

#### 2. Bảng Đối Chiếu Dịch Vụ Tương Đương Lên Đám Mây (Mapping Blueprint)

| Thành phần Cục bộ (Local Rebuild) | Tương đương trên AWS Cloud | Tương đương trên Google Cloud (GCP) | Ưu điểm khi lên Cloud |
|:---|:---|:---|:---|
| **MinIO Object Storage** | **Amazon S3** (Simple Storage Service) | **Google Cloud Storage (GCS)** | Độ bền dữ liệu 99.999999999% (11 số 9), dung lượng vô hạn |
| **Postgres Iceberg Catalog** | **AWS Glue Data Catalog** | **GCP Dataproc Metastore** | Serverless Catalog, quản lý metadata tự động, tích hợp sẵn |
| **Apache Spark Master/Worker**| **Amazon EMR Serverless** | **GCP Dataproc Serverless** | Tự động co giãn theo tải (Auto-scaling), chỉ trả tiền khi chạy |
| **Trino Query Engine** | **Amazon Athena** (dựa trên Trino) | **BigQuery Omni / Starburst Galaxy** | Truy vấn SQL Serverless, không cần quản lý máy chủ |
| **Apache Airflow** | **Amazon MWAA** (Managed Airflow) | **GCP Cloud Composer** | Airflow được nhà mạng vận hành, tự động vá lỗi bảo mật |
| **Apache Superset** | **Amazon QuickSight / Preset.io** | **Google Looker / Looker Studio** | BI đám mây chia sẻ báo cáo dễ dàng, tính năng bảo mật cao |
| **Prometheus & Grafana** | **Amazon Managed Prometheus / Grafana** | **Google Cloud Monitoring** | Giám sát không cần duy trì volume lưu trữ cục bộ |

#### 3. Lộ Trình Di Chuyển 4 Giai Đoạn (Phased Migration Plan)
1. **Giai đoạn 1: Dual-Storage (Đồng bộ Dữ liệu):** Tạo bucket trên AWS S3 / GCP GCS, cấu hình sao chép nền dữ liệu bảng Iceberg từ MinIO lên Cloud.
2. **Giai đoạn 2: Shadow Pipeline (Chạy Song Song):** Triển khai Airflow DAGs trên Cloud để nạp và biến đổi dữ liệu song song với cụm cục bộ, đối soát kết quả doanh thu đảm bảo khớp 100%.
3. **Giai đoạn 3: Cutover (Chuyển Giao Phân Tích):** Trỏ Trino (Athena) và Dashboards BI sang truy vấn trực tiếp trên S3/GCS. Chuyển toàn bộ người dùng kinh doanh sang nền tảng mới.
4. **Giai đoạn 4: Decommission (Dừng Hạ Tầng Cũ):** Tắt cụm máy chủ cục bộ, lưu trữ bản sao lưu cuối cùng (Cold archive) trên S3 Glacier.

#### 4. Đánh giá Lợi ích & Chi phí
* **Lợi ích:** Vận hành phi máy chủ (Serverless), độ sẵn sàng 99.99%, khả năng mở rộng tức thì không giới hạn.
* **Chi phí:** Phát sinh chi phí thuê bao đám mây định kỳ; cần quản lý FinOps chặt chẽ để tránh vượt ngân sách.

---

### Ý TƯỞNG 6: Tối Ưu Hóa Chi Phí & Vận Hành Bền Vững (FinOps & Storage Governance)
*(Z-Order Clustering, Partition Layout Optimization & Storage Tiering)*

#### 1. Bối cảnh & Giá trị
Trong một Data Lakehouse vận hành lâu dài, 80% chi phí lưu trữ và tính toán đến từ việc quét dữ liệu thừa (Unnecessary I/O scanning) và lưu trữ dữ liệu lịch sử ít dùng ở các vùng lưu trữ đắt đỏ.

#### 2. Kỹ Thuật Tối Ưu Hóa Bố Trí Dữ Liệu (Data Layout Optimization)
* **Z-Order Clustering trên Apache Iceberg:**
  * Hiện tại bảng `fact_orders` mới chỉ được phân vùng theo tháng/ngày. Khi người dùng lọc theo `customer_state` (Bang) hoặc `order_status` (Trạng thái đơn), Spark và Trino vẫn phải quét nhiều tệp Parquet.
  * Áp dụng kỹ thuật Z-Order nén đa chiều:
    ```sql
    CALL system.rewrite_data_files(
      table => 'lakehouse.gold.fact_orders',
      strategy => 'sort',
      sort_order => 'zorder(customer_state, order_purchase_date)'
    );
    ```
  * **Hiệu quả:** Thu hẹp phạm vi quét tệp (File Pruning) lên tới **60 - 80%**, giúp các câu lệnh truy vấn phân tích trên Trino phản hồi dưới 1 giây.
* **Chiến Lược Phân Tầng Lưu Trữ (Storage Lifecycle Tiering):**
  * Dữ liệu trong vòng 90 ngày gần nhất (Hot Data): Giữ trên MinIO/S3 Standard với tần suất truy cập cao.
  * Dữ liệu từ 90 ngày đến 1 năm (Warm Data): Chuyển xuống vùng lưu trữ chi phí thấp (S3 Infrequent Access).
  * Dữ liệu trên 1 năm (Cold Archive): Nén gzip tối đa và chuyển xuống S3 Glacier, giúp tiết kiệm **75% chi phí lưu trữ đĩa**.

---

## 3. Ma Trận Đánh Giá & Lộ Trình Lựa Chọn Đầu Tư (Decision Matrix)

Để giúp Lãnh đạo dự án dễ dàng lựa chọn định hướng phát triển phù hợp nhất với nguồn lực và ngân sách hiện tại, dưới đây là bảng ma trận chấm điểm tổng hợp:

| Hạng mục Nâng cấp | Độ Phức Tạp Kỹ thuật | Chi Phí Triển Khai | Giá Trị Mang Lại cho Kinh Doanh | Thời Gian Ước Tính | Đề Xuất Ưu Tiên |
|:---|:---:|:---:|:---:|:---:|:---:|
| **Ý tưởng 1: Real-time Streaming** | Cao (3/5) | Trung bình | Rất lớn (Phát hiện gian lận, Flash-sale) | 3 - 4 tuần | ⭐⭐⭐⭐ (Ưu tiên số 2) |
| **Ý tưởng 2: Chuẩn hóa dbt-core** | Trung bình (2/5) | Thấp (Mã nguồn mở) | Rất lớn (Chuẩn hóa modeling, tự sinh Lineage)| 1 - 2 tuần | ⭐⭐⭐⭐⭐ (Khuyên làm ngay) |
| **Ý tưởng 3: Quản trị DataHub** | Trung bình (2/5) | Thấp | Lớn (Bảo mật PII, Catalog tập trung) | 2 tuần | ⭐⭐⭐ |
| **Ý tưởng 4: AI/ML & Feature Store**| Cao (4/5) | Trung bình | Cực kỳ lớn (Dự đoán Churn, Gợi ý hàng) | 4 - 6 tuần | ⭐⭐⭐⭐ (Ưu tiên số 3) |
| **Ý tưởng 5: Lên Cloud Native** | Rất cao (4/5) | Cao (Chi phí Cloud)| Lớn (Mở rộng vô hạn, Serverless) | 4 - 8 tuần | ⭐⭐⭐ (Khi cần scale lớn) |
| **Ý tưởng 6: FinOps & Z-Order** | Thấp (1/5) | Rất thấp | Cao (Tăng tốc truy vấn gấp 3 lần, tiết kiệm đĩa)| 3 - 5 ngày | ⭐⭐⭐⭐⭐ (Khuyên làm ngay) |

---

## 4. Khuyến Nghị Hành Động Tiếp Theo Dành Cho Chủ Sở Hữu Dự Án

Nếu bạn muốn tiếp tục nâng cấp dự án này lên một tầm cao mới, khuyến nghị lộ trình 2 chặng thực tế nhất như sau:

1. **Giai đoạn Ngắn hạn (Quick Wins - Hoàn thành trong 1 - 2 tuần):**
   * Triển khai **Ý tưởng 6 (Z-Order Clustering)** và **Ý tưởng 2 (dbt-core)**. Đây là hai hạng mục chi phí thấp nhất, không làm phát sinh thêm hạ tầng nặng nề, nhưng ngay lập tức mang lại cấu trúc code chuẩn mực quốc tế và tăng tốc độ truy vấn Trino lên gấp 3 lần.
2. **Giai đoạn Dài hạn (Strategic Expansion - Triển khai trong 1 - 2 tháng):**
   * Nếu bài toán trọng tâm là **Tăng trưởng Doanh thu**: Chọn **Ý tưởng 4 (Hệ thống AI/ML Churn & Recommendation Engine)**.
   * Nếu bài toán trọng tâm là **Vận hành Thời gian Thực**: Chọn **Ý tưởng 1 (Real-time Streaming với Kafka & Iceberg)**.
   * Nếu cần đưa dự án vào **Sản xuất Quy mô Lớn (Enterprise Scale)**: Triển khai **Ý tưởng 5 (Cloud Migration lên AWS/GCP)**.
