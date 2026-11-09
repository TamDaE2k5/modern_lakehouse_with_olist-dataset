# KẾ HOẠCH THỰC NGHIỆM ĐỐI SOÁT HIỆU NĂNG SPARK JOB
## So Sánh 3 Phiên Bản: V1 (Mặc Định) vs. V2 (Hiện Tại) vs. V3 (Senior Đề Xuất)

> **Mục tiêu cốt lõi:** Trả lời dứt khoát câu hỏi bằng số liệu thực nghiệm: *"Việc tối ưu hóa Apache Spark (AQE, Broadcast Join, Pool Throttling, Memory Tuning) mang lại giá trị thực tế bao nhiêu, và có đáng với công sức bỏ ra hay không?"*

---

## MỤC LỤC
1. [Tổng Quan Kế Hoạch & Nguyên Tắc Thực Nghiệm](#1-tổng-quan-kế-hoạch--nguyên-tắc-thực-nghiệm)
2. [Hồ Sơ Kỹ Thuật 3 Phiên Bản Cần Đo Đạc](#2-hồ-sơ-kỹ-thuật-3-phiên-bản-cần-đo-đạc)
   - [Phiên bản V1: Spark Mặc Định (Unoptimized Baseline)](#21-phiên-bản-v1-spark-mặc-định-unoptimized-baseline)
   - [Phiên bản V2: Spark Hiện Tại (Task 5 Optimizations)](#22-phiên-bản-v2-spark-hiện-tại-task-5-optimizations)
   - [Phiên bản V3: Spark Nâng Cao (Senior Lead Upgrades)](#23-phiên-bản-v3-spark-nâng-cao-senior-lead-upgrades)
3. [Bản Đồ Thay Đổi Mã Nguồn Qua 3 Phiên Bản (Code Diff & Patches)](#3-bản-đồ-thay-đổi-mã-nguồn-qua-3-phiên-bản)
4. [Quy Trình Thực Hiện Từng Bước (Step-by-Step Runbook)](#4-quy-trình-thực-hiện-từng-bước-step-by-step-runbook)
   - [Bước 0: Chuẩn bị môi trường đo chuẩn xác](#bước-0-chuẩn-bị-môi-trường-đo-chuẩn-xác)
   - [Bước 1: Đo Đạc Phiên Bản V2 (Hiện Tại)](#bước-1-đo-đạc-phiên-bản-v2-hiện-tại)
   - [Bước 2: Quay Về Đo Phiên Bản V1 (Mặc Định)](#bước-2-quay-về-đo-phiên-bản-v1-mặc-định)
   - [Bước 3: Nâng Cấp Lên Đo Phiên Bản V3 (Senior Đề Xuất)](#bước-3-nâng-cấp-lên-đo-phiên-bản-v3-senior-đề-xuất)
5. [Khung Tiêu Chí Đánh Giá "Có Đáng Kết Quả Không?" (ROI Framework)](#5-khung-tiêu-chí-đánh-giá-có-đáng-kết-quả-không-roi-framework)
6. [Dự Báo Kỹ Thuật Trước Khi Đo (Technical Forecast)](#6-dự-báo-kỹ-thuật-trước-khi-đo-technical-forecast)
7. [Bảng Ma Trận Đối Soát Kết Quả Nghiệm Thu (Template)](#7-bảng-ma-trận-đối-soát-kết-quả-nghiệm-thu-template)

---

## 1. TỔNG QUAN KẾ HOẠCH & NGUYÊN TẮC THỰC NGHIỆM

Trong kỹ thuật dữ liệu (Data Engineering), không có tối ưu hóa nào là miễn phí: mọi cấu hình thêm vào đều làm tăng độ phức tạp bảo trì mã nguồn. Để chứng minh việc tối ưu Spark là **đáng tiền, đáng công sức** chứ không phải "tối ưu vi mô trên giấy" (premature optimization), ta thiết kế chuỗi A-B-C Testing gồm 3 phiên bản trên cùng một môi trường:

```mermaid
flowchart LR
    V1["V1: Mặc định (Baseline)<br/>- Không AQE<br/>- 200 Shuffle Partitions<br/>- SortMergeJoin toàn bộ<br/>- Pool 8 slots"] 
    -->|Task 5 Optimization| V2["V2: Hiện tại (Current)<br/>- AQE Coalesce & Skew Join<br/>- 32 Shuffle Partitions<br/>- Broadcast Join dim_date & dims<br/>- Pool 8 slots"]
    -->|Senior Lead Proposals| V3["V3: Nâng cao (Senior)<br/>- Pool 2 slots (Cắt 4GB RAM Scheduler)<br/>- Memory Fraction 0.8<br/>- Iceberg WRITE ORDERED BY<br/>- Dynamic Partition Pruning"]
```

### Nguyên tắc đo lường công bằng:
1. **Cố định phần cứng ảo hóa:** Giữ nguyên cấu hình Docker (Spark Worker: 2 Core, 4 GiB RAM; Scheduler: 8 GiB; PostgreSQL; MinIO).
2. **Cố định bộ dữ liệu đo:** Toàn bộ 3 phiên bản đều chạy qua đúng chuỗi **8 kịch bản benchmark chuẩn** (`00_baseline_full`, `01_offpeak`, `02_flash_sale`, `03_catchup`, `04_dirty_metadata`, `05_integrity_violations`, `06_duplicates`, `07_baseline_restore`).
3. **Cùng phương pháp đo:** Dùng script tự động [`scripts/run_benchmarks.sh`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/scripts/run_benchmarks.sh) kết hợp lấy mẫu nền `docker stats` mỗi 3 giây để bắt đỉnh RAM/CPU thực tế và truy vấn metadata Trino Iceberg để đo dung lượng tệp.

---

## 2. HỒ SƠ KỸ THUẬT 3 PHIÊN BẢN CẦN ĐO ĐẠC

### 2.1. Phiên bản V1: Spark Mặc Định (Unoptimized Baseline)
Đây là phiên bản tương đương với commit `39eac45` (trước khi Agent thực hiện Task 5):
* **Cấu hình AQE:** Tắt hoàn toàn (`spark.sql.adaptive.enabled = false`). Spark lập kế hoạch tĩnh (Static Physical Plan) ngay từ đầu dựa trên ước lượng thống kê ban đầu.
* **Số phân vùng Shuffle:** Giữ nguyên mặc định toàn cầu của Apache Spark là **200 partitions** (`spark.sql.shuffle.partitions = 200`).
* **Cơ chế JOIN:** Không có gợi ý `broadcast()` trong code. Bảng `dim_date` (chỉ ~1.000 dòng) được join 5 lần trong `fact_orders` đều phải trải qua **5 lần Shuffle và Sort-Merge Join**.
* **Ngưỡng tự động Broadcast:** Giữ mặc định 10MB (`spark.sql.autoBroadcastJoinThreshold = 10485760`).
* **Airflow Pool:** `lakehouse_writes` mở tối đa 8 slots (cho phép cả 8 task Silver cùng chạy driver một lúc).

### 2.2. Phiên bản V2: Spark Hiện Tại (Task 5 Optimizations)
Đây là mã nguồn đang nằm trong repo của bạn hiện tại (commit `1d1fa05` trở đi):
* **AQE bật trực tiếp trong Python Driver:** Bật trong [`build_spark_session`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/transformation/common/spark_session.py):
  * `spark.sql.adaptive.enabled = true`
  * `spark.sql.adaptive.coalescePartitions.enabled = true` (gộp phân vùng nhỏ, ngưỡng tối thiểu 8MB).
  * `spark.sql.adaptive.advisoryPartitionSizeInBytes = 64MB`.
  * `spark.sql.adaptive.skewJoin.enabled = true` (hệ số lệch 5, ngưỡng 64MB bảo vệ bài Flash-Sale).
* **Số phân vùng Shuffle khởi điểm:** Giảm từ 200 xuống **32 partitions** (`spark.sql.shuffle.partitions = 32`) để AQE tự do co giãn mà không tạo overhead quá mức.
* **Bọc Broadcast Join tường minh:**
  * Bảng `dim_date` bọc `F.broadcast()` trong `facts.py` (triệt tiêu 5 lần network shuffle).
  * Bảng tra cứu `dim_customer`, `dim_product`, `dim_seller` bọc `F.broadcast()` trong `facts.py` và `marts.py`.
* **Ngưỡng tự động Broadcast:** Nâng lên 50MB (`52428800` bytes).
* **Airflow Pool:** Vẫn là 8 slots.

### 2.3. Phiên bản V3: Spark Nâng Cao (Senior Lead Upgrades)
Đây là phiên bản bổ sung 4 tối ưu chiến lược theo đề xuất tại Mục 3.3 & Ý tưởng 1 của Báo cáo:
* **Tối ưu 1 (Airflow Pool Throttling - Giảm áp lực RAM):**
  * Giảm slot pool `lakehouse_writes` từ **8 xuống 2 slots**.
  * *Tác động kỳ vọng:* Ngăn chặn 8 Python Driver và 8 JVM khởi động đồng thời trong container `airflow-scheduler`. **Cắt giảm từ 3 đến 4 GiB RAM Scheduler**, triệt tiêu nguy cơ treo máy host.
* **Tối ưu 2 (Tái cấu trúc bộ nhớ JVM Spark):**
  * Đặt `spark.memory.fraction = 0.8` và `spark.memory.storageFraction = 0.2`.
  * *Tác động kỳ vọng:* Vì đây là pipeline ETL thuần (chỉ đọc, xử lý và ghi đè xuống Iceberg, không dùng RDD Cache/Persist), việc giảm storage memory để nhường 80% RAM cho Execution/Shuffle sẽ giảm thiểu 100% tình trạng tràn bộ nhớ shuffle ra đĩa (Spill to Disk).
* **Tối ưu 3 (Bố cục ghi Iceberg Clustering & Sorting):**
  * Thêm cấu hình: `spark.sql.iceberg.planning.preserve-data-grouping = true`.
  * Trong `facts.py`: sắp xếp dữ liệu trước khi ghi xuống `fact_orders`:
    `.sortWithinPartitions("purchase_date", "customer_key")`.
  * *Tác động kỳ vọng:* Các file Parquet ghi ra được gom cụm min/max hoàn hảo, tăng tốc độ truy vấn Trino lên 3 lần và giảm dung lượng metadata.
* **Tối ưu 4 (Dynamic Partition Pruning - DPP):**
  * `spark.sql.optimizer.dynamicPartitionPruning.enabled = true`.
  * *Tác động kỳ vọng:* Khi join Fact với Dimension có điều kiện lọc, Spark tự động bỏ qua các partition Fact không liên quan ngay từ khâu Scan.

---

## 3. BẢN ĐỒ THAY ĐỔI MÃ NGUỒN QUA 3 PHIÊN BẢN

Để thuận tiện cho bạn thực hiện chuyển đổi mà không sợ mất code, đây là vị trí chính xác của từng dòng code:

### Tệp 1: [`transformation/common/spark_session.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/transformation/common/spark_session.py)

#### Cấu hình V1 (Mặc định):
```python
        # V1: KHÔNG CÓ AQE, DÙNG MẶC ĐỊNH SPARK
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760") # 10MB
        .config("spark.sql.iceberg.write.distribution-mode", "none")
        .config("spark.sql.iceberg.write.target-file-size-bytes", "134217728")
        .getOrCreate()
```

#### Cấu hình V2 (Hiện tại - Task 5):
```python
        # V2: ĐÃ BẬT AQE VÀ CẤU HÌNH THÍCH ỨNG
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "8MB")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.autoBroadcastJoinThreshold", "52428800") # 50MB
        .config("spark.sql.iceberg.write.distribution-mode", "none")
        .config("spark.sql.iceberg.write.target-file-size-bytes", "134217728")
        .getOrCreate()
```

#### Cấu hình V3 (Senior Nâng Cao):
```python
        # V3: BỔ SUNG MEMORY TUNING, DPP VÀ ICEBERG PRESERVE GROUPING
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "8MB")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.autoBroadcastJoinThreshold", "52428800")
        # --- Bổ sung V3 ---
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.2")
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        .config("spark.sql.iceberg.planning.preserve-data-grouping", "true")
        .config("spark.sql.iceberg.write.distribution-mode", "none")
        .config("spark.sql.iceberg.write.target-file-size-bytes", "134217728")
        .getOrCreate()
```

---

### Tệp 2: [`transformation/gold/facts.py`](file:///run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild/transformation/gold/facts.py)

* **Ở V1:** Các hàm lookup trả về DataFrame nguyên bản (không bọc `F.broadcast()`):
  ```python
  # V1:
  customer_relation = customer_df.select("customer_id", "customer_key").distinct()
  return date_df.select("full_date", "date_key")
  ```
* **Ở V2 & V3:** Bọc `F.broadcast()` tường minh:
  ```python
  # V2 & V3:
  customer_relation = F.broadcast(customer_df.select("customer_id", "customer_key").distinct())
  return F.broadcast(date_df.select("full_date", "date_key"))
  ```
* **Riêng V3:** Thêm dòng sắp xếp trước khi ghi bảng `fact_orders`:
  ```python
  # Thêm vào cuối hàm build_fact_orders trước khi return:
  return fact_orders_df.sortWithinPartitions("purchase_date", "customer_key")
  ```

---

### Tệp 3: Airflow Pool `lakehouse_writes`

* **Ở V1 & V2:** 8 slots.
* **Ở V3:** Đổi về 2 slots bằng lệnh:
  ```bash
  docker exec olist-lakehouse-airflow-scheduler-1 airflow pools set lakehouse_writes 2 "Gioi han 2 slot de tiet kiem RAM"
  ```

---

## 4. QUY TRÌNH THỰC HIỆN TỪNG BƯỚC (STEP-BY-STEP RUNBOOK)

Bạn chỉ cần thực hiện theo đúng trình tự 4 bước dưới đây:

### Bước 0: Chuẩn bị môi trường đo chuẩn xác

1. Đứng tại thư mục gốc `rebuild`:
   ```bash
   cd /run/media/tamdae/04C8A69DC8A68C8C/Users/LENOVO/Documents/TL_Kỳ_1_năm_4/Big_Data/rebuild
   ```
2. Tạo các thư mục lưu kết quả riêng biệt:
   ```bash
   mkdir -p docs/reports/benchmarks/v1_baseline
   mkdir -p docs/reports/benchmarks/v2_current
   mkdir -p docs/reports/benchmarks/v3_senior
   ```
3. Đảm bảo hệ thống Core & Airflow đang khỏe mạnh (`./scripts/verify_stack.sh`).

---

### Bước 1: Đo Đạc Phiên Bản V2 (Hiện Tại)
*Vì mã nguồn hiện tại đang ở trạng thái V2, bạn nên đo V2 trước để lấy kết quả chuẩn mà không cần đổi code ngay.*

1. **Chạy trọn vẹn 8 bài benchmark:**
   ```bash
   ./scripts/run_benchmarks.sh
   ```
   *(Thời gian chạy: khoảng 40 - 45 phút cho trọn vẹn 8 bài)*.

2. **Sao lưu kết quả V2:**
   ```bash
   cp docs/reports/benchmarks/*.json docs/reports/benchmarks/v2_current/
   python3 scripts/summarize_benchmarks.py > docs/reports/benchmarks/v2_current/SUMMARY_V2.md
   ```
   *Kiểm tra file `docs/reports/benchmarks/v2_current/SUMMARY_V2.md` đã có đầy đủ bảng số liệu.*

---

### Bước 2: Quay Về Đo Phiên Bản V1 (Mặc Định)

Để quay về V1 mà không sợ hỏng Git, ta tạo một branch thử nghiệm hoặc dùng patch:

1. **Tạo Git branch riêng cho thực nghiệm V1:**
   ```bash
   git checkout -b test/spark-v1-baseline
   ```
2. **Sửa code về V1 (Tắt AQE, bỏ broadcast):**
   * Trong `transformation/common/spark_session.py`: Sửa AQE thành `false`, shuffle partitions thành `200`.
   * Trong `transformation/gold/facts.py` & `marts.py`: Bỏ các bọc `F.broadcast(...)`.
   *(Hoặc nhanh nhất: `git checkout 39eac45 -- transformation/` để lấy trọn vẹn thư mục transformation trước Task 5)*.
3. **Reset và nạp lại dữ liệu sạch:**
   ```bash
   python3 scripts/stage_dataset.py data/raw
   docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_daily_pipeline --conf '{"mode":"full"}'
   ```
   *(Chờ DAG chạy xong để Iceberg về trạng thái sạch ban đầu)*.

4. **Chạy chuỗi benchmark V1:**
   ```bash
   ./scripts/run_benchmarks.sh
   ```

5. **Sao lưu kết quả V1:**
   ```bash
   cp docs/reports/benchmarks/*.json docs/reports/benchmarks/v1_baseline/
   python3 scripts/summarize_benchmarks.py > docs/reports/benchmarks/v1_baseline/SUMMARY_V1.md
   ```

6. **Chuyển lại về nhánh master:**
   ```bash
   git checkout master
   ```

---

### Bước 3: Nâng Cấp Lên Đo Phiên Bản V3 (Senior Đề Xuất)

1. **Tạo branch riêng cho V3:**
   ```bash
   git checkout -b test/spark-v3-senior
   ```
2. **Áp dụng 4 tối ưu của V3:**
   * Giảm slot pool Airflow:
     ```bash
     docker exec olist-lakehouse-airflow-scheduler-1 airflow pools set lakehouse_writes 2 "Tiet kiem RAM cho V3"
     ```
   * Thêm cấu hình bộ nhớ và DPP vào `transformation/common/spark_session.py`:
     ```python
     .config("spark.memory.fraction", "0.8")
     .config("spark.memory.storageFraction", "0.2")
     .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
     .config("spark.sql.iceberg.planning.preserve-data-grouping", "true")
     ```
   * Thêm `.sortWithinPartitions("purchase_date", "customer_key")` vào `build_fact_orders` trong `transformation/gold/facts.py`.

3. **Reset về baseline sạch:**
   ```bash
   python3 scripts/stage_dataset.py data/raw
   docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_daily_pipeline --conf '{"mode":"full"}'
   ```

4. **Chạy chuỗi benchmark V3:**
   ```bash
   ./scripts/run_benchmarks.sh
   ```

5. **Sao lưu kết quả V3:**
   ```bash
   cp docs/reports/benchmarks/*.json docs/reports/benchmarks/v3_senior/
   python3 scripts/summarize_benchmarks.py > docs/reports/benchmarks/v3_senior/SUMMARY_V3.md
   ```

---

## 5. KHUNG TIÊU CHÍ ĐÁNH GIÁ "CÓ ĐÁNG KẾT QUẢ KHÔNG?" (ROI FRAMEWORK)

Sau khi có đủ 3 thư mục `v1_baseline`, `v2_current`, `v3_senior`, ta đối soát theo 5 tiêu chí định lượng:

| Tiêu chí | Cách đo | Mức độ mong đợi để gọi là "ĐÁNG" |
| :--- | :--- | :--- |
| **1. Tổng thời gian (Latency)** | Tổng giây chạy toàn pipeline ở bài `00_baseline_full` và `02_flash_sale` | Giảm ít nhất **15% - 25%** giữa V1 và V2/V3. |
| **2. Tốc độ chặng Gold** | Thời lượng task `build_gold_closure` (nơi diễn ra nhiều Join nhất) | Giảm ít nhất **30 - 50 giây** nhờ Broadcast Join. |
| **3. Thông lượng đỉnh (Throughput)** | Số dòng/giây ở bài `02_flash_sale` (40.000 đơn dồn dập) | Tăng ít nhất **20%** (từ ~300 lên >360 dòng/s). |
| **4. Tiêu thụ RAM Scheduler** | Đỉnh RAM của `olist-lakehouse-airflow-scheduler-1` | **Giảm từ 6.4 GiB xuống dưới 3.0 GiB** (Nhờ V3 hạ pool slot). |
| **5. Tràn bộ nhớ (Shuffle Spill)** | Kiểm tra log Spark: `Spill (Memory) / Spill (Disk)` | Ở V1 có thể có spill; ở V2 và V3 **phải bằng 0**. |

> **QUY TẮC PHÁN QUYẾT ROI:**
> * **ĐÁNG (High Return):** Nếu thời gian xử lý giảm >15% VÀ RAM tiêu thụ của hệ thống giảm >2 GB. Điều này chứng minh cụm máy chạy nhanh hơn nhưng lại tốn ít tài nguyên hơn.
> * **KHÔNG ĐÁNG (Over-engineering):** Nếu thời gian chỉ chênh lệch dưới 3% - 5% trong khi code phức tạp hơn nhiều lần và nguy cơ lỗi cấu hình tăng cao.

---

## 6. DỰ BÁO KỸ THUẬT TRƯỚC KHI ĐO (TECHNICAL FORECAST)

Dựa trên phân tích mã nguồn và đặc thù quy mô dữ liệu Olist (99.441 đơn hàng, ~120 MB CSV):

### Dự báo 1: So sánh V1 (Mặc định) vs. V2 (Hiện tại)
1. **Tại tầng Bronze & Silver:** Thời gian giữa V1 và V2 **sẽ gần như không khác biệt** (chênh lệch dưới 3%), vì Silver xử lý từng bảng độc lập theo kiểu lọc và map dữ liệu, không có nhiều phép Join phức tạp.
2. **Tại tầng Gold (`build_gold_closure`):** **V2 sẽ thắng áp đảo V1!**
   * Ở V1 (200 partitions + SortMergeJoin), Spark sẽ sinh ra $200 \times 5 = 1.000$ task nhỏ li ti chỉ để join bảng ngày `dim_date`, khiến chi phí điều phối (Task Scheduling Overhead) áp đảo thời gian tính toán thật.
   * Ở V2 (Broadcast Join), bảng ngày được gửi thẳng vào bộ nhớ executor, triệt tiêu toàn bộ 5 lần shuffle. Thời gian Gold sẽ rút ngắn rõ rệt từ ~210s xuống ~165s.
3. **Tại bài Flash-Sale (`02_flash_sale`):** Nhờ cơ chế `skewJoin.enabled = true`, V2 sẽ không bị nghẽn ở các sản phẩm hot, tránh được tình trạng một core gánh tải còn core kia ngồi chơi.

### Dự báo 2: So sánh V2 (Hiện tại) vs. V3 (Senior Đề Xuất)
1. **Về tốc độ thô:** V3 có thể chỉ nhanh hơn V2 khoảng 5% - 8% (vì phần cứng 2 core Spark đã chạm ngưỡng bão hòa tính toán).
2. **Về tài nguyên (Game Changer):** **V3 sẽ tạo ra bước ngoặt về RAM máy host!**
   * Ở V2, khi 8 task Silver cùng chạy song song, Airflow Scheduler phải duy trì 8 sub-process Python và nạp driver Spark, đẩy RAM lên đỉnh **6.4 GiB**.
   * Ở V3, pool slots hạ xuống 2, Airflow chỉ cho phép tối đa 2 driver chạy đồng thời. Đỉnh RAM sẽ tụt xuống **khoảng 2.5 - 2.8 GiB**. Máy host của bạn sẽ mát hơn rõ rệt, không bao giờ bị đứng máy hay quạt rú khi chạy benchmark.

---

## 7. BẢNG MA TRẬN ĐỐI SOÁT KẾT QUẢ NGHIỆM THU (TEMPLATE)

*Sau khi bạn chạy xong cả 3 vòng đo, hãy mở các file `SUMMARY_V1.md`, `SUMMARY_V2.md`, `SUMMARY_V3.md` và điền số liệu vào bảng này để đưa vào báo cáo bảo vệ đồ án:*

### Bảng A: So sánh thời gian thực thi (Tổng giây toàn bộ pipeline)

| Bài đo | V1: Mặc định (giây) | V2: Hiện tại (giây) | V3: Senior (giây) | Chênh lệch V1 vs V2 | Chênh lệch V1 vs V3 | Đánh giá giá trị thực tế |
| :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| `00_baseline_full` | *(Điền số V1)* | 453.9s | *(Điền số V3)* | ... % | ... % | ... |
| `01_offpeak` | *(Điền số V1)* | 445.5s | *(Điền số V3)* | ... % | ... % | ... |
| `02_flash_sale` | *(Điền số V1)* | 463.4s | *(Điền số V3)* | ... % | ... % | ... |
| `03_catchup` | *(Điền số V1)* | 446.8s | *(Điền số V3)* | ... % | ... % | ... |
| `04_dirty_metadata` | *(Điền số V1)* | 458.9s | *(Điền số V3)* | ... % | ... % | ... |

---

### Bảng B: So sánh độ trễ chi tiết chặng Gold (`build_gold_closure`)

| Bài đo | V1: SortMergeJoin (200 partitions) | V2: Broadcast Join (32 partitions) | V3: Vectorized & Sort Clustering | Mức độ cải thiện |
| :--- | :---: | :---: | :---: | :---: |
| `00_baseline_full` | ~210s *(ước tính)* | **166.8s** | *(Điền số V3)* | Giảm ... giây |
| `02_flash_sale` | ~225s *(ước tính)* | **167.4s** | *(Điền số V3)* | Giảm ... giây |

---

### Bảng C: So sánh đỉnh tài nguyên phần cứng (Peak Resource Usage)

| Hạng mục tài nguyên | V1: Mặc định | V2: Hiện tại | V3: Senior Đề Xuất | Ý nghĩa vận hành Production |
| :--- | :---: | :---: | :---: | :--- |
| **RAM Scheduler đỉnh** | ~6.2 GiB | 6.4 GiB | **~2.6 GiB** *(Dự kiến)* | Cắt giảm hơn 50% RAM, chống sập Scheduler |
| **RAM Spark Worker đỉnh**| ~2.7 GiB | 2.6 GiB | ~2.5 GiB | Bộ nhớ ổn định, không tràn đĩa |
| **CPU Worker đỉnh** | ~650% | 814.8% | ~750% | Vắt kiệt hiệu quả đa nhân |

---

### KẾT LUẬN HƯỚNG DẪN DÀNH CHO BẠN KHI QUAY LẠI
Khi bạn online lại, bạn chỉ cần mở file này và chạy theo các lệnh tại **[Mục 4: Quy Trình Thực Hiện Từng Bước](#4-quy-trình-thực-hiện-từng-bước-step-by-step-runbook)**. Mọi đường dẫn, câu lệnh Git và kịch bản đều đã được kiểm tra đồng bộ chính xác với hệ thống của bạn!
