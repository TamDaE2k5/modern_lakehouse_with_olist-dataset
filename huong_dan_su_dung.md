# HƯỚNG DẪN SỬ DỤNG BÀI TẬP LỚN — OLIST DATA LAKEHOUSE

> Tài liệu này dành cho **người vận hành hệ thống hằng ngày** (chính bạn), không phải tài liệu kiến
> trúc. Mục tiêu: đọc xong là bật được cụm, chạy được pipeline, demo được cho thầy, và xử lý được
> các sự cố hay gặp — mà không cần mở bất kỳ tài liệu nào khác.
>
> Tài liệu kiến trúc chuyên sâu: xem [`README.md`](README.md) và thư mục [`docs/`](docs/).
> Kết quả nghiệm thu đầy đủ: [`docs/reports/BAO_CAO_HOAN_THANH_TOAN_DIEN_LAKEHOUSE.md`](docs/reports/BAO_CAO_HOAN_THANH_TOAN_DIEN_LAKEHOUSE.md).

---

## 0. Ba lệnh cần nhớ (nếu chỉ đọc được 10 giây thì đọc phần này)

Mở **PowerShell**, đứng tại thư mục `D:\BTL_NAM4\BIGDATA\lakehouse\rebuild`:

```powershell
.\scripts\up.ps1            # Bật cụm, tự chờ đến khi 13 container đều khỏe (3-6 phút)
.\scripts\verify_stack.ps1  # Kiểm tra sức khỏe toàn diện trong ~60 giây
.\scripts\down.ps1          # Tắt cụm, GIỮ NGUYÊN toàn bộ dữ liệu
```

Nếu `verify_stack.ps1` in ra dòng xanh **`KET QUA: TAT CA KIEM TRA DEU PASS`** thì hệ thống đang ở
đúng trạng thái đã nghiệm thu, bạn có thể demo ngay.

---

## 1. Chuẩn bị môi trường (chỉ làm một lần)

### 1.1. Yêu cầu máy

| Hạng mục | Yêu cầu | Ghi chú |
|:---|:---|:---|
| Docker Desktop | 4.x trở lên, bật WSL2 backend | **Dữ liệu Docker phải để ở ổ D:** (`D:\BTL_NAM4\BIGDATA\DockerDesktopWSL\`), không để ngốn ổ C: |
| RAM cấp cho WSL2 | Tối thiểu **8 GiB** | Cụm dùng thực tế 6-9 GiB lúc chạy pipeline |
| Dung lượng trống | ~25 GiB | Image + volume + dữ liệu |
| PowerShell | 5.1 (có sẵn trên Windows 11) | Các script `.ps1` viết cho bản này |
| Python trên máy host | 3.11+ kèm `pandas` | **Chỉ cần** khi sinh lại dữ liệu kiểm thử. Bật cụm và demo thì không cần |

> [!TIP]
> Cách đổi nơi lưu dữ liệu Docker sang ổ D: Docker Desktop → **Settings → Resources → Advanced →
> Disk image location** → trỏ vào `D:\BTL_NAM4\BIGDATA\DockerDesktopWSL\` → **Apply & Restart**.

### 1.2. Tệp cấu hình `.env`

Tệp `.env` đã có sẵn và **đã chạy đúng**. Nếu lỡ mất, tạo lại từ `.env.example` rồi điền mật khẩu.
Bộ giá trị đang dùng khớp với bảng tài khoản ở mục 2. Riêng khối `ALERT_*` đang trỏ vào MailHog nội
bộ — **không cần và không nên** điền mật khẩu Gmail thật vào đây.

### 1.3. Image Docker

Toàn bộ image đã nằm sẵn trên máy. Tệp `olist-lakehouse-images.tar` (**5,4 GB**) ở thư mục gốc chỉ
là **bản sao lưu để mang sang máy khác**; trên máy này **không cần nạp**, và có thể xóa nếu cần chỗ
trống. Khi nào cần dùng (máy mới, không có mạng):

```powershell
docker load -i .\olist-lakehouse-images.tar
```

---

## 2. Cổng truy cập và tài khoản

Tất cả đều chỉ mở trên `127.0.0.1` (không lộ ra mạng LAN).

| Giao diện | Địa chỉ | Tài khoản | Dùng để làm gì |
|:---|:---|:---|:---|
| **Airflow** | http://localhost:8083 | `admin` / `admin` | Bật/tắt, kích hoạt và theo dõi 4 DAG |
| **Superset** | http://localhost:8088 | `olist_admin` / `admin` | 5 dashboard kinh doanh, biểu đồ phễu |
| **Grafana** | http://localhost:3000 | `admin` / `admin` | 3 dashboard giám sát hạ tầng & chất lượng |
| **Trino** | http://localhost:8082 | user `trino`, bỏ trống mật khẩu | Chạy SQL trực tiếp trên Lakehouse |
| **MinIO Console** | http://localhost:9001 | `root` / `admin_root` | Xem tệp Parquet, bucket, metadata Iceberg |
| **MailHog** | http://localhost:8025 | *(không cần đăng nhập)* | Hộp thư nhận email cảnh báo sự cố |
| **Prometheus** | http://localhost:9090 | *(không cần đăng nhập)* | Kiểm tra 10 scrape target, alert rules |
| **Spark Master** | http://localhost:8084 | *(không cần đăng nhập)* | Theo dõi job Spark đang chạy |

---

## 3. Vòng đời hệ thống

### 3.1. Bật cụm

```powershell
.\scripts\up.ps1                      # Thông thường
.\scripts\up.ps1 -Build               # Khi vừa sửa Dockerfile, cần build lại image
.\scripts\up.ps1 -TimeoutSeconds 1800 # Khi máy chậm, cần chờ lâu hơn
```

Script dùng cờ `--wait` của Docker Compose nên **nó tự chờ** đến khi mọi healthcheck xanh mới trả
quyền điều khiển. Khi xong, nó in luôn bảng cổng truy cập.

### 3.2. Kiểm tra sức khỏe

```powershell
.\scripts\verify_stack.ps1
```

Chạy 5 nhóm kiểm tra, mỗi mục in `[PASS]` hoặc `[FAIL]`:

| Nhóm | Kiểm tra điều gì | Kỳ vọng |
|:---|:---|:---|
| 1. Container | 13 dịch vụ ở trạng thái running/healthy | 13/13 |
| 2. Web UI | 7 endpoint trả HTTP 200 | 7/7 |
| 3. Airflow DAG | 4 DAG đã nạp, không lỗi import | 4/4, 0 lỗi |
| 4. Đối soát dữ liệu | 4 con số doanh thu qua Trino | `99441` · `13591643.70` · `2251909.54` · `16008872.12` |
| 5. Prometheus | Scrape target UP | 10/10 |

**Bốn con số ở nhóm 4 chính là chữ ký của hệ thống.** Nếu chúng khớp, dữ liệu trong Lakehouse đang
đúng tới từng cent. Nếu lệch, nghĩa là bạn vừa nạp dữ liệu tổng hợp vào mà chưa khôi phục (xem mục 6.3).

### 3.3. Tắt cụm

```powershell
.\scripts\down.ps1        # Dừng container, DỮ LIỆU CÒN NGUYÊN. Bật lại bằng up.ps1 là có ngay.
```

> [!CAUTION]
> **`.\scripts\down.ps1 -RemoveVolumes` sẽ xóa vĩnh viễn toàn bộ Lakehouse** (volume `minio-data` và
> `postgres-data`). Script có hỏi xác nhận, bắt gõ đúng chữ `XOA`. Chỉ dùng khi thực sự muốn dựng lại
> từ đầu — và dựng lại mất khoảng 25-30 phút.
>
> Tương tự, **tệp `bash.txt` ở thư mục gốc có ghi lệnh `docker compose down -v`** — đừng copy-paste
> nguyên câu đó, cờ `-v` chính là cờ xóa sạch dữ liệu.

### 3.4. Xem log khi có trục trặc

```powershell
.\scripts\logs.ps1 -Service airflow-scheduler -Tail 200
.\scripts\logs.ps1 -Service spark-worker -Follow      # Bám đuôi log thời gian thực (Ctrl+C để thoát)
```

Tên dịch vụ thường dùng: `airflow-scheduler`, `airflow-webserver`, `spark-master`, `spark-worker`,
`trino`, `superset`, `minio`, `postgres`, `prometheus`, `grafana`, `mailhog`, `pipeline-metrics`.

---

## 4. Chạy pipeline

### 4.1. Bốn DAG làm gì

| DAG | Nội dung | Thời gian | Khi nào dùng |
|:---|:---|:---|:---|
| `olist_daily_pipeline` | Bronze → Silver → Gold → Quality (toàn trình, tự chứa) | ~7,5 phút | Nạp lại dữ liệu từ đầu, hoặc nạp lô mới |
| `olist_transformation` | Chỉ Silver → Gold, **không** nạp Bronze | ~5 phút | Sửa logic biến đổi, chạy lại mà không nạp lại thô |
| `olist_quality` | 198 bài kiểm định Great Expectations | ~2 phút | Muốn kiểm tra chất lượng riêng |
| `olist_maintenance` | Nén tệp, dọn snapshot, xóa tệp mồ côi của Iceberg | ~1,5 phút | Sau nhiều lần nạp tăng dần, khi số tệp Parquet phình to |

> [!IMPORTANT]
> `olist_daily_pipeline` có lịch `@daily` và `max_active_runs=1`. **Hệ quả thực tế:** ngay khi bạn
> bỏ pause nó, Airflow sẽ tự kích hoạt một lần chạy bù cho hôm nay, và lần chạy đó **chiếm chỗ**,
> khiến lệnh trigger thủ công của bạn phải xếp hàng chờ. Nếu đang vội demo, hãy chờ lần chạy đó
> xong (~7 phút) rồi mới trigger tiếp, hoặc giữ DAG ở trạng thái pause và chỉ trigger thủ công.

### 4.2. Kích hoạt từ giao diện Airflow

1. Mở http://localhost:8083, đăng nhập `admin` / `admin`.
2. Gạt công tắc bên trái tên DAG sang **xanh** (bỏ pause).
3. Bấm nút **▶ (Trigger DAG)**. Muốn truyền tham số thì chọn **Trigger DAG w/ config**.
4. Vào tab **Graph** để xem các ô task chuyển xanh lần lượt.

### 4.3. Kích hoạt bằng dòng lệnh

```powershell
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_daily_pipeline
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_quality
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_maintenance

# Xem các lần chạy gần đây của một DAG
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags list-runs -d olist_daily_pipeline
```

### 4.4. Ba tham số runtime

Khi trigger kèm config, cả 4 DAG đều nhận đúng 3 tham số:

| Tham số | Giá trị | Ý nghĩa |
|:---|:---|:---|
| `mode` | `full` hoặc `incremental` | `full` ghi đè toàn bộ bảng; `incremental` nạp thêm/upsert |
| `run_id` | chuỗi, mặc định `auto` | Mã phiên chạy, dùng để truy vết trong `lakehouse.control.*` |
| `batch_id` | chuỗi, mặc định `auto` | Mã lô dữ liệu nguồn |

Ví dụ trigger kèm tham số (PowerShell, dấu nháy kép phải được thoát bằng `\"`):

```powershell
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_daily_pipeline --conf '{\"mode\":\"full\",\"batch_id\":\"demo01\"}'
```

> `olist_daily_pipeline` mặc định là `full`; ba DAG còn lại mặc định `incremental`.

---

## 5. Truy vấn và xem kết quả

### 5.1. Chạy SQL bằng Trino

```powershell
docker exec -it olist-lakehouse-trino-1 trino
```

Vào rồi gõ SQL trực tiếp. Vài câu hữu ích:

```sql
-- Xem toàn bộ schema của Lakehouse
SHOW SCHEMAS FROM lakehouse;

-- Bốn con số chữ ký
SELECT count(*), sum(item_revenue), sum(freight_total), sum(payment_total)
FROM lakehouse.gold.fact_orders;

-- Dữ liệu bị cách ly vì không đạt chuẩn, kèm lý do
SELECT array_join(_error_codes, ', ') AS ly_do, count(*) AS so_dong
FROM lakehouse.quarantine.order_items GROUP BY 1 ORDER BY 2 DESC;

-- Lịch sử các lần chạy pipeline
SELECT * FROM lakehouse.control.pipeline_runs ORDER BY started_at DESC LIMIT 20;

-- Nhật ký bảo trì Iceberg (số tệp trước/sau khi nén)
SELECT target, action, status, files_before, files_after
FROM lakehouse.control.maintenance_results ORDER BY started_at DESC LIMIT 20;
```

Chạy một câu rồi thoát luôn (không vào chế độ tương tác):

```powershell
docker exec olist-lakehouse-trino-1 trino --execute "SELECT count(*) FROM lakehouse.gold.fact_orders"
```

Sáu truy vấn phân tích nghiệp vụ mẫu nằm sẵn ở [`sql/analysis/`](sql/analysis/).

### 5.2. Superset — phần đẹp nhất để demo

http://localhost:8088 → `olist_admin` / `admin` → menu **Dashboards**. Có 5 dashboard; đáng xem nhất
là **Executive Overview**, trong đó hàng thứ 3 là **biểu đồ phễu vòng đời đơn hàng**. Bấm vào một
bậc của phễu, cả dashboard sẽ tự lọc theo bậc đó (cross-filtering).

### 5.3. Grafana — phần ăn điểm về vận hành

http://localhost:3000 → `admin` / `admin` → dashboard **Pipeline**. Hai panel đáng chỉ cho thầy xem:

- **Tỷ lệ Dữ liệu Cách Ly (Quarantine Rate %)** — đồng hồ đo, xanh khi bình thường, **chuyển đỏ khi
  vượt 5%**. Đây là ngưỡng cảnh báo chất lượng dữ liệu.
- **Xu hướng thời lượng pipeline** — cho thấy pipeline có đang chậm dần đi hay không.

### 5.4. MailHog — chứng minh hệ thống biết tự báo động

http://localhost:8025. Mỗi khi một task Airflow thất bại, hệ thống tự gửi email vào đây kèm tên DAG,
tên task, lần thử thứ mấy và **1.000 ký tự cuối** của thông báo lỗi (phần cuối mới là nguyên nhân
gốc; phần đầu chỉ là dòng lệnh `spark-submit` dài dằng dặc).

**Cách tạo một cảnh báo thật để demo:** kích hoạt DAG bảo trì với tên bảng không có trong allowlist,
task chắc chắn thất bại, và sau 2 lần thử lại (~4-5 phút) email sẽ bay về hộp thư.

---

## 6. Bộ công cụ nâng cao

### 6.1. Sinh lại 6 bộ dữ liệu kiểm thử

Sáu bộ đã nằm sẵn ở `data/synthetic/`. Nếu cần sinh lại (cần `pandas` trên máy host):

```powershell
python .\scripts\generate_load_datasets.py    # 3 bộ test tải: 01_offpeak, 02_flash_sale, 03_catchup
python .\scripts\generate_chaos_datasets.py   # 3 bộ test chịu lỗi: 04, 05, 06
```

| Bộ | Nội dung | Dùng để chứng minh |
|:---|:---|:---|
| `01_offpeak` | 500 đơn giờ thấp điểm | Nạp tăng dần không nhân đôi dữ liệu |
| `02_flash_sale` | 40.000 đơn dồn dập | Khả năng chịu tải |
| `03_catchup` | 7.544 đơn, trạng thái pha trộn | Cơ chế upsert `MERGE INTO` |
| `04_dirty_metadata` | Metadata bẩn, lệch schema | Phòng vệ trước dữ liệu sai định dạng |
| `05_integrity_violations` | Vi phạm khóa ngoại, giá âm | Cơ chế cách ly (quarantine) |
| `06_duplicates` | Trùng lặp, xung đột trạng thái | Cơ chế khử trùng lặp |

Mỗi bộ chaos có kèm tệp `_injection_manifest.json` ghi **chính xác đã tiêm bao nhiêu lỗi loại nào** —
dùng để đối chiếu với số dòng bị cách ly thực tế. Đây là bằng chứng mạnh nhất khi bảo vệ.

### 6.2. Nạp một bộ dữ liệu lên MinIO

```powershell
python .\scripts\stage_dataset.py data\synthetic\02_flash_sale
```

Script xóa sạch `s3://raw/olist/` rồi chép bộ mới lên. Sau đó trigger DAG để Lakehouse xử lý.

### 6.3. Chạy benchmark

```powershell
.\scripts\run_benchmarks.ps1                                # Trọn bộ 8 bài, ~70 phút
.\scripts\run_benchmarks.ps1 -Only 01_offpeak,02_flash_sale # Chọn lọc vài bài
python .\scripts\summarize_benchmarks.py                    # Sinh lại bảng tổng hợp
```

Kết quả: các tệp JSON trong `docs/reports/benchmarks/` và bảng Markdown trong `TONG_HOP.md`.

> [!WARNING]
> **Bài `05` và `06` được thiết kế để DAG THẤT BẠI** — đó là kết quả đúng, chứng minh hệ thống biết
> chặn dữ liệu bẩn. Nhưng nếu bạn dừng giữa chừng sau hai bài đó, Lakehouse sẽ ở trạng thái đỏ.
> Luôn để chuỗi chạy hết bài `07_baseline_restore`, hoặc khôi phục thủ công:
>
> ```powershell
> python .\scripts\stage_dataset.py data\raw
> docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_daily_pipeline --conf '{\"mode\":\"full\"}'
> ```
>
> Chạy lại `verify_stack.ps1` để xác nhận 4 con số đã về đúng.

### 6.4. Cập nhật lại từ điển cột

```powershell
python .\scripts\refresh_column_dictionary.py
```

Dò schema thật trong Spark và Trino rồi sinh lại `docs/data_model/column_dictionary.md`. Chạy khi
bạn vừa thêm/bớt cột ở tầng nào đó, để tài liệu không bị trôi khỏi thực tế.

---

## 7. Sự cố hay gặp và cách xử lý

| Hiện tượng | Nguyên nhân | Cách xử lý |
|:---|:---|:---|
| `up.ps1` báo port đã bị chiếm | Cụm cũ `olist-final-20260907-r1` hoặc tiến trình khác đang giữ cổng | `docker ps -a` tìm container đang chạy rồi `docker stop <ten>`. **Đừng xóa cụm cũ** — chỉ dừng. |
| Container khởi động rồi chết, log ghi `password authentication failed` | Volume `postgres-data` cũ còn mật khẩu cũ | `docker exec -it olist-lakehouse-postgres-1 psql -U postgres -c "ALTER ROLE airflow WITH LOGIN PASSWORD 'admin';"` (làm tương tự với role `iceberg`, `superset`) |
| Grafana báo `password-auth.invalid` | Volume Grafana giữ mật khẩu cũ | `docker exec olist-lakehouse-grafana-1 grafana cli --config /etc/grafana/grafana.ini --homepath /usr/share/grafana admin reset-admin-password admin` |
| Task Spark bị OOM / cụm ì ạch | WSL2 được cấp quá ít RAM. Driver Spark chạy **trong container Airflow** (`deploy_mode=client`), nên `airflow-scheduler` mới là chỗ ngốn RAM, không phải `spark-worker` | Tăng RAM cho WSL2 trong `.wslconfig` lên 10-12 GiB |
| `verify_stack.ps1` FAIL ở nhóm 4 (sai doanh thu) | Vừa nạp dữ liệu tổng hợp mà chưa khôi phục | Làm theo mục **6.3** |
| DAG trigger xong mà không chạy | Lần chạy theo lịch đang chiếm chỗ (`max_active_runs=1`) | Chờ lần chạy đó xong, hoặc pause DAG rồi trigger lại |
| `docker exec` báo sai đường dẫn kiểu `C:/Program Files/Git/opt/...` | Git Bash tự chuyển đổi đường dẫn | Chạy bằng **PowerShell**, hoặc thêm `MSYS_NO_PATHCONV=1` trước lệnh |
| Chạy script Python bị `UnicodeEncodeError: 'cp932'` | Console Windows không ở UTF-8 | Các script đã tự xử lý. Nếu vẫn gặp: `chcp 65001` trước khi chạy |
| Không thấy email trong MailHog | Task chưa thất bại thật, hoặc `ALERT_EMAIL_ENABLED=false` | Kiểm tra `.env`, và nhớ rằng email chỉ gửi **sau khi task dùng hết số lần thử lại** |

Sự cố chuyên sâu hơn: xem [`docs/operations/runbook.md`](docs/operations/runbook.md) mục 4.

---

## 8. Kịch bản demo 10 phút

Chuẩn bị trước: chạy `up.ps1` **ít nhất 10 phút trước giờ demo**, rồi `verify_stack.ps1` cho đến khi
toàn bộ PASS. Mở sẵn 5 tab trình duyệt: Airflow, Superset, Grafana, MailHog, Trino.

| Phút | Việc làm | Câu nói chốt |
|:---:|:---|:---|
| 0-1 | Chạy `verify_stack.ps1` trước mặt thầy | "Toàn bộ 13 container, 7 giao diện, 4 DAG và 4 chỉ số tài chính đều được kiểm chứng tự động trong 60 giây." |
| 1-3 | Airflow → tab **Graph** của `olist_daily_pipeline` | "8 bảng Silver chạy song song, có cổng chất lượng chặn trước khi lên Gold." |
| 3-5 | Trino → chạy câu đối soát doanh thu | "Mỗi tổng tiền ở tầng Gold khớp **từng cent** với tệp CSV nguồn của nó." |
| 5-7 | Superset → **Executive Overview**, bấm vào phễu | "Phễu này lấy số trực tiếp từ Lakehouse, và bấm vào là cả dashboard lọc theo." |
| 7-8 | Grafana → panel **Quarantine Rate** | "Vượt 5% là đồng hồ chuyển đỏ — hệ thống tự biết chất lượng dữ liệu đang xuống." |
| 8-10 | MailHog → mở một email cảnh báo | "Và khi hỏng, nó tự gửi email kèm đúng nguyên nhân gốc, không cần ai ngồi canh." |

**Ba con số nên nói ra trong 30 giây đầu**, vì cả ba đều là đo thật:

1. **136 tệp → 1 tệp (−99,3%)** sau khi nén — hiểu đúng bài toán small-file của lakehouse.
2. **170.415 dòng chỉ làm chậm 2,1%** — kiến trúc chịu tải, có số liệu chứng minh.
3. **297/299 lần khử trùng lặp chọn sai bản ghi** — tự tìm ra lỗi logic trong hệ thống mình xây,
   bằng bộ dữ liệu chaos tự thiết kế. Đây là điểm mạnh nhất của bài.

---

## 9. Những việc tuyệt đối không làm

1. **Không chạy `down.ps1 -RemoveVolumes`** trừ khi thực sự muốn dựng lại từ đầu (mất 25-30 phút).
2. **Không copy lệnh `docker compose down -v` từ `bash.txt`** — cờ `-v` xóa sạch dữ liệu.
3. **Không xóa cụm cũ `olist-final-20260907-r1`** — chỉ dừng, dữ liệu trong đó vẫn cần giữ.
4. **Không điền mật khẩu Gmail thật vào `.env`** — đã có MailHog làm hộp thư giả lập.
5. **Không demo ngay sau khi nạp bộ chaos `05`/`06`** mà quên khôi phục — hệ thống sẽ đỏ trước mặt thầy.

---

## 10. Bản đồ tài liệu — cần gì đọc đó

| Khi bạn muốn… | Đọc tệp này |
|:---|:---|
| **Cài đặt lần đầu trên một máy mới** (nạp image, dựng lại từ con số 0) | [`CAI_DAT_CHO_NHOM.md`](CAI_DAT_CHO_NHOM.md) |
| Hiểu kiến trúc tổng thể, bảng cổng, cấu trúc thư mục | [`README.md`](README.md) |
| Xem kết quả nghiệm thu, số liệu benchmark, 9 ý tưởng nâng cấp | [`docs/reports/BAO_CAO_HOAN_THANH_TOAN_DIEN_LAKEHOUSE.md`](docs/reports/BAO_CAO_HOAN_THANH_TOAN_DIEN_LAKEHOUSE.md) |
| Xử lý sự cố vận hành chuyên sâu, sao lưu & khôi phục | [`docs/operations/runbook.md`](docs/operations/runbook.md) |
| Kịch bản thuyết minh chi tiết hơn | [`docs/operations/demo_guide.md`](docs/operations/demo_guide.md) |
| Hiểu luồng dữ liệu Bronze → Silver → Gold | [`docs/architecture/data_flow.md`](docs/architecture/data_flow.md) |
| Tra ý nghĩa từng cột trong từng bảng | [`docs/data_model/column_dictionary.md`](docs/data_model/column_dictionary.md) |
| Hiểu mô hình sao (star schema) tầng Gold | [`docs/data_model/star_old.md`](docs/data_model/star_old.md) |
| Biết bản rebuild hơn bản cũ ở điểm nào | [`docs/SO_SANH_BAN_CU_VA_MOI.md`](docs/SO_SANH_BAN_CU_VA_MOI.md) |
| Đọc nhật ký triển khai từ con số 0 | [`NHAT_KY_REBUILD_LAKEHOUSE.md`](NHAT_KY_REBUILD_LAKEHOUSE.md) |

---

## 11. Danh mục kiểm tra trước buổi bảo vệ

- [ ] Đã sao lưu toàn bộ thư mục dự án sang ổ đĩa khác (hiện **chưa có git**, chỉ tồn tại một bản).
- [ ] Đã chạy `up.ps1` và `verify_stack.ps1` → toàn bộ PASS.
- [ ] Bốn con số doanh thu đúng: `99441` · `13591643.70` · `2251909.54` · `16008872.12`.
- [ ] Hộp thư MailHog có sẵn ít nhất một email cảnh báo để trình diễn.
- [ ] Đã mở sẵn 5 tab trình duyệt, đăng nhập sẵn (không để thầy chờ lúc gõ mật khẩu).
- [ ] Đã thuộc ba con số ở mục 8.
- [ ] Laptop cắm sạc, đã tắt các ứng dụng nặng khác để nhường RAM cho Docker.
