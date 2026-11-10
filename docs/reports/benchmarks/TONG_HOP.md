# Tổng hợp số liệu Benchmark (sinh tự động)

> Tệp này do `scripts/summarize_benchmarks.py` kết xuất từ các file JSON đo được.

### Bảng 1 — Tổng quan các bài đo

| Bài đo | Chế độ | Kết quả | Tổng thời gian (giây) | Dòng ghi thêm | Thông lượng (dòng/giây) | Task chậm nhất |
|:---|:---:|:---:|---:|---:|---:|:---|
| `00_baseline_full` | full | **SUCCESS** | 332.6 | 636,273 | 1,913.0 | `build_gold_closure` |
| `01_offpeak` | incremental | **SUCCESS** | 346.5 | 1,057 | 3.1 | `silver_sellers` |
| `02_flash_sale` | incremental | **SUCCESS** | 348.0 | 170,415 | 489.7 | `build_gold_closure` |
| `03_catchup` | incremental | **SUCCESS** | 347.8 | 16,210 | 46.6 | `build_gold_closure` |
| `04_dirty_metadata` | incremental | **SUCCESS** | 348.8 | 5,071 | 14.5 | `build_gold_closure` |
| `05_integrity_violations` | incremental | **FAILED** | 602.6 | 4,893 | 8.1 | `silver_order_payments` |
| `06_duplicates` | incremental | **FAILED** | 602.2 | 5,430 | 9.0 | `build_gold_closure` |
| `07_baseline_restore` | full | **SUCCESS** | 359.7 | 0 | 0.0 | `silver_sellers` |

### Bảng 2 — Độ trễ từng chặng (giây)

> Chặng Silver gồm 8 task chạy song song; con số ghi nhận là **task lâu nhất**, vì đó mới
> là thời gian thực tế mà chặng này chiếm trên đường găng của pipeline.

| Bài đo | Nạp Bronze | Silver (8 task song song) | Dựng Gold | Kiểm định chất lượng |
|:---|---:|---:|---:|---:|
| `00_baseline_full` | 24.9 | 114.6 | 124.3 | 53.3 |
| `01_offpeak` | 21.3 | 129.4 | 125.0 | 48.3 |
| `02_flash_sale` | 23.6 | 124.0 | 126.0 | 53.3 |
| `03_catchup` | 22.8 | 121.4 | 130.5 | 55.0 |
| `04_dirty_metadata` | 23.0 | 125.5 | 125.5 | 55.3 |
| `05_integrity_violations` | 23.4 | 127.6 | 125.2 | 17.6 |
| `06_duplicates` | 23.2 | 126.3 | 126.4 | 17.9 |
| `07_baseline_restore` | 27.2 | 126.5 | 125.9 | 62.5 |

### Bảng 3 — Tài nguyên đỉnh của Spark Worker và Trino

| Bài đo | Spark Worker CPU đỉnh | Spark Worker RAM đỉnh | Trino RAM đỉnh | Số mẫu đo |
|:---|---:|---:|---:|---:|
| `00_baseline_full` | 791.8% | 2,087 MiB | 1,484 MiB | 67 |
| `01_offpeak` | 835.1% | 2,252 MiB | 1,612 MiB | 68 |
| `02_flash_sale` | 721.5% | 2,340 MiB | 1,796 MiB | 70 |
| `03_catchup` | 900.0% | 2,147 MiB | 1,798 MiB | 70 |
| `04_dirty_metadata` | 813.5% | 2,231 MiB | 1,866 MiB | 70 |
| `05_integrity_violations` | 778.1% | 2,394 MiB | 2,020 MiB | 120 |
| `06_duplicates` | 853.4% | 2,454 MiB | 2,144 MiB | 120 |
| `07_baseline_restore` | 729.6% | 2,472 MiB | 2,196 MiB | 72 |

### Bảng 4 — Tác động lên dữ liệu và số tệp Parquet

| Bài đo | silver.orders (dòng) | quarantine.orders (dòng) | gold.fact_orders (dòng) | Tổng tệp Parquet sau | Thay đổi dung lượng |
|:---|---:|---:|---:|---:|---:|
| `00_baseline_full` | 99,244 | 197 | 99,441 | 15 | +28.3 MB |
| `01_offpeak` | 99,244 | 197 | 99,441 | 21 | -0.2 MB |
| `02_flash_sale` | 99,244 | 278 | 99,441 | 53 | +4.9 MB |
| `03_catchup` | 99,245 | 278 | 99,441 | 55 | +0.7 MB |
| `04_dirty_metadata` | 99,245 | 724 | 99,441 | 90 | +0.6 MB |
| `05_integrity_violations` | 99,245 | 861 | 99,441 | 155 | +0.6 MB |
| `06_duplicates` | 99,247 | 1,464 | 99,441 | 189 | +0.5 MB |
| `07_baseline_restore` | 99,244 | 197 | 99,441 | 15 | -7.2 MB |
