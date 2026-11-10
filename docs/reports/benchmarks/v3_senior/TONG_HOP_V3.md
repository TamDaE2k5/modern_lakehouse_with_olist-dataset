# Tổng hợp số liệu Benchmark (sinh tự động)

> Tệp này do `scripts/summarize_benchmarks.py` kết xuất từ các file JSON đo được.

### Bảng 1 — Tổng quan các bài đo

| Bài đo | Chế độ | Kết quả | Tổng thời gian (giây) | Dòng ghi thêm | Thông lượng (dòng/giây) | Task chậm nhất |
|:---|:---:|:---:|---:|---:|---:|:---|
| `00_baseline_full` | full | **SUCCESS** | 335.4 | 636,273 | 1,897.1 | `build_gold_closure` |
| `01_offpeak` | incremental | **SUCCESS** | 337.5 | 1,057 | 3.1 | `build_gold_closure` |
| `02_flash_sale` | incremental | **SUCCESS** | 358.7 | 170,415 | 475.1 | `build_gold_closure` |
| `03_catchup` | incremental | **SUCCESS** | 346.6 | 16,210 | 46.8 | `build_gold_closure` |
| `04_dirty_metadata` | incremental | **SUCCESS** | 351.0 | 5,071 | 14.4 | `build_gold_closure` |
| `05_integrity_violations` | incremental | **FAILED** | 612.3 | 4,893 | 8.0 | `build_gold_closure` |
| `06_duplicates` | incremental | **FAILED** | 608.8 | 5,430 | 8.9 | `build_gold_closure` |
| `07_baseline_restore` | full | **SUCCESS** | 362.9 | 0 | 0.0 | `build_gold_closure` |

### Bảng 2 — Độ trễ từng chặng (giây)

> Chặng Silver gồm 8 task chạy song song; con số ghi nhận là **task lâu nhất**, vì đó mới
> là thời gian thực tế mà chặng này chiếm trên đường găng của pipeline.

| Bài đo | Nạp Bronze | Silver (8 task song song) | Dựng Gold | Kiểm định chất lượng |
|:---|---:|---:|---:|---:|
| `00_baseline_full` | 27.1 | 35.9 | 127.9 | 54.0 |
| `01_offpeak` | 22.6 | 31.2 | 131.2 | 50.1 |
| `02_flash_sale` | 27.6 | 33.4 | 130.1 | 52.5 |
| `03_catchup` | 22.7 | 32.8 | 132.7 | 52.9 |
| `04_dirty_metadata` | 22.7 | 33.5 | 129.5 | 55.8 |
| `05_integrity_violations` | 23.0 | 32.8 | 136.9 | 17.0 |
| `06_duplicates` | 23.1 | 38.5 | 129.2 | 16.7 |
| `07_baseline_restore` | 27.0 | 35.1 | 128.8 | 64.6 |

### Bảng 3 — Tài nguyên đỉnh của Spark Worker và Trino

| Bài đo | Spark Worker CPU đỉnh | Spark Worker RAM đỉnh | Trino RAM đỉnh | Số mẫu đo |
|:---|---:|---:|---:|---:|
| `00_baseline_full` | 555.3% | 2,259 MiB | 1,185 MiB | 67 |
| `01_offpeak` | 772.7% | 2,181 MiB | 1,482 MiB | 68 |
| `02_flash_sale` | 788.2% | 2,354 MiB | 1,760 MiB | 72 |
| `03_catchup` | 883.5% | 2,188 MiB | 1,867 MiB | 69 |
| `04_dirty_metadata` | 812.9% | 2,310 MiB | 1,973 MiB | 70 |
| `05_integrity_violations` | 779.2% | 2,477 MiB | 2,091 MiB | 122 |
| `06_duplicates` | 689.0% | 2,312 MiB | 2,084 MiB | 122 |
| `07_baseline_restore` | 922.3% | 2,405 MiB | 2,124 MiB | 73 |

### Bảng 4 — Tác động lên dữ liệu và số tệp Parquet

| Bài đo | silver.orders (dòng) | quarantine.orders (dòng) | gold.fact_orders (dòng) | Tổng tệp Parquet sau | Thay đổi dung lượng |
|:---|---:|---:|---:|---:|---:|
| `00_baseline_full` | 99,244 | 197 | 99,441 | 15 | +28.2 MB |
| `01_offpeak` | 99,244 | 197 | 99,441 | 21 | -0.2 MB |
| `02_flash_sale` | 99,244 | 278 | 99,441 | 53 | +5.0 MB |
| `03_catchup` | 99,245 | 278 | 99,441 | 55 | +0.7 MB |
| `04_dirty_metadata` | 99,245 | 724 | 99,441 | 90 | +0.6 MB |
| `05_integrity_violations` | 99,245 | 861 | 99,441 | 155 | +0.6 MB |
| `06_duplicates` | 99,247 | 1,464 | 99,441 | 189 | +0.5 MB |
| `07_baseline_restore` | 99,244 | 197 | 99,441 | 15 | -7.2 MB |
