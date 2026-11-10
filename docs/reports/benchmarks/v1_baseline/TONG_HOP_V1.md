# Tổng hợp số liệu Benchmark (sinh tự động)

> Tệp này do `scripts/summarize_benchmarks.py` kết xuất từ các file JSON đo được.

### Bảng 1 — Tổng quan các bài đo

| Bài đo | Chế độ | Kết quả | Tổng thời gian (giây) | Dòng ghi thêm | Thông lượng (dòng/giây) | Task chậm nhất |
|:---|:---:|:---:|---:|---:|---:|:---|
| `00_baseline_full` | full | **SUCCESS** | 1,006.0 | 636,273 | 632.5 | `build_gold_closure` |
| `01_offpeak` | incremental | **SUCCESS** | 1,288.1 | 1,057 | 0.8 | `build_gold_closure` |
| `02_flash_sale` | incremental | **SUCCESS** | 1,303.6 | 170,415 | 130.7 | `build_gold_closure` |
| `03_catchup` | incremental | **SUCCESS** | 1,289.0 | 16,210 | 12.6 | `build_gold_closure` |
| `04_dirty_metadata` | incremental | **SUCCESS** | 1,300.2 | 5,071 | 3.9 | `build_gold_closure` |
| `05_integrity_violations` | incremental | **FAILED** | 1,551.3 | 4,893 | 3.2 | `build_gold_closure` |
| `06_duplicates` | incremental | **FAILED** | 2,605.2 | 5,430 | 2.1 | `build_gold_closure` |
| `07_baseline_restore` | full | **FAILED** | 278.4 | 0 | 0.0 | `ingest_bronze_layer` |

### Bảng 2 — Độ trễ từng chặng (giây)

> Chặng Silver gồm 8 task chạy song song; con số ghi nhận là **task lâu nhất**, vì đó mới
> là thời gian thực tế mà chặng này chiếm trên đường găng của pipeline.

| Bài đo | Nạp Bronze | Silver (8 task song song) | Dựng Gold | Kiểm định chất lượng |
|:---|---:|---:|---:|---:|
| `00_baseline_full` | 39.0 | 159.3 | 713.0 | 80.8 |
| `01_offpeak` | 35.5 | 178.7 | 963.9 | 89.2 |
| `02_flash_sale` | 36.8 | 207.1 | 957.1 | 88.5 |
| `03_catchup` | 35.5 | 188.6 | 959.9 | 90.0 |
| `04_dirty_metadata` | 35.7 | 192.5 | 957.8 | 93.9 |
| `05_integrity_violations` | 35.9 | 199.2 | 971.0 | 18.9 |
| `06_duplicates` | 35.9 | 197.3 | 972.1 | 18.7 |
| `07_baseline_restore` | 4.6 | 0.0 | 0.0 | 0.0 |

### Bảng 3 — Tài nguyên đỉnh của Spark Worker và Trino

| Bài đo | Spark Worker CPU đỉnh | Spark Worker RAM đỉnh | Trino RAM đỉnh | Số mẫu đo |
|:---|---:|---:|---:|---:|
| `00_baseline_full` | 705.0% | 3,063 MiB | 1,447 MiB | 201 |
| `01_offpeak` | 632.5% | 3,166 MiB | 1,716 MiB | 257 |
| `02_flash_sale` | 767.1% | 3,217 MiB | 1,770 MiB | 260 |
| `03_catchup` | 593.1% | 3,360 MiB | 1,650 MiB | 257 |
| `04_dirty_metadata` | 694.4% | 3,644 MiB | 1,942 MiB | 259 |
| `05_integrity_violations` | 572.5% | 3,303 MiB | 1,919 MiB | 309 |
| `06_duplicates` | 835.0% | 3,701 MiB | 1,639 MiB | 519 |
| `07_baseline_restore` | 310.7% | 429 MiB | 1,348 MiB | 56 |

### Bảng 4 — Tác động lên dữ liệu và số tệp Parquet

| Bài đo | silver.orders (dòng) | quarantine.orders (dòng) | gold.fact_orders (dòng) | Tổng tệp Parquet sau | Thay đổi dung lượng |
|:---|---:|---:|---:|---:|---:|
| `00_baseline_full` | 99,244 | 197 | 99,441 | 15 | +27.7 MB |
| `01_offpeak` | 99,244 | 197 | 99,441 | 21 | +0.1 MB |
| `02_flash_sale` | 99,244 | 278 | 99,441 | 92 | +5.1 MB |
| `03_catchup` | 99,245 | 278 | 99,441 | 94 | +0.8 MB |
| `04_dirty_metadata` | 99,245 | 724 | 99,441 | 276 | +1.4 MB |
| `05_integrity_violations` | 99,245 | 861 | 99,441 | 497 | +1.4 MB |
| `06_duplicates` | 99,247 | 1,464 | 99,441 | 691 | +1.4 MB |
| `07_baseline_restore` | 99,247 | 1,464 | 99,441 | 691 | +0.0 MB |
