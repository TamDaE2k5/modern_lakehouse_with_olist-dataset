# Tổng hợp số liệu Benchmark (sinh tự động)

> Tệp này do `scripts/summarize_benchmarks.py` kết xuất từ các file JSON đo được.

### Bảng 1 — Tổng quan các bài đo

| Bài đo | Chế độ | Kết quả | Tổng thời gian (giây) | Dòng ghi thêm | Thông lượng (dòng/giây) | Task chậm nhất |
|:---|:---:|:---:|---:|---:|---:|:---|
| `00_baseline_full` | full | **SUCCESS** | 453.9 | 0 | 0.0 | `build_gold_closure` |
| `01_offpeak` | incremental | **SUCCESS** | 445.5 | 1,057 | 2.4 | `build_gold_closure` |
| `02_flash_sale` | incremental | **SUCCESS** | 463.4 | 170,415 | 367.7 | `silver_sellers` |
| `03_catchup` | incremental | **SUCCESS** | 446.8 | 16,210 | 36.3 | `build_gold_closure` |
| `04_dirty_metadata` | incremental | **SUCCESS** | 458.9 | 5,071 | 11.1 | `build_gold_closure` |
| `05_integrity_violations` | incremental | **FAILED** | 701.0 | 4,893 | 7.0 | `build_gold_closure` |
| `06_duplicates` | incremental | **FAILED** | 696.4 | 5,430 | 7.8 | `build_gold_closure` |
| `07_baseline_restore` | full | **SUCCESS** | 457.5 | 0 | 0.0 | `build_gold_closure` |

### Bảng 2 — Độ trễ từng chặng (giây)

> Chặng Silver gồm 8 task chạy song song; con số ghi nhận là **task lâu nhất**, vì đó mới
> là thời gian thực tế mà chặng này chiếm trên đường găng của pipeline.

| Bài đo | Nạp Bronze | Silver (8 task song song) | Dựng Gold | Kiểm định chất lượng |
|:---|---:|---:|---:|---:|
| `00_baseline_full` | 37.7 | 158.6 | 166.8 | 77.5 |
| `01_offpeak` | 30.4 | 156.9 | 166.6 | 71.8 |
| `02_flash_sale` | 34.2 | 168.8 | 167.4 | 75.9 |
| `03_catchup` | 31.0 | 156.9 | 168.6 | 75.6 |
| `04_dirty_metadata` | 33.1 | 163.9 | 165.3 | 78.8 |
| `05_integrity_violations` | 32.4 | 164.7 | 166.3 | 22.2 |
| `06_duplicates` | 30.5 | 159.8 | 165.1 | 22.2 |
| `07_baseline_restore` | 35.9 | 159.2 | 160.8 | 87.2 |

### Bảng 3 — Tài nguyên đỉnh của Spark Worker và Trino

| Bài đo | Spark Worker CPU đỉnh | Spark Worker RAM đỉnh | Trino RAM đỉnh | Số mẫu đo |
|:---|---:|---:|---:|---:|
| `00_baseline_full` | 611.9% | 2,478 MiB | 2,596 MiB | 90 |
| `01_offpeak` | 674.8% | 2,704 MiB | 2,698 MiB | 88 |
| `02_flash_sale` | 814.8% | 2,600 MiB | 2,754 MiB | 92 |
| `03_catchup` | 698.3% | 2,694 MiB | 2,791 MiB | 88 |
| `04_dirty_metadata` | 788.1% | 2,582 MiB | 2,915 MiB | 91 |
| `05_integrity_violations` | 580.0% | 2,529 MiB | 2,907 MiB | 139 |
| `06_duplicates` | 617.2% | 2,481 MiB | 2,935 MiB | 138 |
| `07_baseline_restore` | 836.4% | 2,578 MiB | 2,953 MiB | 91 |

### Bảng 4 — Tác động lên dữ liệu và số tệp Parquet

| Bài đo | silver.orders (dòng) | quarantine.orders (dòng) | gold.fact_orders (dòng) | Tổng tệp Parquet sau | Thay đổi dung lượng |
|:---|---:|---:|---:|---:|---:|
| `00_baseline_full` | 99,244 | 197 | 99,441 | 15 | +0.0 MB |
| `01_offpeak` | 99,244 | 197 | 99,441 | 21 | -0.2 MB |
| `02_flash_sale` | 99,244 | 278 | 99,441 | 53 | +4.9 MB |
| `03_catchup` | 99,245 | 278 | 99,441 | 55 | +0.7 MB |
| `04_dirty_metadata` | 99,245 | 724 | 99,441 | 90 | +0.6 MB |
| `05_integrity_violations` | 99,245 | 861 | 99,441 | 155 | +0.6 MB |
| `06_duplicates` | 99,247 | 1,464 | 99,441 | 189 | +0.5 MB |
| `07_baseline_restore` | 99,244 | 197 | 99,441 | 15 | -7.2 MB |
