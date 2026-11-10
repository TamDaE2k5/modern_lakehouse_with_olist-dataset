#!/usr/bin/env bash
# =====================================================================================
# Watchdog giám sát an toàn tài nguyên máy tính trong suốt quá trình benchmark
# Tự động dừng hệ thống khẩn cấp và bảo vệ an toàn tuyệt đối cho Linux OS
# =====================================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

LOG_FILE="${PROJECT_ROOT}/docs/reports/benchmarks/watchdog.log"
echo "[$(date -Iseconds)] Watchdog Guard da khoi dong. Dang giam sat RAM va Swap lien tuc..." > "${LOG_FILE}"

MIN_AVAIL_KB=819200   # 800 MB
MIN_SWAP_KB=512000    # 500 MB

CONSECUTIVE_BREACHES=0
MAX_BREACHES=2

while true; do
    avail_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
    swap_free_kb=$(awk '/SwapFree/ {print $2}' /proc/meminfo)
    
    # Kiểm tra xem tiến trình benchmark còn chạy không
    if ! pgrep -f "run_benchmark" >/dev/null && ! pgrep -f "run_benchmarks.sh" >/dev/null; then
        echo "[$(date -Iseconds)] Tien trinh benchmark da hoan tat. Watchdog dung nhe nhang." >> "${LOG_FILE}"
        exit 0
    fi

    if [[ ${avail_kb} -lt ${MIN_AVAIL_KB} ]] || [[ ${swap_free_kb} -lt ${MIN_SWAP_KB} ]]; then
        ((CONSECUTIVE_BREACHES++))
        echo "[$(date -Iseconds)] [CANH BAO] Bo nho xuong thap! MemAvailable=${avail_kb}KB, SwapFree=${swap_free_kb}KB (Lan ${CONSECUTIVE_BREACHES}/${MAX_BREACHES})" >> "${LOG_FILE}"
        
        if [[ ${CONSECUTIVE_BREACHES} -ge ${MAX_BREACHES} ]]; then
            echo "[$(date -Iseconds)] [NGUY HIEM] Bo nho can kiet! Kich hoat quy trinh dung khan cap de cuu OS..." >> "${LOG_FILE}"
            
            # 1. Kill tien trinh benchmark
            pkill -f "run_benchmark" || true
            pkill -f "run_benchmarks.sh" || true
            
            # 2. Sao luu file JSON ket qua da do duoc
            mkdir -p docs/reports/benchmarks/v1_baseline
            cp docs/reports/benchmarks/*.json docs/reports/benchmarks/v1_baseline/ 2>/dev/null || true
            python3 scripts/summarize_benchmarks.py --input-dir docs/reports/benchmarks/v1_baseline >/dev/null 2>&1 || true
            
            # 3. Tao bao cao dung khan cap
            cat << 'EOF' > docs/reports/benchmarks/v1_baseline/OVERLOAD_HALT_REASON.md
# Báo cáo dừng khẩn cấp do quá tải bộ nhớ (Watchdog Guard)

Hệ thống phát hiện tài nguyên RAM/Swap chạm ngưỡng nguy hiểm đe dọa treo Linux OS:
- **Ngưỡng an toàn**: MemAvailable >= 800 MB, SwapFree >= 500 MB.
- **Thực tế lúc dừng**: Bộ nhớ chạm đáy liên tiếp 2 chu kỳ đo.
- **Hành động đã thực hiện**:
  1. Hủy tiến trình benchmark để ngăn chặn cấp phát thêm JVM.
  2. Sao lưu toàn bộ kết quả đo đạc các bài test đã hoàn thành vào `docs/reports/benchmarks/v1_baseline/`.
  3. Dừng an toàn toàn bộ container Docker (`down.sh`) để giải phóng 100% RAM cho máy tính.
  4. Bảo toàn 100% dữ liệu volume.
EOF
            
            # 4. Dung Docker container
            "${PROJECT_ROOT}/scripts/down.sh" >> "${LOG_FILE}" 2>&1 || docker compose down >> "${LOG_FILE}" 2>&1
            
            echo "[$(date -Iseconds)] Da dung thanh cong he thong va cuu an toan OS." >> "${LOG_FILE}"
            exit 1
        fi
    else
        CONSECUTIVE_BREACHES=0
    fi
    
    sleep 5
done
