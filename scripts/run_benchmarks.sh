#!/usr/bin/env bash
# =====================================================================================
# Chạy trọn bộ chuỗi benchmark Giai đoạn 2 trên cả 6 bộ dữ liệu, rồi khôi phục trạng thái gốc.
#
# Cách dùng:
#   ./scripts/run_benchmarks.sh
#   ./scripts/run_benchmarks.sh --only 01_offpeak,02_flash_sale
#   ./scripts/run_benchmarks.sh --skip-restore
# =====================================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}" || exit 1

PYTHON_CMD="python3"
if ! command -v python3 &>/dev/null && command -v python &>/dev/null; then
    PYTHON_CMD="python"
fi

SKIP_RESTORE=0
ONLY_FILTER=""

print_usage() {
    echo "Cách dùng: $0 [TUỲ_CHỌN]"
    echo "Tuỳ chọn:"
    echo "  -o, --only BAI_DO          Chỉ chạy một hoặc vài bài đo (cách nhau bởi dấu phẩy)"
    echo "                             Ví dụ: --only 01_offpeak,02_flash_sale"
    echo "  -s, --skip-restore         Bỏ qua bước khôi phục baseline ở cuối (07_baseline_restore)"
    echo "  --python DUONG_DAN         Chỉ định trình thông dịch Python (mặc định: python3)"
    echo "  -h, --help                 Hiển thị trợ giúp này"
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -o|--only)
            ONLY_FILTER="$2"
            shift 2
            ;;
        -s|--skip-restore)
            SKIP_RESTORE=1
            shift
            ;;
        --python)
            PYTHON_CMD="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            ;;
        *)
            echo "Lỗi: Tham số không hợp lệ: $1" >&2
            echo "Dùng '$0 --help' để xem hướng dẫn." >&2
            exit 1
            ;;
    esac
done

# Định nghĩa danh sách các bài đo: "nhãn|thư_mục|chế_độ"
SUITE=(
    "00_baseline_full|data/raw|full"
    "01_offpeak|data/synthetic/01_offpeak|incremental"
    "02_flash_sale|data/synthetic/02_flash_sale|incremental"
    "03_catchup|data/synthetic/03_catchup|incremental"
    "04_dirty_metadata|data/synthetic/04_dirty_metadata|incremental"
    "05_integrity_violations|data/synthetic/05_integrity_violations|incremental"
    "06_duplicates|data/synthetic/06_duplicates|incremental"
)

if [[ ${SKIP_RESTORE} -eq 0 ]]; then
    SUITE+=("07_baseline_restore|data/raw|full")
fi

declare -a FILTERED_SUITE=()
for item in "${SUITE[@]}"; do
    IFS="|" read -r nhan thumuc mode <<< "${item}"
    if [[ -n "${ONLY_FILTER}" ]]; then
        # Kiểm tra xem nhãn có nằm trong danh sách phân tách bằng dấu phẩy không
        matched=0
        IFS="," read -ra filter_items <<< "${ONLY_FILTER}"
        for f in "${filter_items[@]}"; do
            f_trimmed="$(echo "${f}" | tr -d ' ')"
            if [[ "${f_trimmed}" == "${nhan}" ]]; then
                matched=1
                break
            fi
        done
        if [[ ${matched} -eq 0 ]]; then
            continue
        fi
    fi
    FILTERED_SUITE+=("${item}")
done

if [[ ${#FILTERED_SUITE[@]} -eq 0 ]]; then
    echo "Không tìm thấy bài đo nào khớp với bộ lọc: '${ONLY_FILTER}'"
    exit 1
fi

declare -a RESULTS=()

for item in "${FILTERED_SUITE[@]}"; do
    IFS="|" read -r nhan thumuc mode <<< "${item}"
    echo ""
    echo -e "\033[1;36m>>> BAI DO: ${nhan} (mode: ${mode}, data: ${thumuc})\033[0m"
    start_sec=$(date +%s.%N 2>/dev/null || date +%s)
    
    set +e
    "${PYTHON_CMD}" scripts/run_benchmark.py --dataset "${thumuc}" --label "${nhan}" --mode "${mode}"
    exit_code=$?
    set -e
    
    end_sec=$(date +%s.%N 2>/dev/null || date +%s)
    duration=$(awk "BEGIN {printf \"%.1f\", ${end_sec} - ${start_sec}}" 2>/dev/null || echo "N/A")
    
    if [[ ${exit_code} -eq 0 ]]; then
        status="SUCCESS"
    else
        status="FAILED"
        echo -e "\033[1;33m[CANH BAO] Bai do ${nhan} ket thuc voi ma loi ${exit_code}. Van tiep tuc cac bai con lai de thu du so lieu.\033[0m"
    fi
    
    RESULTS+=("${nhan}|${mode}|${status}|${duration}")
done

echo ""
echo -e "\033[1;36m=== TONG KET CHUOI BENCHMARK ===\033[0m"
printf "%-26s %-14s %-10s %-12s\n" "Bai do" "Che do" "Ket qua" "Thoi gian (s)"
printf "%-26s %-14s %-10s %-12s\n" "--------------------------" "--------------" "----------" "------------"

for res in "${RESULTS[@]}"; do
    IFS="|" read -r r_nhan r_mode r_status r_dur <<< "${res}"
    if [[ "${r_status}" == "SUCCESS" ]]; then
        status_colored="\033[1;32mSUCCESS\033[0m"
    else
        status_colored="\033[1;31mFAILED\033[0m"
    fi
    printf "%-26s %-14s " "${r_nhan}" "${r_mode}"
    echo -en "${status_colored}    "
    printf "%10s\n" "${r_dur}"
done

echo ""
echo "Chi tiet tung bai: docs/reports/benchmarks/*.json"
