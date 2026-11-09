#!/usr/bin/env bash
# =====================================================================================
# Xem log của một dịch vụ trong cụm Lakehouse.
#
# Cách dùng:
#   ./scripts/logs.sh airflow-scheduler
#   ./scripts/logs.sh --service spark-worker --follow
#   ./scripts/logs.sh -s trino --tail 100
# =====================================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_compose.sh"

SERVICE=""
FOLLOW=0
TAIL=200

print_usage() {
    echo "Cách dùng: $0 [TUỲ_CHỌN] [TEN_DICH_VU]"
    echo "Tuỳ chọn:"
    echo "  -s, --service TEN   Tên dịch vụ cần xem log (vd: airflow-scheduler, trino, spark-worker)"
    echo "  -f, --follow        Bám đuôi log thời gian thực (follow)"
    echo "  -n, --tail DONG     Số dòng log cuối cùng cần hiển thị (mặc định: 200)"
    echo "  -h, --help          Hiển thị trợ giúp này"
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--service)
            SERVICE="$2"
            shift 2
            ;;
        -f|--follow)
            FOLLOW=1
            shift
            ;;
        -n|--tail)
            TAIL="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            ;;
        -*)
            echo "Lỗi: Tham số không hợp lệ: $1" >&2
            echo "Dùng '$0 --help' để xem hướng dẫn." >&2
            exit 1
            ;;
        *)
            if [[ -z "${SERVICE}" ]]; then
                SERVICE="$1"
                shift
            else
                echo "Lỗi: Dư tham số: $1" >&2
                exit 1
            fi
            ;;
    esac
done

if [[ -z "${SERVICE}" ]]; then
    echo -e "\033[1;31mLỗi: Chưa chỉ định tên dịch vụ.\033[0m" >&2
    echo "Cách dùng: $0 <ten-dich-vu> [--follow] [--tail 200]" >&2
    exit 1
fi

LOG_ARGS=("logs" "--tail" "${TAIL}")
if [[ ${FOLLOW} -eq 1 ]]; then
    LOG_ARGS+=("--follow")
fi
LOG_ARGS+=("${SERVICE}")

exec "${DOCKER_COMPOSE[@]}" "${LOG_ARGS[@]}"
