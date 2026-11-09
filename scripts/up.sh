#!/usr/bin/env bash
# =====================================================================================
# Khởi động toàn bộ 13 container của Olist Data Lakehouse và chờ tới khi tất cả khỏe mạnh.
#
# Cách dùng:
#   ./scripts/up.sh
#   ./scripts/up.sh --build
#   ./scripts/up.sh --timeout-seconds 1200
# =====================================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_compose.sh"

BUILD=0
TIMEOUT_SECONDS=900

print_usage() {
    echo "Cách dùng: $0 [TUỲ_CHỌN]"
    echo "Tuỳ chọn:"
    echo "  -b, --build                  Build lại image trước khi khởi động"
    echo "  -t, --timeout-seconds GIÂY   Thời gian chờ tối đa (mặc định: 900 giây)"
    echo "  -h, --help                   Hiển thị trợ giúp này"
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -b|--build)
            BUILD=1
            shift
            ;;
        -t|--timeout-seconds|--timeout)
            TIMEOUT_SECONDS="$2"
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

UP_ARGS=("up" "-d" "--wait" "--wait-timeout" "${TIMEOUT_SECONDS}")
if [[ ${BUILD} -eq 1 ]]; then
    UP_ARGS+=("--build")
fi

echo -e "\033[1;34mDang khoi dong Olist Data Lakehouse (han cho ${TIMEOUT_SECONDS} giay)...\033[0m"
"${DOCKER_COMPOSE[@]}" "${UP_ARGS[@]}"
EXIT_CODE=$?

if [[ ${EXIT_CODE} -ne 0 ]]; then
    echo -e "\033[1;31mKhoi dong that bai (ma loi ${EXIT_CODE}). Xem log: ./scripts/logs.sh <ten-dich-vu>\033[0m" >&2
    exit ${EXIT_CODE}
fi

echo ""
"${DOCKER_COMPOSE[@]}" ps --format "table {{.Service}}\t{{.Status}}"

cat << "EOF"

--- CONG TRUY CAP WEB UI ---
  Airflow     http://localhost:8083    admin / admin
  Trino       http://localhost:8082    user: trino
  Superset    http://localhost:8088    olist_admin / admin
  MinIO       http://localhost:9001    root / admin_root
  Grafana     http://localhost:3000    admin / admin
  Prometheus  http://localhost:9090
  MailHog     http://localhost:8025    (hop thu canh bao su co)
  Spark       http://localhost:8084
EOF
