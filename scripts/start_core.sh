#!/usr/bin/env bash
# =====================================================================================
# SCRIPT KHỞI ĐỘNG HẠ TẦNG LÕI (CORE) CỦA OLIST DATA LAKEHOUSE
#
# QUY TRÌNH THỰC HIỆN:
#   1. Kiểm tra môi trường (Docker daemon, file .env).
#   2. Build ĐẦY ĐỦ toàn bộ Docker images cho tất cả các tầng (Spark, Airflow,
#      Superset, MinIO, MinIO mc).
#   3. Khởi động CHỈ các dịch vụ hạ tầng LÕI (Core Services):
#      - MinIO (S3 Object Storage - lưu trữ bronze, silver, gold, raw)
#      - MinIO Init (Tự động khởi tạo buckets và tài khoản IAM)
#      - PostgreSQL (Iceberg JDBC Catalog & DB metastore)
#      - Spark Master (Master node điều phối tính toán Iceberg)
#      - Spark Worker (Worker node thực thi tính toán)
#   4. Chờ healthcheck (--wait) và in bảng tổng hợp trạng thái các cổng Web UI.
#
# CÁCH DÙNG:
#   ./scripts/start_core.sh
#   ./scripts/start_core.sh --skip-build           # Bỏ qua bước build nếu image đã có sẵn
#   ./scripts/start_core.sh --no-cache             # Build lại toàn bộ sạch sẽ không dùng cache
#   ./scripts/start_core.sh --timeout-seconds 900  # Thời gian chờ tối đa (mặc định: 600s)
# =====================================================================================

set -eo pipefail

# ANSI color codes
CLR_RESET="\033[0m"
CLR_BOLD="\033[1m"
CLR_BLUE="\033[1;34m"
CLR_GREEN="\033[1;32m"
CLR_YELLOW="\033[1;33m"
CLR_RED="\033[1;31m"
CLR_CYAN="\033[1;36m"

# Load cấu hình Docker Compose chung
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_compose.sh"

# Tham số mặc định
SKIP_BUILD=0
NO_CACHE=0
PULL_IMAGES=0
TIMEOUT_SECONDS=600

PROG_NAME="$(basename "$0")"

print_usage() {
    echo -e "${CLR_BOLD}Cách dùng:${CLR_RESET} ./${PROG_NAME} [TUỲ_CHỌN] hoặc ./scripts/${PROG_NAME} [TUỲ_CHỌN]"
    echo ""
    echo -e "${CLR_BOLD}Mục đích:${CLR_RESET}"
    echo "  Build toàn bộ Docker images cho hệ thống Lakehouse, sau đó chỉ kích hoạt"
    echo "  các container hạ tầng LÕI (Core: MinIO, PostgreSQL, Spark Master, Spark Worker)."
    echo ""
    echo -e "${CLR_BOLD}Tuỳ chọn:${CLR_RESET}"
    echo "  -s, --skip-build             Bỏ qua bước build image, chạy ngay hạ tầng core"
    echo "      --no-cache               Build Docker images mà không dùng cache"
    echo "      --pull                   Luôn kéo base image mới nhất khi build"
    echo "  -t, --timeout-seconds GIÂY   Thời gian chờ tối đa cho healthcheck (mặc định: 600s)"
    echo "  -h, --help                   Hiển thị hướng dẫn này và thoát"
    echo ""
    echo -e "${CLR_BOLD}Ví dụ:${CLR_RESET}"
    echo "  ./scripts/${PROG_NAME}                           # Build toàn bộ rồi chạy core (khuyến nghị cho lần đầu)"
    echo "  ./scripts/${PROG_NAME} --skip-build              # Đã build trước đó hoặc đã docker load tarball"
    echo "  ./scripts/${PROG_NAME} --no-cache                # Rebuild triệt để khi sửa Dockerfile"
    exit 0
}

# Phân tích tham số dòng lệnh
while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--skip-build)
            SKIP_BUILD=1
            shift
            ;;
        --no-cache)
            NO_CACHE=1
            shift
            ;;
        --pull)
            PULL_IMAGES=1
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
            echo -e "${CLR_RED}Lỗi: Tham số không hợp lệ: $1${CLR_RESET}" >&2
            echo "Dùng '$0 --help' để xem hướng dẫn chi tiết." >&2
            exit 1
            ;;
    esac
done

# =====================================================================================
# BƯỚC 0: KIỂM TRA ĐIỀU KIỆN TIÊN QUYẾT (PRE-FLIGHT CHECKS)
# =====================================================================================
echo -e "${CLR_BLUE}=====================================================================${CLR_RESET}"
echo -e "${CLR_BLUE}        OLIST DATA LAKEHOUSE - KHỞI ĐỘNG HẠ TẦNG LÕI (CORE)         ${CLR_RESET}"
echo -e "${CLR_BLUE}=====================================================================${CLR_RESET}"

# 1. Kiểm tra lệnh docker
if ! command -v docker &> /dev/null; then
    echo -e "${CLR_RED}[LỖI] Không tìm thấy lệnh 'docker'. Vui lòng cài đặt Docker engine trước.${CLR_RESET}" >&2
    exit 1
fi

# 2. Kiểm tra Docker daemon đang chạy
if ! docker info > /dev/null 2>&1; then
    echo -e "${CLR_RED}[LỖI] Docker daemon chưa chạy hoặc user hiện tại không có quyền truy cập docker.sock.${CLR_RESET}" >&2
    echo -e "${CLR_YELLOW}Gợi ý: chạy 'sudo systemctl start docker' hoặc kiểm tra group docker.${CLR_RESET}" >&2
    exit 1
fi

# 3. Kiểm tra file cấu hình .env
if [[ ! -f "${PROJECT_ROOT}/.env" ]]; then
    if [[ -f "${PROJECT_ROOT}/.env.example" ]]; then
        echo -e "${CLR_YELLOW}[THÔNG BÁO] Chưa có file .env, đang tự động tạo từ .env.example...${CLR_RESET}"
        cp "${PROJECT_ROOT}/.env.example" "${PROJECT_ROOT}/.env"
        echo -e "${CLR_GREEN}[OK] Đã tạo file .env thành công.${CLR_RESET}"
    else
        echo -e "${CLR_RED}[LỖI] Không tìm thấy file .env hoặc .env.example trong thư mục dự án.${CLR_RESET}" >&2
        exit 1
    fi
fi

# =====================================================================================
# BƯỚC 1: BUILD TOÀN BỘ DOCKER IMAGES (THEO YÊU CẦU ĐẦU BÀI)
# =====================================================================================
if [[ ${SKIP_BUILD} -eq 1 ]]; then
    echo -e "\n${CLR_YELLOW}[BƯỚC 1/2] Bỏ qua build image theo tham số --skip-build.${CLR_RESET}"
else
    echo -e "\n${CLR_CYAN}[BƯỚC 1/2] Đang build toàn bộ Docker images cho tất cả các tầng...${CLR_RESET}"
    echo "  - Spark 3.5.9 + Iceberg 1.11.0 (Master & Worker)"
    echo "  - Apache Airflow 2.11.2 (Webserver, Scheduler, Triggerer, Init)"
    echo "  - Apache Superset 6.1.0 (BI Platform & Init)"
    echo "  - MinIO Storage & MinIO Client (Bucket & IAM Provisioner)"
    echo ""

    BUILD_CMD=("${DOCKER_COMPOSE[@]}" "build")
    if [[ ${NO_CACHE} -eq 1 ]]; then
        BUILD_CMD+=("--no-cache")
    fi
    if [[ ${PULL_IMAGES} -eq 1 ]]; then
        BUILD_CMD+=("--pull")
    fi

    echo -e "${CLR_BLUE}Thực thi: ${BUILD_CMD[*]}${CLR_RESET}"
    if "${BUILD_CMD[@]}"; then
        echo -e "${CLR_GREEN}==> Build toàn bộ images thành công!${CLR_RESET}"
    else
        EXIT_CODE=$?
        echo -e "${CLR_RED}[LỖI] Build images thất bại (mã lỗi ${EXIT_CODE}). Vui lòng kiểm tra Dockerfile hoặc kết nối mạng.${CLR_RESET}" >&2
        exit ${EXIT_CODE}
    fi
fi

# =====================================================================================
# BƯỚC 2: KHỞI ĐỘNG HẠ TẦNG LÕI (CORE ONLY)
# =====================================================================================
echo -e "\n${CLR_CYAN}[BƯỚC 2/2] Đang khởi động hạ tầng LÕI (Core Services)...${CLR_RESET}"
echo "  Các dịch vụ được kích hoạt:"
echo "    1. minio        : Lưu trữ Object Storage S3 (Buckets raw, lakehouse)"
echo "    2. minio-init   : Khởi tạo bucket và chính sách phân quyền IAM (Chạy một lần)"
echo "    3. postgres     : Cơ sở dữ liệu Iceberg JDBC Catalog & Metastore"
echo "    4. spark-master : Apache Spark 3.5.9 Cluster Master"
echo "    5. spark-worker : Apache Spark 3.5.9 Worker Node"
echo ""

# Chỉ dùng file cấu hình lõi: docker-compose.yml
DOCKER_COMPOSE_CORE=(docker compose --project-name "${COMPOSE_PROJECT_NAME}" -f "docker-compose.yml")

echo -e "${CLR_BLUE}Đang khởi động và chờ kiểm tra sức khỏe (Healthcheck tối đa ${TIMEOUT_SECONDS}s)...${CLR_RESET}"
if "${DOCKER_COMPOSE_CORE[@]}" up -d --wait --wait-timeout "${TIMEOUT_SECONDS}"; then
    echo -e "${CLR_GREEN}==> Toàn bộ dịch vụ Core đã khỏe mạnh (Healthy)!${CLR_RESET}"
else
    EXIT_CODE=$?
    echo -e "\n${CLR_RED}[LỖI] Khởi động hạ tầng core thất bại hoặc quá thời gian chờ (mã lỗi ${EXIT_CODE}).${CLR_RESET}" >&2
    echo -e "${CLR_YELLOW}Kiểm tra log của từng dịch vụ bằng lệnh:${CLR_RESET}" >&2
    echo "  ./scripts/logs.sh minio" >&2
    echo "  ./scripts/logs.sh minio-init" >&2
    echo "  ./scripts/logs.sh postgres" >&2
    echo "  ./scripts/logs.sh spark-master" >&2
    echo "  ./scripts/logs.sh spark-worker" >&2
    exit ${EXIT_CODE}
fi

# =====================================================================================
# BƯỚC 3: HIỂN THỊ TRẠNG THÁI & THÔNG TIN KẾT NỐI
# =====================================================================================
echo ""
echo -e "${CLR_BOLD}--- BẢNG TRẠNG THÁI CÁC DỊCH VỤ CORE ĐANG CHẠY ---${CLR_RESET}"
"${DOCKER_COMPOSE_CORE[@]}" ps --format "table {{.Service}}\t{{.Status}}\t{{.Ports}}"

cat << "EOF"

=====================================================================================
                      HẠ TẦNG LÕI (CORE) ĐÃ SẴN SÀNG!
=====================================================================================
  Dịch vụ               Địa chỉ Web UI / Endpoint     Tài khoản / Mô tả
  --------------------  ----------------------------  ------------------------------
  MinIO Web Console     http://localhost:9001         root / admin_root (xem .env)
  MinIO S3 API          http://localhost:9000         S3 storage endpoint
  Spark Master Web UI   http://localhost:8084         Quản lý cụm Spark Master
  Spark Worker Web UI   http://localhost:8085         Quản lý tài nguyên Spark Worker
  PostgreSQL (Catalog)  localhost:5432 (Nội bộ)       Iceberg JDBC Catalog & DBs
=====================================================================================

HƯỚNG DẪN CÁC THAO TÁC TIẾP THEO:
  1. Theo dõi log dịch vụ Core:
     ./scripts/logs.sh spark-master
     ./scripts/logs.sh minio

  2. Khởi động thêm các tầng khác khi cần:
     • Bật toàn bộ hệ thống (Trino, Airflow, Superset, Prometheus, Grafana):
       ./scripts/up.sh

     • Hoặc chỉ bật thêm tầng Truy vấn SQL (Trino):
       docker compose --project-name olist-lakehouse -f docker-compose.yml -f docker-compose.query.yml up -d --wait trino

     • Hoặc chỉ bật thêm tầng Điều phối Pipeline (Airflow):
       docker compose --project-name olist-lakehouse -f docker-compose.yml -f docker-compose.orchestration.yml up -d --wait

  3. Dừng hạ tầng Core khi dùng xong (bảo toàn 100% dữ liệu):
     ./scripts/down.sh
=====================================================================================
EOF
