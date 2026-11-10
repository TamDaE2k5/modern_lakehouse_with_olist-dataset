#!/usr/bin/env bash
# =====================================================================================
# Tham số Docker Compose dùng chung cho các script vận hành Bash trên Ubuntu / Linux.
#
# VÌ SAO CHỈ CÓ BẤY NHIÊU ĐÂY?
# Bản tiền nhiệm của bộ script này từng có file dài hàng trăm dòng, tự chế cả cơ chế
# dò tìm đường dẫn Docker và lớp bọc bắt lỗi riêng (over-engineering).
#
# Thực tế, thứ duy nhất mà mọi lệnh vận hành đều cần dùng chung chỉ là **danh sách 5 file
# compose**. Mọi việc còn lại Docker Compose đã làm tốt hơn bất kỳ lớp bọc tự chế nào.
#
# CÁCH DÙNG: source từ script khác rồi dùng mảng "${DOCKER_COMPOSE[@]}"
#     source "$(dirname "${BASH_SOURCE[0]}")/_compose.sh"
#     "${DOCKER_COMPOSE[@]}" ps
# =====================================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Tự chuyển thư mục làm việc về gốc dự án để đường dẫn tương đối luôn phân giải đúng
cd "${PROJECT_ROOT}" || exit 1

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-olist-lakehouse}"

COMPOSE_FILES=(
    -f "docker-compose.yml"               # Hạ tầng lõi: MinIO, PostgreSQL, Spark
    -f "docker-compose.query.yml"         # Tầng truy vấn: Trino
    -f "docker-compose.orchestration.yml" # Tầng điều phối: Airflow
    -f "docker-compose.bi.yml"            # Tầng BI: Superset
    -f "docker-compose.monitoring.yml"    # Giám sát: Prometheus, Grafana, MailHog
)

DOCKER_COMPOSE=(docker compose --project-name "${COMPOSE_PROJECT_NAME}" "${COMPOSE_FILES[@]}")
