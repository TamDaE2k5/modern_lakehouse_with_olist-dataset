#!/usr/bin/env bash
# =====================================================================================
# Dừng toàn bộ container của Lakehouse, BẢO TOÀN 100% dữ liệu trong Docker volume.
#
# Cách dùng:
#   ./scripts/down.sh
#   ./scripts/down.sh --remove-volumes   (sẽ hỏi xác nhận 'XOA' trước khi xoá volume)
# =====================================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_compose.sh"

REMOVE_VOLUMES=0

print_usage() {
    echo "Cách dùng: $0 [TUỲ_CHỌN]"
    echo "Tuỳ chọn:"
    echo "  -v, --remove-volumes   XÓA SẠCH toàn bộ volume dữ liệu (có hỏi xác nhận)"
    echo "  -h, --help             Hiển thị trợ giúp này"
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -v|--remove-volumes)
            REMOVE_VOLUMES=1
            shift
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

DOWN_ARGS=("down" "--remove-orphans")

if [[ ${REMOVE_VOLUMES} -eq 1 ]]; then
    echo -e "\033[1;33m[CANH BAO] Thao tac nay se XOA VINH VIEN toan bo du lieu Lakehouse (minio-data, postgres-data).\033[0m"
    read -r -p "Go dung chu XOA de xac nhan: " confirm
    if [[ "${confirm}" != "XOA" ]]; then
        echo "Da huy. Khong co gi bi xoa."
        exit 0
    fi
    DOWN_ARGS+=("-v")
fi

echo -e "\033[1;34mDang dung cac container...\033[0m"
"${DOCKER_COMPOSE[@]}" "${DOWN_ARGS[@]}"

if [[ ${REMOVE_VOLUMES} -eq 1 ]]; then
    echo -e "\033[1;32mDa dung va XOA volume. Lan chay sau se dung lai Lakehouse tu dau.\033[0m"
else
    echo -e "\033[1;32mDa dung. Du lieu van nguyen ven trong volume - bat lai bang ./scripts/up.sh\033[0m"
fi
