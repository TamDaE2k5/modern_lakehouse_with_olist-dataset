#!/usr/bin/env bash
# =====================================================================================
# Kiểm tra sức khỏe toàn diện cụm Lakehouse: container, cổng Web UI, DAG, dữ liệu và doanh thu.
#
# Cách dùng:
#   ./scripts/verify_stack.sh
# =====================================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_compose.sh"

FAILURES=0

write_check() {
    local name="$1"
    local ok="$2"
    local detail="$3"
    
    if [[ "${ok}" -eq 1 ]]; then
        printf "  \033[1;32m[PASS]\033[0m %-44s %s\n" "${name}" "${detail}"
    else
        printf "  \033[1;31m[FAIL]\033[0m %-44s %s\n" "${name}" "${detail}"
        ((FAILURES++))
    fi
}

echo ""
echo -e "\033[1;34m=== 1. TRANG THAI CONTAINER ===\033[0m"
ps_lines=$("${DOCKER_COMPOSE[@]}" ps --format "{{.Service}};{{.State}};{{.Health}}" 2>/dev/null || true)

if [[ -z "${ps_lines}" ]]; then
    write_check "Docker Compose PS" 0 "Khong lay duoc thong tin container"
else
    while IFS=";" read -r service state health; do
        [[ -z "${service}" ]] && continue
        is_ok=0
        if [[ "${state}" == "running" ]] && [[ "${health}" == "healthy" || -z "${health}" ]]; then
            is_ok=1
        fi
        detail="${state}"
        [[ -n "${health}" ]] && detail="${detail} ${health}"
        write_check "${service}" "${is_ok}" "${detail}"
    done <<< "${ps_lines}"
fi

echo ""
echo -e "\033[1;34m=== 2. CONG WEB UI ===\033[0m"
declare -A ENDPOINTS=(
    ["Airflow"]="http://localhost:8083/health"
    ["Trino"]="http://localhost:8082/v1/info"
    ["Superset"]="http://localhost:8088/health"
    ["MinIO"]="http://localhost:9000/minio/health/live"
    ["Prometheus"]="http://localhost:9090/-/healthy"
    ["Grafana"]="http://localhost:3000/api/health"
    ["MailHog"]="http://localhost:8025/api/v2/messages?limit=1"
)

# Danh sách thứ tự hiển thị cố định
ENDPOINT_KEYS=("Airflow" "Trino" "Superset" "MinIO" "Prometheus" "Grafana" "MailHog")

for name in "${ENDPOINT_KEYS[@]}"; do
    url="${ENDPOINTS[$name]}"
    status_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "${url}" 2>/dev/null || echo "000")
    if [[ "${status_code}" == "200" ]]; then
        write_check "${name}" 1 "HTTP ${status_code}"
    else
        if [[ "${status_code}" == "000" ]]; then
            write_check "${name}" 0 "khong ket noi duoc"
        else
            write_check "${name}" 0 "HTTP ${status_code}"
        fi
    fi
done

echo ""
echo -e "\033[1;34m=== 3. AIRFLOW DAGS ===\033[0m"
dag_output=$(docker exec olist-lakehouse-airflow-scheduler-1 airflow dags list 2>/dev/null || true)
EXPECTED_DAGS=("olist_daily_pipeline" "olist_transformation" "olist_quality" "olist_maintenance")

for dag in "${EXPECTED_DAGS[@]}"; do
    if echo "${dag_output}" | grep -q "${dag}"; then
        write_check "${dag}" 1 "da nap"
    else
        write_check "${dag}" 0 "chua nap"
    fi
done

import_errors=$(docker exec olist-lakehouse-airflow-scheduler-1 airflow dags list-import-errors 2>/dev/null || true)
if echo "${import_errors}" | grep -q "No data found"; then
    write_check "Khong co loi import DAG" 1 ""
else
    write_check "Khong co loi import DAG" 0 "Co loi import DAG"
fi

echo ""
echo -e "\033[1;34m=== 4. DOI SOAT DU LIEU QUA TRINO ===\033[0m"
sql="SELECT count(*), sum(item_revenue), sum(freight_total), sum(payment_total) FROM lakehouse.gold.fact_orders"
trino_row=$(docker exec olist-lakehouse-trino-1 trino --execute "${sql}" --output-format CSV 2>/dev/null | head -n 1 || true)

if [[ -n "${trino_row}" ]]; then
    # Bỏ dấu ngoặc kép nếu có
    clean_row=$(echo "${trino_row}" | tr -d '"')
    IFS="," read -r val_count val_revenue val_freight val_payment <<< "${clean_row}"
    
    # So sánh count
    if [[ "${val_count}" == "99441" ]]; then
        write_check "fact_orders: so don hang" 1 "${val_count} (ky vong 99441)"
    else
        write_check "fact_orders: so don hang" 0 "${val_count} (ky vong 99441)"
    fi
    
    # So sánh revenue
    if [[ "${val_revenue}" == "13591643.70" || "${val_revenue}" == "13591643.7" ]]; then
        write_check "fact_orders: doanh thu hang" 1 "${val_revenue} (ky vong 13591643.70)"
    else
        write_check "fact_orders: doanh thu hang" 0 "${val_revenue} (ky vong 13591643.70)"
    fi
    
    # So sánh freight
    if [[ "${val_freight}" == "2251909.54" ]]; then
        write_check "fact_orders: cuoc van chuyen" 1 "${val_freight} (ky vong 2251909.54)"
    else
        write_check "fact_orders: cuoc van chuyen" 0 "${val_freight} (ky vong 2251909.54)"
    fi
    
    # So sánh payment
    if [[ "${val_payment}" == "16008872.12" ]]; then
        write_check "fact_orders: tong thanh toan" 1 "${val_payment} (ky vong 16008872.12)"
    else
        write_check "fact_orders: tong thanh toan" 0 "${val_payment} (ky vong 16008872.12)"
    fi
else
    write_check "Truy van Trino" 0 "khong tra ve ket qua"
fi

echo ""
echo -e "\033[1;34m=== 5. PROMETHEUS SCRAPE TARGETS ===\033[0m"
targets_json=$(curl -s --max-time 10 "http://localhost:9090/api/v1/targets" 2>/dev/null || true)
if [[ -n "${targets_json}" ]]; then
    python_parse=$(python3 -c '
import sys, json
try:
    data = json.loads(sys.stdin.read())
    active = data.get("data", {}).get("activeTargets", [])
    total = len(active)
    up = sum(1 for t in active if t.get("health") == "up")
    print(f"{up}/{total}")
except Exception as e:
    print(f"ERR:{e}")
' <<< "${targets_json}" 2>/dev/null || echo "ERR:python_failed")

    if [[ "${python_parse}" =~ ^[0-9]+/[0-9]+$ ]]; then
        up_count="${python_parse%/*}"
        total_count="${python_parse#*/}"
        if [[ "${up_count}" -eq "${total_count}" && "${total_count}" -gt 0 ]]; then
            write_check "Scrape targets UP" 1 "${python_parse}"
        else
            write_check "Scrape targets UP" 0 "${python_parse}"
        fi
    else
        write_check "Prometheus targets" 0 "Loi phan tich API (${python_parse})"
    fi
else
    write_check "Prometheus targets" 0 "khong doc duoc API"
fi

echo ""
if [[ ${FAILURES} -eq 0 ]]; then
    echo -e "\033[1;32mKET QUA: TAT CA KIEM TRA DEU PASS.\033[0m"
    exit 0
else
    echo -e "\033[1;31mKET QUA: ${FAILURES} muc FAIL - xem chi tiet o tren.\033[0m"
    exit 1
fi
