#!/bin/bash
set -euo pipefail

timeout_seconds="${TRINO_PUBLISH_TIMEOUT_SECONDS:-120}"
case "$timeout_seconds" in
  ''|*[!0-9]*) echo "TRINO_PUBLISH_TIMEOUT_SECONDS phải là số nguyên dương" >&2; exit 2 ;;
esac
if [ "$timeout_seconds" -lt 1 ]; then
  echo "TRINO_PUBLISH_TIMEOUT_SECONDS phải lớn hơn 0" >&2
  exit 2
fi

if [ "${1:-}" != "--inner" ]; then
  set +e
  /usr/bin/timeout --signal=TERM "${timeout_seconds}s" /bin/bash "$0" --inner
  status=$?
  set -e
  if [ "$status" -eq 124 ]; then
    echo "Publisher semantic view đã hết thời gian sau ${timeout_seconds} giây" >&2
  fi
  exit "$status"
fi

log_file=/tmp/semantic-view-publisher.log
/usr/lib/trino/bin/launcher run --etc-dir /etc/trino -Dnode.id="semantic-${HOSTNAME}" >"$log_file" 2>&1 &
trino_pid=$!
cleanup() {
  kill "$trino_pid" 2>/dev/null || true
  wait "$trino_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

deadline=$((SECONDS + timeout_seconds))
until trino --server http://127.0.0.1:8080 --execute 'SELECT 1' >/dev/null 2>&1; do
  if ! kill -0 "$trino_pid" 2>/dev/null; then
    sed -n '1,200p' "$log_file" >&2
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "Trino publisher chưa sẵn sàng trong ${timeout_seconds} giây" >&2
    exit 124
  fi
  sleep 2
done

trino --server http://127.0.0.1:8080 --file /opt/lakehouse/sql/trino/create_views.sql
actual="$(trino --server http://127.0.0.1:8080 --output-format TSV --execute \
  "SELECT table_name FROM lakehouse.information_schema.views WHERE table_schema='gold' AND table_name LIKE 'vw_%' ORDER BY table_name")"
expected="$(printf '%s\n' vw_order_lines vw_order_summary vw_payment_summary vw_review_summary)"
if [ "$actual" != "$expected" ]; then
  echo "Tập semantic view sau publish không đúng contract" >&2
  exit 1
fi
for view in vw_order_lines vw_order_summary vw_payment_summary vw_review_summary; do
  trino --server http://127.0.0.1:8080 --execute "SELECT * FROM lakehouse.gold.${view} WHERE false" >/dev/null
done
echo "Đã publish và xác minh 4 semantic view."
