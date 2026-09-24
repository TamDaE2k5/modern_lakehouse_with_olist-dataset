#!/usr/bin/env bash
set -euo pipefail

: "${AIRFLOW_ADMIN_PASSWORD:?AIRFLOW_ADMIN_PASSWORD is required}"

mkdir -p "${AIRFLOW_HOME}/logs/spark-events"

airflow db migrate

if airflow users list --output json \
  | python -c 'import json, sys; users = json.load(sys.stdin); raise SystemExit(0 if any(user.get("username") == "admin" for user in users) else 1)'
then
  echo "Airflow admin user already exists"
else
  airflow users create \
    --username admin \
    --firstname Lakehouse \
    --lastname Admin \
    --role Admin \
    --email admin@localhost \
    --password "${AIRFLOW_ADMIN_PASSWORD}"
fi

airflow db check

airflow pools set lakehouse_writes 8 "Serialize lakehouse writers and maintenance"
airflow pools get lakehouse_writes --output json \
  | python /opt/lakehouse/airflow/verify_pool.py
