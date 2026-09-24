#!/bin/bash
set -euo pipefail

: "${ICEBERG_DB_PASSWORD:?ICEBERG_DB_PASSWORD is required}"

runtime_defaults="${SPARK_CONF_DIR}/spark-defaults.conf"
umask 077
mkdir -p "${SPARK_CONF_DIR}"
cp /opt/spark/conf/spark-defaults.conf.template "${runtime_defaults}"
printf 'spark.sql.catalog.lakehouse.jdbc.password %s\n' "${ICEBERG_DB_PASSWORD}" >> "${runtime_defaults}"

exec /opt/entrypoint.sh "$@"
