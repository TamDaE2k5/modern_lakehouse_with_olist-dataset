#!/bin/sh
set -eu

mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null
mc mb --ignore-existing local/raw local/lakehouse
if [ "${MINIO_ROOT_USER}" = "${MINIO_UPLOAD_USER}" ] || \
   [ "${MINIO_ROOT_USER}" = "${MINIO_PROCESSING_USER}" ] || \
   [ "${MINIO_ROOT_USER}" = "${MINIO_QUERY_USER}" ] || \
   [ "${MINIO_UPLOAD_USER}" = "${MINIO_PROCESSING_USER}" ] || \
   [ "${MINIO_UPLOAD_USER}" = "${MINIO_QUERY_USER}" ] || \
   [ "${MINIO_PROCESSING_USER}" = "${MINIO_QUERY_USER}" ]; then
  echo "MinIO admin and scoped user names must all be distinct" >&2
  exit 1
fi
if [ "${MINIO_ROOT_PASSWORD}" = "${MINIO_UPLOAD_PASSWORD}" ] || \
   [ "${MINIO_ROOT_PASSWORD}" = "${MINIO_PROCESSING_PASSWORD}" ] || \
   [ "${MINIO_ROOT_PASSWORD}" = "${MINIO_QUERY_PASSWORD}" ] || \
   [ "${MINIO_UPLOAD_PASSWORD}" = "${MINIO_PROCESSING_PASSWORD}" ] || \
   [ "${MINIO_UPLOAD_PASSWORD}" = "${MINIO_QUERY_PASSWORD}" ] || \
   [ "${MINIO_PROCESSING_PASSWORD}" = "${MINIO_QUERY_PASSWORD}" ]; then
  echo "MinIO admin and scoped passwords must all be distinct" >&2
  exit 1
fi
for policy in upload processing query; do
  mc admin policy create local "olist-${policy}" "/policies/${policy}.json"
done
mc admin user add local "${MINIO_UPLOAD_USER}" "${MINIO_UPLOAD_PASSWORD}"
mc admin user add local "${MINIO_PROCESSING_USER}" "${MINIO_PROCESSING_PASSWORD}"
mc admin user add local "${MINIO_QUERY_USER}" "${MINIO_QUERY_PASSWORD}"
mc admin policy attach local olist-upload --user "${MINIO_UPLOAD_USER}"
mc admin policy attach local olist-processing --user "${MINIO_PROCESSING_USER}"
mc admin policy attach local olist-query --user "${MINIO_QUERY_USER}"
mc ls local
