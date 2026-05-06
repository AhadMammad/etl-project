#!/bin/sh
set -e

until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null; do
  echo "Waiting for MinIO..."
  sleep 3
done

mc mb --ignore-existing "local/${MINIO_BUCKET}"
echo "Bucket '${MINIO_BUCKET}' is ready."
