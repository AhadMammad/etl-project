#!/bin/sh
# Reads Parquet FlowFile content from stdin and uploads to MinIO via mc pipe.
# Environment variables are injected by the NiFi container (docker-compose.yml).
DATE=$(date +%Y/%m/%d)
TIME=$(date +%H%M%S)

mc --config-dir /tmp/.mc alias set local \
    "http://${MINIO_HOST:-minio}:${MINIO_PORT:-9000}" \
    "${MINIO_ROOT_USER:-minioadmin}" \
    "${MINIO_ROOT_PASSWORD:-minioadmin}" >/dev/null 2>&1

exec mc --config-dir /tmp/.mc pipe \
    "local/${MINIO_BUCKET:-etl-data}/employees/${DATE}/employees_${TIME}.parquet"
