#!/bin/sh
set -e

mkdir -p /workspace
envsubst < /opt/duckdb/init.sql.tpl > /workspace/init.sql

exec "$@"
