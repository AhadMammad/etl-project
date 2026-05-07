FROM debian:bookworm-slim

ARG DUCKDB_VERSION=1.1.3
ARG TARGETARCH

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates curl unzip gettext-base \
 && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
      arm64) DUCK_ARCH=aarch64 ;; \
      amd64) DUCK_ARCH=amd64 ;; \
      *)     DUCK_ARCH=amd64 ;; \
    esac; \
    curl -fsSL "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-${DUCK_ARCH}.zip" -o /tmp/duckdb.zip; \
    unzip /tmp/duckdb.zip -d /usr/local/bin; \
    chmod +x /usr/local/bin/duckdb; \
    rm /tmp/duckdb.zip; \
    duckdb -c "INSTALL httpfs;"

COPY init.sql.tpl /opt/duckdb/init.sql.tpl
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /workspace

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["sleep", "infinity"]
