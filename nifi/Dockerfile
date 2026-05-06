# Copy mc from the local minio image — no internet download needed
FROM minio/minio:ahad AS minio-source

FROM apache/nifi:1.23.2

USER root

RUN mkdir -p /opt/nifi/nifi-current/lib/custom /opt/nifi/scripts

# MySQL JDBC driver
ADD https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar \
    /opt/nifi/nifi-current/lib/custom/mysql-connector-j-8.0.33.jar

# mc client — copied from the local minio image, guaranteed compatible
COPY --from=minio-source /usr/bin/mc /usr/local/bin/mc

# Upload script used by NiFi's ExecuteStreamCommand processor
COPY upload-to-minio.sh /opt/nifi/scripts/upload-to-minio.sh

RUN chown -R nifi:nifi /opt/nifi/nifi-current/lib/custom /opt/nifi/scripts && \
    chmod +x /usr/local/bin/mc /opt/nifi/scripts/upload-to-minio.sh

USER nifi
