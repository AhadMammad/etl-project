INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET minio_secret (
    TYPE S3,
    KEY_ID '${MINIO_ROOT_USER}',
    SECRET '${MINIO_ROOT_PASSWORD}',
    ENDPOINT '${MINIO_HOST}:${MINIO_PORT}',
    URL_STYLE 'path',
    USE_SSL false
);
