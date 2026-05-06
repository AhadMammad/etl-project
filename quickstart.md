# ETL Project — Quickstart

A local ETL pipeline: **MySQL → Apache NiFi → Parquet → MinIO**

NiFi reads an `employees` table from MySQL, converts the records to Parquet format, and writes them to a MinIO bucket — all orchestrated with Docker Compose.

---

## Prerequisites

| Requirement | Notes |
| --- | --- |
| Docker + Docker Compose v2 | `docker compose version` |
| `make` | pre-installed on macOS/Linux |
| Local image `minio/minio:ahad` | must exist — will not be pulled |

Verify your MinIO image is available:

```bash
docker images minio/minio
```

---

## Project Structure

```text
etl-project/
├── docker-compose.yml          # orchestrates all services
├── .env                        # credentials and config
├── Makefile                    # all commands
├── mysql/
│   └── init.sql                # employees table + 20 sample rows
├── nifi/
│   └── Dockerfile              # NiFi 1.23.2 + MySQL JDBC driver
├── minio/
│   └── create-bucket.sh        # creates the etl-data bucket
└── scripts/
    └── setup_nifi_flow.py      # provisions NiFi flow via REST API
```

---

## Start the Stack

### Step 1 — Build and start all services

```bash
make up
```

This builds the NiFi image (downloads the MySQL JDBC driver) and starts MySQL, MinIO, and NiFi in detached mode.

> **First run only:** the NiFi image build takes ~2–3 minutes due to the JDBC driver download. Subsequent runs use the Docker layer cache and start in seconds.

### Step 2 — Wait for NiFi to be healthy

NiFi takes ~90 seconds to fully start. Check when it's ready:

```bash
make status
```

Wait until the `etl_nifi` container shows `(healthy)` before proceeding.

### Step 3 — Deploy the ETL flow

```bash
make setup
```

This spins up a one-shot Python container that calls the NiFi REST API to:

- Create 4 controller services (JDBC pool, Avro writer/reader, Parquet writer)
- Create 3 processors (QueryDatabaseTableRecord → ConvertRecord → PutS3Object)
- Wire them together and start the flow

---

## Endpoints

| Service | URL | Credentials |
| --- | --- | --- |
| NiFi Canvas | <http://localhost:8080/nifi> | none |
| MinIO Console | <http://localhost:9001> | `minioadmin` / `minioadmin` |
| MinIO S3 API | <http://localhost:9000> | — |
| MySQL | `localhost:3306` | user: `etl_user` pass: `etl_pass` db: `etl_db` |

---

## Verify the Pipeline

1. Open the **NiFi Canvas** at <http://localhost:8080/nifi> — you should see 3 processors connected and running (green play icons).

2. Open the **MinIO Console** at <http://localhost:9001> and log in. Navigate to the `etl-data` bucket. After the first NiFi run (~60 seconds), Parquet files will appear under:

   ```text
   etl-data/employees/yyyy/MM/dd/employees_HHmmss.parquet
   ```

3. Check MySQL directly:

   ```bash
   docker exec -it etl_mysql mysql -u etl_user -petl_pass etl_db -e "SELECT * FROM employees LIMIT 5;"
   ```

---

## Make Commands

```bash
make help                # print all commands and endpoints
make up                  # build + start all services
make setup               # deploy NiFi ETL flow (run after 'make up')
make status              # check container health
make logs                # tail all logs
make logs SERVICE=nifi   # tail logs for a specific service
make down                # stop containers (volumes + images preserved)
make clean               # remove containers (volumes + images preserved)
make clean-all           # remove containers + volumes (images preserved)
```

> No `make` target ever deletes Docker images.

---

## Configuration

All credentials and settings live in [.env](.env):

```dotenv
MYSQL_ROOT_PASSWORD=root
MYSQL_DATABASE=etl_db
MYSQL_USER=etl_user
MYSQL_PASSWORD=etl_pass
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=etl-data
NIFI_PORT=8080
```

---

## Full Reset

To wipe all data and start fresh (images are kept):

```bash
make clean-all
make up
make setup
```

---

## Troubleshooting

### NiFi setup fails with connection error

NiFi is not ready yet. Run `make status` and wait for `etl_nifi` to show `(healthy)`, then re-run `make setup`.

### `make setup` creates duplicate processors after a restart

The flow is persisted in the `nifi_conf` volume — you only need to run `make setup` once. After a `make down` / `make up` cycle NiFi reloads the saved flow automatically. Only run `make setup` again after a `make clean-all` (which wipes the volume).

### MinIO bucket not found

Check that the `etl_minio_setup` container exited with code `0`:

```bash
docker ps -a --filter name=etl_minio_setup
```

If it failed, run: `make logs SERVICE=minio-setup`

### NiFi processors show errors

Open <http://localhost:8080/nifi>, right-click a processor → **View Status History** or check the NiFi bulletin board (top-right bell icon) for error details.

### MySQL data volume already initialized

If you see no data after `make up`, the volume from a previous run is still mounted. Run `make clean-all` for a full reset.
