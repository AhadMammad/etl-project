#!/usr/bin/env python3
"""
Provisions a NiFi ETL flow via the NiFi 1.x REST API.

Flow:
  QueryDatabaseTable  →  ConvertAvroToParquet  →  PutS3Object (MinIO)

QueryDatabaseTable outputs native Avro FlowFiles — no RecordSetWriterFactory
controller service required. ConvertAvroToParquet needs no CS at all.
Only one controller service: DBCPConnectionPool for MySQL.
"""

import os
import sys
import time
import json
import requests

NIFI_BASE_URL = os.getenv("NIFI_BASE_URL", "http://nifi:8080/nifi-api")
HEADERS = {"Content-Type": "application/json"}

MYSQL_HOST  = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT  = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB    = os.getenv("MYSQL_DATABASE", "etl_db")
MYSQL_USER  = os.getenv("MYSQL_USER", "etl_user")
MYSQL_PASS  = os.getenv("MYSQL_PASSWORD", "etl_pass")

MINIO_HOST   = os.getenv("MINIO_HOST", "minio")
MINIO_PORT   = os.getenv("MINIO_PORT", "9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "etl-data")
MINIO_USER   = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS   = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

JDBC_JAR    = "/opt/nifi/nifi-current/lib/custom/mysql-connector-j-8.0.33.jar"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
JDBC_URL    = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true"
)


# ── Tables to ingest ──────────────────────────────────────────────────────────
# Each entry creates one QueryDatabaseTable processor. They all fan in to the
# shared ConvertAvroToParquet → StripDotAttributes → PutS3Object chain, so
# adding a table is a one-line change.
#
#   name           MySQL table name
#   max_value_col  column QDT tracks for incremental loads — typically a
#                  monotonic PK (id) or an updated_at timestamp
#   schedule       optional cron/timer string (default "60 sec")
TABLES = [
    {"name": "employees", "max_value_col": "id"},
    {"name": "orders",    "max_value_col": "id"},
    {"name": "products",  "max_value_col": "id"},
]


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def get(path):
    r = requests.get(f"{NIFI_BASE_URL}{path}", headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def post(path, body):
    r = requests.post(f"{NIFI_BASE_URL}{path}", headers=HEADERS,
                      data=json.dumps(body), timeout=30)
    r.raise_for_status()
    return r.json()


def put(path, body):
    r = requests.put(f"{NIFI_BASE_URL}{path}", headers=HEADERS,
                     data=json.dumps(body), timeout=30)
    r.raise_for_status()
    return r.json()


# ── Startup wait ──────────────────────────────────────────────────────────────

def wait_for_nifi(max_wait=300):
    print("Waiting for NiFi to be ready", end="", flush=True)
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            r = requests.get(f"{NIFI_BASE_URL}/system-diagnostics", timeout=5)
            if r.status_code == 200:
                print(" ready!")
                return
        except requests.exceptions.ConnectionError:
            pass
        print(".", end="", flush=True)
        time.sleep(5)
    print()
    sys.exit("ERROR: NiFi did not become ready in time.")


# ── Controller services ───────────────────────────────────────────────────────

def create_controller_service(pg_id, cs_type, name, properties=None):
    body = {
        "revision": {"version": 0},
        "component": {
            "type": cs_type,
            "name": name,
            "properties": properties or {}
        }
    }
    resp = post(f"/process-groups/{pg_id}/controller-services", body)
    cs_id = resp["id"]
    print(f"  Created {name}: {cs_id}")
    return cs_id


def enable_controller_service(cs_id):
    cs_data = get(f"/controller-services/{cs_id}")
    body = {"revision": cs_data["revision"], "state": "ENABLED"}
    put(f"/controller-services/{cs_id}/run-status", body)

    deadline = time.time() + 60
    while time.time() < deadline:
        data  = get(f"/controller-services/{cs_id}")
        state = data["component"]["state"]
        if state == "ENABLED":
            print(f"  Enabled: {cs_id}")
            return
        if state == "INVALID":
            errors = data["component"].get("validationErrors", [])
            raise RuntimeError(f"Controller service {cs_id} is INVALID: {errors}")
        time.sleep(2)
    raise TimeoutError(f"Controller service {cs_id} never reached ENABLED.")


# ── Processors ────────────────────────────────────────────────────────────────

def create_processor(pg_id, proc_type, name, position,
                     auto_terminate=None, scheduling_period="0 sec"):
    """Create a processor with no properties — set them via update_processor."""
    body = {
        "revision": {"version": 0},
        "component": {
            "type": proc_type,
            "name": name,
            "position": position,
            "config": {
                "schedulingStrategy": "TIMER_DRIVEN",
                "schedulingPeriod": scheduling_period,
                "autoTerminatedRelationships": auto_terminate or [],
            }
        }
    }
    resp = post(f"/process-groups/{pg_id}/processors", body)
    print(f"  Created {name}: {resp['id']}")
    return resp["id"]


def update_processor(proc_id, properties, retries=5, delay=3):
    """
    Set processor properties via PUT and verify they were saved.
    Sends only id + properties to avoid read-only fields rejecting the request.
    Retries because NiFi can take a moment to accept a CS reference after enabling.
    """
    for attempt in range(1, retries + 1):
        proc_data = get(f"/processors/{proc_id}")
        revision  = proc_data["revision"]

        body = {
            "revision": revision,
            "component": {
                "id": proc_id,
                "config": {"properties": properties}
            }
        }
        put(f"/processors/{proc_id}", body)

        saved     = get(f"/processors/{proc_id}")["component"]["config"].get("properties", {})
        # NiFi masks sensitive properties as '********' in GET responses — treat as saved.
        all_saved = all(saved.get(k) in (v, "********") for k, v in properties.items())
        if all_saved:
            print(f"  Properties saved on {proc_id}: {list(properties.keys())}")
            return

        print(f"  [{attempt}/{retries}] not yet saved, retrying in {delay}s...")
        time.sleep(delay)

    raise RuntimeError(
        f"Could not save properties on {proc_id} after {retries} attempts.\n"
        f"  Keys attempted: {list(properties.keys())}\n"
        f"  Last saved values: { {k: saved.get(k) for k in properties} }"
    )


def start_processor(proc_id):
    revision = get(f"/processors/{proc_id}")["revision"]
    put(f"/processors/{proc_id}/run-status", {"revision": revision, "state": "RUNNING"})
    print(f"  Started: {proc_id}")


# ── Cleanup ───────────────────────────────────────────────────────────────────

def cleanup_flow(pg_id):
    """Stop and delete all processors and connections in the process group."""
    # Stop all processors first
    processors = get(f"/process-groups/{pg_id}/processors").get("processors", [])
    for p in processors:
        pid = p["id"]
        rev = p["revision"]
        try:
            put(f"/processors/{pid}/run-status", {"revision": rev, "state": "STOPPED"})
        except Exception:
            pass

    time.sleep(2)

    # Delete all connections (must be empty / have no queued data)
    connections = get(f"/process-groups/{pg_id}/connections").get("connections", [])
    for c in connections:
        cid = c["id"]
        rev = c["revision"]
        # Purge any queued flowfiles so the connection can be deleted
        try:
            drop = post(f"/connections/{cid}/drop-requests", {})
            req_id = drop.get("dropRequest", {}).get("id")
            if req_id:
                deadline = time.time() + 30
                while time.time() < deadline:
                    status = get(f"/connections/{cid}/drop-requests/{req_id}")
                    if status.get("dropRequest", {}).get("finished"):
                        break
                    time.sleep(1)
        except Exception:
            pass
        try:
            requests.delete(
                f"{NIFI_BASE_URL}/connections/{cid}",
                headers=HEADERS,
                params={"version": rev["version"]},
                timeout=30
            ).raise_for_status()
        except Exception as e:
            print(f"  Warning: could not delete connection {cid}: {e}")

    # Delete all processors
    for p in processors:
        pid = p["id"]
        rev = p["revision"]
        try:
            requests.delete(
                f"{NIFI_BASE_URL}/processors/{pid}",
                headers=HEADERS,
                params={"version": rev["version"]},
                timeout=30
            ).raise_for_status()
            print(f"  Deleted processor {pid}")
        except Exception as e:
            print(f"  Warning: could not delete processor {pid}: {e}")

    # Delete all controller services
    services = get(f"/flow/process-groups/{pg_id}/controller-services").get("controllerServices", [])
    for cs in services:
        csid = cs["id"]
        rev  = cs["revision"]
        try:
            put(f"/controller-services/{csid}/run-status", {"revision": rev, "state": "DISABLED"})
            time.sleep(1)
            rev = get(f"/controller-services/{csid}")["revision"]
            requests.delete(
                f"{NIFI_BASE_URL}/controller-services/{csid}",
                headers=HEADERS,
                params={"version": rev["version"]},
                timeout=30
            ).raise_for_status()
            print(f"  Deleted controller service {csid}")
        except Exception as e:
            print(f"  Warning: could not delete controller service {csid}: {e}")


# ── Connections ───────────────────────────────────────────────────────────────

def create_connection(pg_id, src_id, relationship, dst_id):
    body = {
        "revision": {"version": 0},
        "component": {
            "source":      {"id": src_id, "groupId": pg_id, "type": "PROCESSOR"},
            "destination": {"id": dst_id, "groupId": pg_id, "type": "PROCESSOR"},
            "selectedRelationships": [relationship],
            "backPressureDataSizeThreshold": "1 GB",
            "backPressureObjectThreshold": "10000"
        }
    }
    post(f"/process-groups/{pg_id}/connections", body)
    print(f"  Connected {src_id} --[{relationship}]--> {dst_id}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("NiFi ETL Flow Setup")
    print("=" * 60)

    wait_for_nifi()

    print("\n[1] Getting root process group...")
    pg_id = get("/process-groups/root")["id"]
    print(f"  Root PG: {pg_id}")

    print("\n[1b] Cleaning up existing flow components...")
    cleanup_flow(pg_id)
    time.sleep(2)

    # ── Controller services ───────────────────────────────────────────────────
    print("\n[2] Creating controller services...")
    dbcp_id = create_controller_service(
        pg_id,
        "org.apache.nifi.dbcp.DBCPConnectionPool",
        "MySQL DBCPConnectionPool",
        {
            "Database Connection URL":    JDBC_URL,
            "Database Driver Class Name": JDBC_DRIVER,
            "database-driver-locations":  JDBC_JAR,
            "Database User":              MYSQL_USER,
            "Password":                   MYSQL_PASS,
            "Validation query":           "SELECT 1"
        }
    )

    print("\n[3] Enabling controller services...")
    time.sleep(3)
    enable_controller_service(dbcp_id)
    time.sleep(2)

    # ── Shared chain ──────────────────────────────────────────────────────────
    # All QueryDatabaseTable processors fan into this single converter→strip→S3
    # chain. Per-table prefixing is done via the ${tablename} attribute that
    # QueryDatabaseTable adds to every emitted FlowFile.
    print("\n[4] Creating shared converter / strip / S3 processors...")

    conv_id = create_processor(
        pg_id,
        "org.apache.nifi.processors.parquet.ConvertAvroToParquet",
        "ConvertAvroToParquet",
        {"x": 600.0, "y": 200.0},
        auto_terminate=["failure"]
    )

    ua_id = create_processor(
        pg_id,
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        "StripDotAttributes",
        {"x": 900.0, "y": 200.0}
    )

    s3_id = create_processor(
        pg_id,
        "org.apache.nifi.processors.aws.s3.PutS3Object",
        "PutS3Object → MinIO",
        {"x": 1200.0, "y": 200.0},
        auto_terminate=["failure", "success"]
    )

    print("\n[5] Setting shared processor properties...")
    update_processor(ua_id, {
        "Delete Attributes Expression": r"mime\.type|avro\.schema|record\.count|hive\.ddl"
    })

    # PutS3Object: property *keys* must match NiFi's canonical names — and the
    # convention is inconsistent. Some are display-name-as-key ("Signer Override",
    # "Bucket"), others are kebab-case ("use-path-style-access",
    # "use-chunked-encoding"). Setting the wrong form is silently accepted as a
    # dynamic property and has no effect.
    #
    # MinIO rejects the AWS SDK's default V4-with-chunked-encoding requests
    # (streaming SHA-256 header) with "400 invalid header name". To avoid that:
    #   - Signer Override = S3SignerType  → V2 auth, fewer headers
    #   - use-chunked-encoding = false    → disable aws-chunked transfer encoding
    #   - use-path-style-access = true    → http://host:port/bucket/key style
    update_processor(s3_id, {
        "Bucket":                  MINIO_BUCKET,
        # ${tablename} is set by QueryDatabaseTable on every FlowFile,
        # giving each table its own prefix in the bucket.
        "Object Key":              (
            "${tablename}/${now():format('yyyy/MM/dd')}"
            "/${tablename}_${now():format('HHmmss')}.parquet"
        ),
        "Region":                  "us-east-1",
        "Access Key":              MINIO_USER,
        "Secret Key":              MINIO_PASS,
        "Endpoint Override URL":   f"http://{MINIO_HOST}:{MINIO_PORT}",
        "use-path-style-access":   "true",
        "Signer Override":         "S3SignerType",
        "use-chunked-encoding":    "false",
        "Multipart Threshold":     "5 GB",
        "Multipart Part Size":     "5 GB"
    })

    print("\n[6] Wiring shared chain (Convert → Strip → S3)...")
    create_connection(pg_id, conv_id, "success", ua_id)
    create_connection(pg_id, ua_id,   "success", s3_id)

    # ── Per-table QueryDatabaseTable processors ───────────────────────────────
    print(f"\n[7] Creating {len(TABLES)} QueryDatabaseTable processor(s)...")
    qdt_ids = []
    for i, tbl in enumerate(TABLES):
        qdt_id = create_processor(
            pg_id,
            "org.apache.nifi.processors.standard.QueryDatabaseTable",
            f"QueryDatabaseTable[{tbl['name']}]",
            {"x": 200.0, "y": 100.0 + i * 180.0},
            scheduling_period=tbl.get("schedule", "60 sec")
        )
        update_processor(qdt_id, {
            "Database Connection Pooling Service": dbcp_id,
            "db-fetch-db-type":                    "MySQL",
            "Table Name":                          tbl["name"],
            "Maximum-value Columns":               tbl["max_value_col"],
            "fetch-size":                          "1000"
        })
        create_connection(pg_id, qdt_id, "success", conv_id)
        qdt_ids.append(qdt_id)

    # ── Start ─────────────────────────────────────────────────────────────────
    print("\n[8] Starting processors...")
    start_processor(conv_id)
    start_processor(ua_id)
    start_processor(s3_id)
    for qdt_id in qdt_ids:
        start_processor(qdt_id)

    print("\n" + "=" * 60)
    print("ETL flow is running!")
    print(f"  NiFi UI:   http://localhost:8080/nifi")
    print(f"  MinIO UI:  http://localhost:9001  (minioadmin / minioadmin)")
    print(f"  Bucket:    {MINIO_BUCKET}")
    print(f"  Tables:    {', '.join(t['name'] for t in TABLES)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
