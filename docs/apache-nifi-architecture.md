# Apache NiFi Architecture

---

## Table of Contents

1. [What is Apache NiFi?](#1-what-is-apache-nifi)
2. [Core Concepts (The Mental Model)](#2-core-concepts-the-mental-model)
3. [Internal Architecture Deep Dive](#3-internal-architecture-deep-dive)
4. [The FlowFile Lifecycle](#4-the-flowfile-lifecycle)
5. [Threading & Scheduling Model](#5-threading--scheduling-model)
6. [Clustering Architecture](#6-clustering-architecture)
7. [Data Provenance & Lineage](#7-data-provenance--lineage)
8. [Security Architecture](#8-security-architecture)
9. [Back-Pressure & Flow Control](#9-back-pressure--flow-control)
10. [NiFi Registry — Version Control for Flows](#10-nifi-registry--version-control-for-flows)
11. [Best Use Cases & Project Types](#11-best-use-cases--project-types)

---

## 1. What is Apache NiFi?

Apache NiFi is a **data flow automation platform**. Think of it as a visual plumbing system for data — you connect pipes (processors) together on a canvas, data flows through those pipes, and NiFi handles all the hard parts: retries, error routing, buffering, and auditability.

It was originally built by the NSA (called "NiagaraFiles"), open-sourced in 2014, and donated to Apache. This origin explains why it has **unusually strong security, auditing, and provenance** capabilities compared to other ETL tools.

**One-line summary:** NiFi moves data from A to B, reliably, visually, with full audit trails.

---

## 2. Core Concepts (The Mental Model)

Before looking at code or architecture diagrams, lock in these six concepts. Everything else in NiFi builds on them.

```mermaid
mindmap
  root((Apache NiFi))
    FlowFile
      Content - the actual data bytes
      Attributes - metadata key/value pairs
    Processor
      Reads FlowFiles
      Transforms FlowFiles
      Writes FlowFiles
    Connection
      Queue between processors
      Has back-pressure settings
    Process Group
      Logical grouping of processors
      Can be nested
    Controller Service
      Shared resource - DB pool, SSL context
      Reused across processors
    Reporting Task
      Background task
      Sends metrics to monitoring systems
```

### FlowFile — The Unit of Data

A **FlowFile** is NiFi's fundamental data object. It has two parts:

| Part | What it is | Example |
| --- | --- | --- |
| **Content** | The actual bytes of data | A JSON string, a CSV row, a PDF file |
| **Attributes** | Key-value metadata map | `filename=orders.csv`, `source=kafka`, `size=1024` |

The content is stored on disk (in the Content Repository). The attributes live in memory. This design means NiFi can handle **very large files** without running out of RAM.

### Processor — The Worker

A Processor does one job. Examples:

- `GetFile` — reads files from a directory
- `ConvertRecord` — transforms CSV to JSON
- `PutDatabaseRecord` — writes to a database
- `PublishKafka` — sends to a Kafka topic

Each processor has **relationships** (like exit paths): `success`, `failure`, `retry`. You route FlowFiles down different paths based on these outcomes.

### Connection — The Queue

A Connection is a queue that sits between two processors. It:

- Buffers FlowFiles when the downstream processor is busy
- Enforces back-pressure (stops upstream when full)
- Shows you queue depth in real-time on the canvas

---

## 3. Internal Architecture Deep Dive

NiFi's internal runtime has five major components:

```mermaid
graph TB
    subgraph NiFi JVM Process
        WS[Web Server<br/>REST API + UI]
        FE[Flow Engine<br/>Scheduler + Thread Pools]
        
        subgraph Repositories
            FR[FlowFile Repository<br/>WAL - tracks active FlowFiles]
            CR[Content Repository<br/>Actual data bytes on disk]
            PR[Provenance Repository<br/>Full audit event log]
        end
        
        subgraph Flow Components
            PG[Process Groups]
            PROC[Processors]
            CONN[Connections / Queues]
            CS[Controller Services]
            RT[Reporting Tasks]
        end
    end

    WS --> FE
    FE --> PROC
    PROC --> CONN
    CONN --> PROC
    FE --> FR
    PROC --> CR
    PROC --> PR
    CS --> PROC
    RT --> PR
```

### Web Server

- Hosts the drag-and-drop canvas UI (Angular app)
- Exposes the REST API on port `8080` (HTTP) or `8443` (HTTPS)
- All UI actions go through this REST API — no magic hidden protocol

### Flow Engine

The brain of NiFi. It:

- Maintains the in-memory representation of your flow
- Schedules processors according to their timer/cron settings
- Manages thread pools (one per Process Group by default)

### FlowFile Repository (Write-Ahead Log)

Every active FlowFile gets a record in this WAL (Write-Ahead Log). This is how NiFi survives crashes — on restart, it reads the WAL and knows exactly which FlowFiles were in flight.

The WAL uses an efficient binary format and is stored at `$NIFI_HOME/flowfile_repository/`.

### Content Repository

The actual bytes of FlowFile content live here, stored as flat files. NiFi uses a technique called **content claims** — multiple FlowFiles can point to the same content block (useful after a fork/copy). Content is only deleted when no FlowFile references it anymore (reference counting).

### Provenance Repository

Every single thing that happens to a FlowFile (created, modified, routed, dropped, sent) is written here as an immutable event. This enables full data lineage — you can replay the history of any byte that passed through NiFi.

---

## 4. The FlowFile Lifecycle

This is what happens to data from the moment it enters NiFi to when it exits:

```mermaid
sequenceDiagram
    participant Source as External Source<br/>(file / kafka / HTTP)
    participant Ingest as Ingest Processor<br/>(GetFile / ConsumeKafka)
    participant FR as FlowFile Repository<br/>(WAL)
    participant CR as Content Repository<br/>(disk)
    participant Q as Connection Queue
    participant Transform as Transform Processor<br/>(JoltTransform / ConvertRecord)
    participant PR as Provenance Repository
    participant Sink as Sink Processor<br/>(PutS3 / PutDatabaseRecord)

    Source->>Ingest: raw data arrives
    Ingest->>CR: write content bytes to disk
    Ingest->>FR: register new FlowFile in WAL
    Ingest->>PR: log RECEIVE event
    Ingest->>Q: enqueue FlowFile

    Q->>Transform: dequeue when thread available
    Transform->>CR: read content
    Transform->>CR: write new content (transformed)
    Transform->>FR: update FlowFile record
    Transform->>PR: log CONTENT_MODIFIED event
    Transform->>Q: enqueue to next connection

    Q->>Sink: dequeue
    Sink->>Source: write to destination
    Sink->>PR: log SEND event
    Sink->>FR: remove FlowFile from WAL
    Sink->>CR: release content claim (GC eligible)
```

Key insight: **the FlowFile record in the WAL is only removed after the data is successfully delivered to the destination.** This is what gives NiFi its at-least-once delivery guarantee.

---

## 5. Threading & Scheduling Model

NiFi uses a thread pool model. Understanding this prevents the most common performance mistakes.

```mermaid
graph LR
    subgraph Process Group A
        TP_A[Thread Pool<br/>default: 10 threads]
        P1[GetFile<br/>1 concurrent task]
        P2[JoltTransform<br/>4 concurrent tasks]
        P3[PutS3<br/>2 concurrent tasks]
        TP_A --> P1
        TP_A --> P2
        TP_A --> P3
    end

    subgraph Process Group B - Isolated
        TP_B[Thread Pool<br/>custom: 20 threads]
        P4[ConsumeKafka<br/>6 concurrent tasks]
        P5[ConvertRecord<br/>8 concurrent tasks]
        TP_B --> P4
        TP_B --> P5
    end
```

### Key Scheduling Concepts

| Setting | What it controls |
| --- | --- |
| **Concurrent Tasks** | How many threads can run this processor at the same time |
| **Run Schedule** | Timer interval (e.g., every 1s) OR cron expression |
| **Run Duration** | How long one task can hold a thread before yielding |
| **Yield Duration** | How long a processor waits after doing no work |

**Common mistake:** Setting Concurrent Tasks too high on a database sink creates connection pool exhaustion. Always match Concurrent Tasks to your downstream system's capacity.

---

## 6. Clustering Architecture

NiFi clusters are **masterless** — every node runs the same flow and can process data. A lightweight coordinator role (not a full master) handles cluster-wide decisions.

```mermaid
graph TB
    subgraph ZooKeeper Ensemble
        ZK1[ZooKeeper Node 1]
        ZK2[ZooKeeper Node 2]
        ZK3[ZooKeeper Node 3]
    end

    subgraph NiFi Cluster
        subgraph Node 1 - Primary + Coordinator
            N1[NiFi Node 1]
            N1_UI[Serves UI]
        end
        subgraph Node 2
            N2[NiFi Node 2]
        end
        subgraph Node 3
            N3[NiFi Node 3]
        end
    end

    LB[Load Balancer<br/>Nginx / AWS ALB]
    Client[Browser / API Client]

    Client --> LB
    LB --> N1_UI
    N1 <--> ZK1
    N2 <--> ZK1
    N3 <--> ZK1
    ZK1 <--> ZK2
    ZK2 <--> ZK3

    N1 <-->|Heartbeat + Site-to-Site| N2
    N2 <-->|Heartbeat + Site-to-Site| N3

    style N1 fill:#f96,stroke:#333
```

### Cluster Roles

| Role | Description | How many |
| --- | --- | --- |
| **Coordinator** | Routes requests, manages cluster state | 1 (elected by ZooKeeper) |
| **Primary Node** | Runs processors marked "primary only" (e.g., once-per-cluster tasks) | 1 (elected by ZooKeeper) |
| **Worker Node** | Processes data — every node is this | All nodes |

### Site-to-Site (S2S) Protocol

NiFi has a built-in data transfer protocol called **Site-to-Site** for moving data between NiFi instances. Unlike Kafka or HTTP, S2S:

- Is NiFi-aware (respects back-pressure end-to-end)
- Supports both push and pull modes
- Uses efficient binary compression
- Works through firewalls (pull mode)

```mermaid
graph LR
    subgraph Edge NiFi - Factory Floor
        E1[Collect Sensor Data]
        E2[Light Filtering]
        E3[Remote Process Group<br/>S2S Push]
    end
    subgraph Central NiFi - Data Center
        C1[Remote Process Group<br/>S2S Receive]
        C2[Enrich + Transform]
        C3[Write to Data Lake]
    end

    E1 --> E2 --> E3
    E3 -->|Site-to-Site| C1
    C1 --> C2 --> C3
```

---

## 7. Data Provenance & Lineage

Provenance is NiFi's **superpower**. Every FlowFile event is recorded: where data came from, what happened to it, where it went.

```mermaid
timeline
    title FlowFile Provenance Events (single record)
    RECEIVE : Data received from Kafka topic orders
    FORK    : Split into individual order records
    CONTENT_MODIFIED : JSON transformed to internal schema
    ATTRIBUTES_MODIFIED : Added region=EU attribute
    SEND    : Written to S3 bucket s3://datalake/orders/
    DROP    : Original parent FlowFile discarded after fork
```

### What You Can Do With Provenance

- **Replay any FlowFile** from any point in its history (re-sends the exact bytes to a processor)
- **Search** for FlowFiles by attribute (`filename contains orders_2024`)
- **Audit** who changed the flow and when (via NiFi Registry)
- **Debug** why a record ended up malformed — step through the transformation chain

---

## 8. Security Architecture

NiFi was built security-first. The full security stack:

```mermaid
graph TD
    subgraph Transport Security
        TLS[TLS 1.2/1.3<br/>All node-to-node and client-to-node traffic]
    end

    subgraph Authentication - pick one
        CERT[Client Certificates<br/>mTLS]
        LDAP[LDAP / Active Directory]
        OIDC[OpenID Connect<br/>Okta, Keycloak, Azure AD]
        KNOX[Apache Knox SSO]
    end

    subgraph Authorization
        POLICIES[Resource-level Policies<br/>per processor, per connection]
        ROLES[User Groups + Roles]
    end

    subgraph Sensitive Data
        KS[Keystore<br/>private keys for TLS]
        TS[Truststore<br/>trusted CA certs]
        SENS[Sensitive Properties<br/>encrypted at rest with master key]
    end

    TLS --> CERT
    TLS --> LDAP
    TLS --> OIDC
    CERT --> POLICIES
    LDAP --> POLICIES
    OIDC --> POLICIES
    POLICIES --> ROLES
```

Sensitive values in processor configs (passwords, API keys) are **AES-256 encrypted** using a master key derived from `nifi.sensitive.props.key` in `nifi.properties`. They are never stored in plaintext.

---

## 9. Back-Pressure & Flow Control

Back-pressure prevents NiFi from running out of disk or memory when a downstream system is slow. It's configured on every Connection.

```mermaid
graph LR
    P1[Fast Producer<br/>10,000 records/sec]
    Q1{Connection Queue<br/>⚠️ Back-pressure threshold<br/>10,000 objects OR 1 GB}
    P2[Slow Consumer<br/>1,000 records/sec]

    P1 -->|enqueue| Q1
    Q1 -->|dequeue| P2

    Q1 -->|threshold hit!<br/>P1 is PENALIZED<br/>stops scheduling| P1
```

### Back-Pressure Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| **Back Pressure Object Threshold** | 10,000 | Stop upstream when queue has this many FlowFiles |
| **Back Pressure Data Size Threshold** | 1 GB | Stop upstream when queue holds this much data |
| **Load Balance Strategy** | Do not load balance | How to distribute FlowFiles across cluster nodes |

When back-pressure kicks in, the upstream processor is simply not scheduled. No errors, no data loss — it just waits. This propagates upstream naturally, creating **organic flow control** across the entire pipeline.

---

## 10. NiFi Registry — Version Control for Flows

NiFi Registry is a **separate companion service** that gives your NiFi flows what code has always had: version control, change history, and the ability to promote flows across environments (dev → staging → prod).

Think of it like **Git, but for NiFi canvas flows**.

Without Registry, if you accidentally delete a processor or a teammate changes a flow that breaks production, there is no undo. With Registry, every save is a versioned snapshot you can diff, rollback, or promote.

---

### The Problem Registry Solves

```mermaid
graph LR
    subgraph Without Registry
        D1[Developer changes flow on canvas]
        D2[Breaks production]
        D3[No history, no rollback]
        D1 --> D2 --> D3
    end

    subgraph With Registry
        R1[Developer saves flow as v1]
        R2[Makes changes - saves as v2]
        R3[v2 breaks prod]
        R4[One-click rollback to v1]
        R1 --> R2 --> R3 --> R4
    end
```

---

### How Registry Fits Into the Architecture

Registry is a **completely separate process** — its own JVM, its own port (default `18080`), its own storage. NiFi connects to it like a client connects to a server.

```mermaid
graph TB
    subgraph NiFi Registry Service - port 18080
        REG_API[REST API]
        REG_DB[(Registry Database<br/>H2 / PostgreSQL<br/>stores flow snapshots)]
        REG_FS[Flow Storage<br/>JSON snapshots on disk<br/>or in Git]
        REG_API --> REG_DB
        REG_API --> REG_FS
    end

    subgraph NiFi Cluster
        N1[NiFi Node 1]
        N2[NiFi Node 2]
        N3[NiFi Node 3]
    end

    subgraph Environments
        DEV[Dev NiFi]
        STAGING[Staging NiFi]
        PROD[Prod NiFi]
    end

    N1 -->|save version| REG_API
    N2 -->|pull version| REG_API
    DEV -->|commit v3| REG_API
    STAGING -->|import v3| REG_API
    PROD -->|import v3 after approval| REG_API
```

---

### Core Concepts of Registry

#### Bucket

A **Bucket** is a top-level folder inside Registry. It groups related flows together. Think of it like a repository in GitHub.

Examples:

- `ETL-Pipelines` bucket
- `IoT-Flows` bucket
- `Finance-Ingestion` bucket

Access control is set at the bucket level — some teams can only read, others can write.

#### Versioned Flow

A **Versioned Flow** is a named flow stored inside a bucket. Each time you save it, it creates a new immutable version (v1, v2, v3...).

#### Flow Snapshot

A **Snapshot** is one specific version of a flow — the complete JSON representation of every processor, connection, and configuration at that moment in time. It captures:

- All processors and their properties
- All connections and their back-pressure settings
- Controller Services referenced by the flow
- Parameter Contexts (variables)

Sensitive properties (passwords) are **not stored** in snapshots — they must be set in the target environment.

---

### The Version Control Workflow

```mermaid
sequenceDiagram
    participant Dev as Developer
    participant NiFi as NiFi Canvas
    participant Reg as NiFi Registry
    participant Prod as Production NiFi

    Dev->>NiFi: Right-click Process Group
    Dev->>NiFi: "Start Version Control"
    NiFi->>Reg: Register flow in bucket ETL-Pipelines
    Reg-->>NiFi: Flow saved as v1

    Dev->>NiFi: Modify processors
    Dev->>NiFi: "Save New Version"
    NiFi->>Reg: Save snapshot v2
    Reg-->>NiFi: Confirmed

    Dev->>NiFi: Discovers bug in v2
    Dev->>NiFi: "Change Version" → select v1
    NiFi->>Reg: Fetch snapshot v1
    Reg-->>NiFi: Returns v1 JSON
    NiFi->>NiFi: Reverts canvas to v1 state

    Dev->>Prod: Promote v2 to production
    Prod->>Reg: Import flow from bucket, version v2
    Reg-->>Prod: Returns v2 snapshot
    Prod->>Prod: Apply sensitive props manually
```

---

### Flow States — What the Icons Mean

On the NiFi canvas, a versioned Process Group shows a colored icon in its corner:

| Icon Color | State | Meaning |
| --- | --- | --- |
| **Green** (checkmark) | Up to date | Canvas matches the latest Registry version |
| **Yellow** (pencil) | Locally modified | You made changes not yet saved to Registry |
| **Red** (X) | Stale | Registry has a newer version than what's on canvas |
| **Gray** (question) | Sync failure | Cannot reach Registry |

---

### Parameter Contexts — How Env-Specific Values Work

A common question: *"If Registry stores the flow, how do dev/prod use different database URLs?"*

The answer is **Parameter Contexts**. Instead of hardcoding values in processors, you use parameters:

```text
Processor property: Database URL
Value: #{db_url}          ← parameter reference, not a hardcoded value
```

Each environment has its own Parameter Context with environment-specific values:

```mermaid
graph LR
    subgraph Registry
        FLOW[Flow v3<br/>uses param db_url]
    end

    subgraph Dev NiFi
        PC_DEV[Parameter Context<br/>db_url = jdbc:mysql://dev-db:3306/app]
    end

    subgraph Prod NiFi
        PC_PROD[Parameter Context<br/>db_url = jdbc:mysql://prod-db:3306/app]
    end

    FLOW -->|same snapshot| PC_DEV
    FLOW -->|same snapshot| PC_PROD
```

Same flow snapshot, different runtime values. This is the correct way to promote flows across environments.

---

### Registry Storage Backends

By default Registry stores flow snapshots as JSON files on local disk. In production you should use one of:

| Backend | How to configure | Best for |
| --- | --- | --- |
| **Local filesystem** | Default — `flow_storage_directory` | Development, single-node |
| **Git repository** | `GitFlowPersistenceProvider` in `providers.xml` | Teams using GitOps, full audit trail in Git |
| **Database (PostgreSQL)** | `DatabaseFlowPersistenceProvider` | HA setups, easier backup |

The **Git backend** is especially powerful — every version save becomes a Git commit, giving you `git log`, `git diff`, and integration with GitHub/GitLab PR workflows.

```mermaid
graph LR
    NiFi -->|save v4| Registry
    Registry -->|git commit| GitRepo[(Git Repository<br/>GitHub / GitLab)]
    GitRepo -->|PR review| Reviewer
    Reviewer -->|merge approved| GitRepo
    GitRepo -->|pull| ProdRegistry[Prod Registry]
    ProdRegistry -->|import| ProdNiFi[Prod NiFi]
```

---

### Registry vs Provenance — What's the Difference?

People often confuse these two. They track completely different things:

| | NiFi Registry | Data Provenance |
| --- | --- | --- |
| **Tracks** | Flow design changes (who changed what processor) | Data movement (what happened to each record) |
| **Unit** | Flow version (the canvas blueprint) | FlowFile event (a single data record) |
| **Who uses it** | Data engineers building pipelines | Data stewards auditing data lineage |
| **Stored in** | Registry service (separate process) | Provenance Repository (inside NiFi) |
| **Rollback** | Yes — revert to previous flow version | No — immutable event log |

---

### Quick Setup Checklist

```text
1. Start NiFi Registry service (separate docker container or process)
2. In NiFi UI → Hamburger menu → Controller Settings → Registry Clients
3. Add Registry Client: URL = http://registry-host:18080
4. Right-click any Process Group on canvas → Version → Start Version Control
5. Select bucket, give the flow a name → Save
```

From that point on, the Process Group is version-controlled. Every "Save New Version" is an immutable snapshot you can diff, rollback, or import on any other NiFi instance connected to the same Registry.

---

## 11. Best Use Cases & Project Types

NiFi excels in specific scenarios. Here's where it shines versus where you should use something else.

```mermaid
quadrantChart
    title NiFi vs Other Tools by Use Case
    x-axis Low Data Volume --> High Data Volume
    y-axis Simple Flow --> Complex Routing Logic
    quadrant-1 NiFi + Kafka
    quadrant-2 NiFi Ideal Zone
    quadrant-3 Airflow / Simple Scripts
    quadrant-4 Kafka Streams / Flink
    NiFi ETL Pipelines: [0.45, 0.65]
    IoT Data Collection: [0.35, 0.55]
    CDC Pipelines: [0.55, 0.60]
    Log Aggregation: [0.60, 0.40]
    Security Data Feeds: [0.30, 0.75]
    Data Lake Ingestion: [0.65, 0.50]
    Real-time Enrichment: [0.55, 0.70]
```

### Top Use Cases

#### 1. Data Lake / Data Warehouse Ingestion

**What:** Pull from dozens of source systems (databases, APIs, files, FTP), standardize formats, and land in S3/ADLS/GCS or Snowflake/BigQuery.

**Why NiFi:** Built-in connectors for 300+ systems. Schema Registry integration for format evolution. Back-pressure protects the data lake from spikes.

```mermaid
graph LR
    A[(Oracle DB<br/>CDC via QueryDB)] --> N
    B[REST APIs<br/>GetHTTP] --> N
    C[SFTP Files<br/>FetchSFTP] --> N
    D[Kafka Topics<br/>ConsumeKafka] --> N
    N{NiFi<br/>Transform + Route}
    N --> E[(S3 / ADLS<br/>Parquet / Delta Lake)]
    N --> F[(Snowflake<br/>PutDatabaseRecord)]
```

#### 2. IoT & Edge Data Collection

**What:** Collect sensor data from factory floors, vehicles, or field devices. Light filtering/aggregation at the edge, forward to central systems.

**Why NiFi:** Site-to-Site protocol handles unreliable edge networks gracefully. MiNiFi (lightweight NiFi agent) runs on small devices. Central NiFi pulls from edge agents.

```mermaid
graph LR
    subgraph Edge Devices
        S1[Sensor Array 1<br/>MiNiFi C++]
        S2[Sensor Array 2<br/>MiNiFi C++]
        S3[PLC Gateway<br/>MiNiFi Java]
    end
    subgraph On-Premise
        HUB[NiFi Hub<br/>S2S Receiver]
    end
    subgraph Cloud
        K[Apache Kafka]
        DL[Data Lake]
    end
    S1 -->|S2S| HUB
    S2 -->|S2S| HUB
    S3 -->|S2S| HUB
    HUB --> K --> DL
```

#### 3. Security Operations / SIEM Feeding

**What:** Aggregate logs and security events from firewalls, IDS/IPS, endpoints, and cloud services. Normalize formats (Syslog, CEF, LEEF) and feed a SIEM.

**Why NiFi:** NSA origin means excellent security capabilities. Provenance gives a tamper-evident audit trail. Sensitive data masking processors built-in.

#### 4. Change Data Capture (CDC) Pipelines

**What:** Capture row-level changes from relational databases and stream them to a downstream system in near real-time.

**Why NiFi:** `QueryDatabaseTable` and `CaptureChangeMySQL`/`CaptureChangeMSSQL` processors handle CDC natively. Works well with Debezium as a source.

#### 5. Healthcare / HL7 Data Integration

**What:** Route HL7 messages between hospital systems (EHR, labs, pharmacy). Transform between HL7 v2, FHIR R4, and other formats.

**Why NiFi:** Dedicated HL7 processors. HIPAA-compliant security model. Proven in production at major health systems.

#### 6. Multi-System Data Synchronization

**What:** Keep data in sync across multiple systems — CRM to data warehouse to analytics platform.

**Why NiFi:** Event-driven routing, attribute-based decision making, and retry logic make bidirectional sync manageable.

---

### When NOT to Use NiFi

| Scenario | Better Choice |
| --- | --- |
| Sub-second streaming aggregations | Apache Flink or Kafka Streams |
| Complex batch orchestration with dependencies | Apache Airflow or Prefect |
| Simple one-off file transfers | Shell scripts or cloud-native tools |
| High-throughput pure messaging (millions/sec) | Apache Kafka alone |
| ML feature pipelines | Feast + Spark / dbt |

---

### Most Common Project Types in the Wild

| Project Type | Stack | NiFi's Role |
| --- | --- | --- |
| **Modern Data Platform** | NiFi + Kafka + Spark + Delta Lake | Ingestion layer, raw landing |
| **Operational Data Store** | NiFi + PostgreSQL + Elasticsearch | ETL + search indexing |
| **Real-Time Dashboard** | NiFi + Kafka + ClickHouse + Grafana | Collection + enrichment |
| **Hybrid Cloud Migration** | On-prem NiFi → S2S → Cloud NiFi → S3 | Secure data transfer |
| **API Data Aggregator** | NiFi + Redis + PostgreSQL | Polling + caching layer |
| **Log Management** | NiFi + Elasticsearch + Kibana | Log normalization + indexing |
| **IoT Platform** | MiNiFi + NiFi + InfluxDB + Grafana | Edge-to-cloud pipeline |

---

## Quick Reference: Repository Paths

| Repository | Default Path | Tuning Tip |
| --- | --- | --- |
| FlowFile Repository | `./flowfile_repository` | Put on fast SSD, separate disk from content |
| Content Repository | `./content_repository` | Can span multiple disks with multiple paths |
| Provenance Repository | `./provenance_repository` | Most I/O intensive — dedicate a disk |
| Database Repository | `./database_repository` | Component state storage — small, keep on SSD |

---

*Built with Apache NiFi 1.x / 2.x architecture. Diagrams reflect the open-source release.*
