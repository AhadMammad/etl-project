# Apache NiFi UI Guide

> **Companion to:** [apache-nifi-architecture.md](./apache-nifi-architecture.md)
> **Version:** NiFi 1.x / 2.x UI (same canvas model)

---

## Table of Contents

1. [Canvas Overview](#1-canvas-overview)
2. [Top Toolbar](#2-top-toolbar)
3. [Component Palette (Left Toolbar)](#3-component-palette-left-toolbar)
4. [Processor — Visual Anatomy](#4-processor--visual-anatomy)
5. [Connection — Visual Anatomy](#5-connection--visual-anatomy)
6. [Configuring a Processor](#6-configuring-a-processor)
7. [Process Groups](#7-process-groups)
8. [Controller Services](#8-controller-services)
9. [Queue Inspection & Back-Pressure](#9-queue-inspection--back-pressure)
10. [Data Provenance UI](#10-data-provenance-ui)
11. [Monitoring: Bulletins & Status Bar](#11-monitoring-bulletins--status-bar)
12. [NiFi Registry: Version Control](#12-nifi-registry-version-control)
13. [Keyboard Shortcuts & Power-User Tips](#13-keyboard-shortcuts--power-user-tips)

---

## 1. Canvas Overview

The NiFi canvas is a single-page application that opens at `https://<host>:8443/nifi`. Every element in your data flow lives here.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Apache NiFi                                          [Search...]  [?] [admin▼] │  ← Top Toolbar
├──┬──────────────────────────────────────────────────────────────────────────────┤
│  │                                                                               │
│P │                                                                               │
│A │                         C A N V A S                                          │
│L │                                                                               │
│E │   ┌──────────────────┐        ┌──────────────────┐                           │
│T │   │ ⚙  GetFile       │───────▶│ ⚙  ConvertRecord │                           │
│T │   │ ▶ RUNNING        │        │ ▶ RUNNING        │                           │
│E │   │ In: 0 (0 bytes)  │        │ In: 0 (0 bytes)  │                           │
│  │   │ Out: 12 (48 KB)  │        │ Out: 12 (48 KB)  │                           │
│  │   └──────────────────┘        └──────────────────┘                           │
│  │                                                                               │
│  │   ┌─────────────────────────────────────────────────────┐                   │
│  │   │  Process Group: "Kafka Ingestion"                    │                   │
│  │   │  ▶ 3 processors  ● 2 queued                         │                   │
│  │   └─────────────────────────────────────────────────────┘                   │
│  │                                                                               │
├──┴──────────────────────────────────────────────────────────────────────────────┤
│ NiFi Flow  /  Kafka Ingestion            [●] 5 Active Threads  [■] 0 Errors     │  ← Status Bar
│ Version 3 ✓ (up to date)                Queued: 24 (96 KB)  ⏱ 14:32:01        │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Canvas Zones at a Glance

| Zone | Purpose |
|---|---|
| **Top Toolbar** | Global controls: search, user menu, NiFi Registry sync |
| **Component Palette** (left) | Drag-and-drop source for all component types |
| **Canvas** (center) | Your flow — processors, connections, process groups |
| **Status Bar** (bottom) | Live cluster health, thread counts, queue totals |

---

## 2. Top Toolbar

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                   │
│  [≡] Apache NiFi    [▶ Play All] [■ Stop All] [⟳] [🔒 Lock] │                  │
│                                                                │                  │
│       [🔍 Search processors, parameters, labels...           ]│  [?]  [admin ▼] │
│                                                                                   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Button Reference

```
┌──────────────────────────────────────────────────────────────────────────┐
│  BUTTON        │  ICON   │  WHAT IT DOES                                  │
├──────────────────────────────────────────────────────────────────────────┤
│  Play All      │   ▶     │  Start ALL stopped processors in the flow      │
│  Stop All      │   ■     │  Stop ALL running processors                   │
│  Refresh       │   ⟳     │  Refresh canvas status (auto-refreshes 30s)    │
│  Acquire Lock  │   🔒    │  Prevent other users from editing this flow     │
│  Search        │   🔍    │  Full-text search: names, IDs, attributes       │
│  Help          │   ?     │  Opens NiFi documentation                       │
│  User Menu     │ admin ▼ │  Logout, user info, policies, Registry tokens   │
└──────────────────────────────────────────────────────────────────────────┘
```

### Search Bar — Power Feature

Typing in the search bar is a **live global search** across the entire flow — not just the current view. It matches:

- Processor names and types (`GetFile`, `PutS3`)
- Property values (`jdbc:postgresql://...`)
- Parameter names and values
- Labels and comments
- FlowFile attribute names used in expressions

```
┌───────────────────────────────────────────────────────────┐
│  🔍  postgres                                             │
├───────────────────────────────────────────────────────────┤
│  PROCESSORS (3)                                           │
│  ├── PutDatabaseRecord  [Orders Pipeline > Write Layer]   │
│  ├── QueryDatabaseTable [CDC Group > Poller]              │
│  └── ExecuteSQL         [Reports > Nightly Batch]         │
│  CONTROLLER SERVICES (1)                                  │
│  └── DBCPConnectionPool [Root > postgres-prod]            │
│  PARAMETERS (2)                                           │
│  ├── db.url  = jdbc:postgresql://prod-db:5432/orders      │
│  └── db.user = etl_user                                   │
└───────────────────────────────────────────────────────────┘
```

---

## 3. Component Palette (Left Toolbar)

The left sidebar is your **drag-and-drop palette**. Click any icon and drag it onto the canvas to add a component.

```
┌───────┐
│       │  ← Collapsed state (default on smaller screens)
│  [⚙] │  Processor
│       │
│  [→→] │  Input Port
│       │
│  [→→] │  Output Port
│       │
│  [⬡]  │  Process Group
│       │
│  [☁]  │  Remote Process Group (S2S)
│       │
│  [≡]  │  Funnel (merge multiple connections)
│       │
│  [T]  │  Label (annotation/comment box)
│       │
└───────┘
```

### Adding a Processor — Step by Step

```
Step 1: Drag the [⚙] icon from the palette onto the canvas
        ┌─────────────────────────────┐
        │  Add Processor              │
        │                             │
        │  🔍 Filter processor types  │
        │  ┌─────────────────────┐   │
        │  │  ConsumeKafka_2_6  │◀──┤── most common for Kafka
        │  │  ConsumeKafkaRecord│   │
        │  │  FetchFile         │   │
        │  │  GenerateFlowFile  │   │
        │  │  GetFile           │   │
        │  │  GetHTTP           │   │
        │  │  ...               │   │
        │  └─────────────────────┘   │
        │                             │
        │  [ Tags: database, sql ]    │← filter by tag
        │                             │
        │       [Cancel]  [ADD]       │
        └─────────────────────────────┘

Step 2: Processor appears on canvas in STOPPED state
        ┌─────────────────────┐
        │ ⚙  GetFile          │
        │ ■ STOPPED           │
        │ In: 0  Out: 0       │
        └─────────────────────┘

Step 3: Double-click to configure, then ▶ to start
```

---

## 4. Processor — Visual Anatomy

Each processor on the canvas is a card with live statistics. Here's every element labeled:

```
        ┌ ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ┐
        │                                               │
        │  ┌─────────────────────────────────────────┐ │
  [1]──▶│  │ ⚙  ConvertRecord                        │ │
  [2]──▶│  │ ▶  RUNNING          [✎] [■] [×]         │◀─[3]
  [3]──▶│  │                                         │ │
  [4]──▶│  │ In:   1,204  (4.8 MB)  5min             │ │
  [5]──▶│  │ Out:  1,198  (6.1 MB)  5min             │ │
  [6]──▶│  │ Read: 4.8 MB  Written: 6.1 MB           │ │
  [7]──▶│  │ Tasks: 248    Time: 1.2s                 │ │
  [8]──▶│  └─────────────────────────────────────────┘ │
        │                                               │
  [9]──▶│  ┌──[ failure ]──▶                           │
 [10]──▶│  └──[ success ]──▶                           │
        └ ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ┘
```

| # | Element | Description |
|---|---|---|
| 1 | **⚙ Icon + Name** | Processor type icon and display name (editable) |
| 2 | **Status badge** | `▶ RUNNING` (green), `■ STOPPED` (red), `⏸ DISABLED` (grey), `⚠ INVALID` (yellow) |
| 3 | **Action buttons** | `✎` configure, `■` stop/`▶` start, `×` delete |
| 4 | **In** | FlowFiles and bytes received in the last 5 min |
| 5 | **Out** | FlowFiles and bytes sent in the last 5 min |
| 6 | **Read/Written** | Content repository I/O |
| 7 | **Tasks/Time** | Scheduler invocations and cumulative CPU time |
| 8 | **Bulletin indicator** | `⚠` appears top-right when there are errors/warnings |
| 9 | **Relationship labels** | Named exit paths — hover to see full name |
| 10 | **Connection arrows** | Click and drag from a relationship to another processor |

### Processor Status Colors

```
  ▶  RUNNING    — Green  — actively processing FlowFiles
  ■  STOPPED    — Red    — not scheduled, will not run
  ⏸  DISABLED   — Grey   — administratively disabled
  ⚠  INVALID    — Yellow — missing required config (check ✎)
  ●  VALIDATING — Blue   — controller services loading
```

### Bulletin Badge (Top-Right Corner)

```
  ┌─────────────────────────┐
  │ ⚙  PutDatabaseRecord  ⚠│◀── red badge = ERROR bulletin
  │ ■ STOPPED               │
  │ ...                     │    Hover over ⚠ to see the message:
  └─────────────────────────┘    ┌─────────────────────────────────────┐
                                  │ ERROR  2024-01-15 14:32:01           │
                                  │ Failed to obtain connection to DB.   │
                                  │ Connection refused: prod-db:5432     │
                                  └─────────────────────────────────────┘
```

---

## 5. Connection — Visual Anatomy

Connections are the arrows between processors. They are also **queues** — click one to see what's buffered inside.

```
  ┌─────────────────┐                           ┌──────────────────┐
  │ ⚙  GetFile      │                           │ ⚙  ConvertRecord │
  │ ▶ RUNNING       │                           │ ▶ RUNNING        │
  └─────────────────┘                           └──────────────────┘
           │
           │  [success]              ← relationship name label
           │
           ▼
    ┌──────────────────┐
    │   12 (48 KB)     │◀──── queue depth (FlowFiles / bytes)
    │  ████░░░░░░░░░░  │◀──── visual fill = % of back-pressure threshold
    └──────────────────┘
           │
           ▼  ─────────────────────────────────────────────▶
```

### Connection Context Menu (Right-Click)

```
Right-click on any connection arrow:
┌──────────────────────────────────┐
│  ✎  Configure                    │← set back-pressure thresholds
│  👁  View queued FlowFiles        │← inspect what's buffered
│  ⊳   Empty queue                 │← drop all buffered data (destructive!)
│  ×   Delete                      │
└──────────────────────────────────┘
```

### Connection Configuration Dialog

```
┌──────────────────────────────────────────────────────────┐
│  Connection Configuration                           [×]  │
├──────────────────────────────────────────────────────────┤
│  Name:  [  success                               ]       │
│                                                           │
│  ── Relationships ─────────────────────────────          │
│  [✓] success     [ ] failure    [ ] retry                │
│      ↑ checked = this connection carries this output     │
│                                                           │
│  ── Back Pressure ──────────────────────────────         │
│  Object Threshold:  [ 10000          ]  FlowFiles        │
│  Size Threshold:    [ 1 GB           ]                   │
│                                                           │
│  ── Load Balance ────────────────────────────────        │
│  Strategy: [ Do not load balance         ▼ ]             │
│            ┌─────────────────────────────────┐           │
│            │ Do not load balance              │           │
│            │ Round robin                      │           │
│            │ Single node                      │           │
│            │ Partition by attribute           │           │
│            └─────────────────────────────────┘           │
│                                                           │
│  Prioritizers:  [ FirstInFirstOutPrioritizer  ▼ ]        │
│                 (drag to reorder multiple prioritizers)   │
│                                                           │
│                              [Cancel]  [Apply]           │
└──────────────────────────────────────────────────────────┘
```

---

## 6. Configuring a Processor

Double-click any processor to open its configuration dialog. This is the most-used dialog in NiFi.

### Tab 1 — Settings

```
┌──────────────────────────────────────────────────────────────────┐
│  Configure Processor — ConvertRecord                       [×]   │
├─────────────────────────────────────────────────────────────────┤
│  [Settings] [Scheduling] [Properties] [Relationships] [Comments] │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Name:           [ ConvertRecord                          ]      │
│  Id:             [ a1b2c3d4-e5f6-7890-abcd-ef1234567890  ]      │
│  Type:           ConvertRecord  1.23.0                           │
│                                                                   │
│  Penalty Duration:   [ 30 sec  ]   ← wait before retrying        │
│  Yield Duration:     [ 1 sec   ]   ← wait if no data to process  │
│                                                                   │
│  Bulletin Level:  [ WARN ▼ ]   ← minimum level to show in UI     │
│                   [ DEBUG / INFO / WARN / ERROR ]                 │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

### Tab 2 — Scheduling

```
├─────────────────────────────────────────────────────────────────┤
│  [Settings] [Scheduling] [Properties] [Relationships] [Comments] │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Scheduling Strategy:  [ Timer driven  ▼ ]                      │
│                        [ Timer driven / CRON driven / Event ]    │
│                                                                   │
│  Concurrent Tasks:     [ 1  ]   ← threads for this processor     │
│                                                                   │
│  Run Schedule:         [ 0 sec ]   ← run as fast as possible     │
│                        examples: 5 sec / 1 min / 0 0 * * * ?    │
│                                                                   │
│  Run Duration:         [ 0 ms  ]   ← 0 = yield after each task   │
│                        (increase to batch more work per thread)   │
│                                                                   │
│  Execution:  ( ) All Nodes   (●) Primary Node                    │
│              ↑ choose "Primary Node" for once-per-cluster tasks   │
└──────────────────────────────────────────────────────────────────┘
```

### Tab 3 — Properties (Most Important Tab)

```
├─────────────────────────────────────────────────────────────────┤
│  [Settings] [Scheduling] [Properties] [Relationships] [Comments] │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  PROPERTY NAME              VALUE              [+] Add Property  │
│  ─────────────────────────────────────────────────────────────  │
│  * Record Reader            [CSVReader     ▼ ]  [→ configure]    │
│  * Record Writer            [JsonRecordSet ▼ ]  [→ configure]    │
│    Include Zero Record FF   [ false        ▼ ]                   │
│                                                                   │
│  ── Dynamic Properties ──────────────────────────               │
│  (none)                                                           │
│                                                                   │
│  ── Expression Language ──────────────────────────              │
│  You can use ${attribute.name} in any value field                │
│  Examples:                                                        │
│    ${filename}              → FlowFile filename attribute         │
│    ${now():format('yyyy')}  → current year                        │
│    ${literal(5):multiply(${count})} → math                       │
│                                                                   │
│  ── Parameters ────────────────────────────────────             │
│  Use #{parameter.name} to reference parameter context values     │
│  Example: #{db.url}  →  jdbc:postgresql://prod:5432/orders       │
└──────────────────────────────────────────────────────────────────┘
```

### Tab 4 — Relationships

```
├─────────────────────────────────────────────────────────────────┤
│  [Settings] [Scheduling] [Properties] [Relationships] [Comments] │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  RELATIONSHIP   AUTO-TERMINATE   RETRY    DESCRIPTION            │
│  ──────────────────────────────────────────────────────────────  │
│  success         [ ]              [ ]     Record converted OK     │
│  failure         [✓]              [ ]     Conversion failed       │
│                                                                   │
│  ↑ Auto-Terminate = drop FlowFiles on this path (no connection   │
│    needed). Good for failure paths you want to discard.           │
│                                                                   │
│  Retry = NiFi will automatically re-queue on this path (NiFi 2)  │
│                                                                   │
│                              [Cancel]  [Apply]                   │
└──────────────────────────────────────────────────────────────────┘
```

---

## 7. Process Groups

Process Groups let you **organize processors into logical modules**. They appear as blue boxes on the canvas. Double-click to drill in.

### On the Canvas (Collapsed View)

```
  ┌──────────────────────────────────────────────────────────────┐
  │  ⬡  Kafka → S3 Ingestion Pipeline                           │
  │                                                              │
  │  ▶  8 processors running    ■  2 processors stopped         │
  │  Queued: 45 FlowFiles (180 KB)                               │
  │                                                              │
  │  ← [In Port: kafka-raw]         [Out Port: s3-landing] →    │
  │                                                              │
  └──────────────────────────────────────────────────────────────┘
                    ▲
                    └── Double-click to enter the group
```

### Inside a Process Group (Drill-Down View)

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Apache NiFi                                          [Search...]        │
├─────────────────────────────────────────────────────────────────────────┤
│  Breadcrumb: NiFi Flow  /  Kafka → S3 Ingestion  /  Transform Layer     │
│              ↑ click to navigate back up                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ╔═══════════╗       ╔═══════════════╗       ╔══════════════╗            │
│  ║ Input Port║──────▶║  JoltTransform║──────▶║  Output Port ║            │
│  ║ kafka-raw ║       ║  ▶ RUNNING    ║       ║  s3-landing  ║            │
│  ╚═══════════╝       ╚═══════════════╝       ╚══════════════╝            │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

### Process Group Context Menu (Right-Click)

```
Right-click on a Process Group:
┌────────────────────────────────────────┐
│  ✎  Configure                          │← name, comments, parameters
│  ▶   Start                             │
│  ■   Stop                              │
│  →   Enter group                       │← same as double-click
│  ⎗   Copy                              │
│  ⟳   Refresh remote                    │
│  ──────────────────────────────────   │
│  📦  Download flow definition (JSON)   │← export this group as JSON
│  📋  Upload flow definition            │← replace with uploaded JSON
│  ──────────────────────────────────   │
│  🔗  Commit to Registry               │← version control (if Registry connected)
│  🔄  Sync with Registry               │
│  ──────────────────────────────────   │
│  ×   Delete                            │
└────────────────────────────────────────┘
```

### Process Group Configuration — Parameters Tab

Parameter Contexts replace hardcoded values. They are scoped to a Process Group.

```
┌──────────────────────────────────────────────────────────────────┐
│  Process Group Configuration — Kafka → S3                  [×]   │
├──────────────────────────────────────────────────────────────────┤
│  [General] [Parameters]                                           │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Parameter Context:  [ kafka-prod-params ▼ ]  [+ Create New]     │
│                                                                   │
│  ── Parameters in context ──────────────────────────────         │
│  NAME              VALUE                 SENSITIVE               │
│  ──────────────────────────────────────────────────────         │
│  kafka.brokers     kafka-prod:9092       no                       │
│  kafka.topic       orders-raw            no                       │
│  kafka.username    nifi_consumer         no                       │
│  kafka.password    ••••••••••••••        yes ← never shown        │
│  s3.bucket         datalake-prod         no                       │
│  s3.prefix         raw/orders/           no                       │
│                                                                   │
│  Usage in processors: #{kafka.brokers}  #{s3.bucket}              │
└──────────────────────────────────────────────────────────────────┘
```

---

## 8. Controller Services

Controller Services are **shared resources** (DB connection pools, SSL contexts, schema registries) reused across many processors. They live at the Process Group level and are configured separately from processors.

### Accessing Controller Services

```
Right-click canvas (empty area) → Configure → Controller Services tab

  OR

Hamburger menu [≡] → Controller Settings → Controller Services
```

### Controller Services List View

```
┌────────────────────────────────────────────────────────────────────────┐
│  Controller Services                                             [×]    │
├────────────────────────────────────────────────────────────────────────┤
│  [ + Add Service ]                                [ Filter...      ]    │
│                                                                          │
│  NAME                    TYPE                  STATE     SCOPE          │
│  ──────────────────────────────────────────────────────────────────    │
│  ⚡ postgres-prod         DBCPConnectionPool   ● Enabled  Root         │
│  ⚡ kafka-schema-reg      AvroSchemaRegistry   ● Enabled  Root         │
│  ⚡ csv-reader            CSVReader            ● Enabled  Kafka Group  │
│  ⚡ json-writer           JsonRecordSetWriter  ● Enabled  Kafka Group  │
│  ⚡ ssl-context           StandardSSLContext   ○ Disabled Root         │
│                                                                          │
│  ● = Enabled (ready to use)                                              │
│  ○ = Disabled (cannot be used by processors)                             │
└────────────────────────────────────────────────────────────────────────┘
```

### DBCPConnectionPool — Configuration Example

This is the most common Controller Service — it creates a JDBC connection pool.

```
┌──────────────────────────────────────────────────────────────────────┐
│  Configure Controller Service — DBCPConnectionPool            [×]    │
├──────────────────────────────────────────────────────────────────────┤
│  [Settings] [Properties]                                              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  PROPERTY                        VALUE                                 │
│  ──────────────────────────────────────────────────────────────────  │
│  * Database Connection URL       #{db.url}                            │
│    jdbc:postgresql://#{db.host}:#{db.port}/#{db.name}                 │
│                                                                        │
│  * Database Driver Class Name    org.postgresql.Driver                 │
│    Database Driver Location(s)   /opt/nifi/lib/postgresql-42.jar      │
│  * Database User                 #{db.user}                           │
│  * Password                      ••••••••  (sensitive)                │
│                                                                        │
│  ── Pool Settings ─────────────────────────────────────────────      │
│    Max Total Connections         [ 10 ]  ← match to Concurrent Tasks  │
│    Max Idle Connections          [ 5  ]                                │
│    Min Idle Connections          [ 1  ]                                │
│    Max Wait Time                 [ 500 ms ]                            │
│    Validation Query              [ SELECT 1 ]                          │
│                                                                        │
│                               [Cancel]  [Apply]  [Enable ▶]           │
└──────────────────────────────────────────────────────────────────────┘
```

> **Important:** You must **Disable** a Controller Service before editing its properties, and **Enable** it again after. While disabled, all processors using it will go INVALID.

### Enable / Disable Flow

```
  [Disable Service]
       │
       ▼
  ┌────────────────────────────────────────────────┐
  │  Disabling Service: postgres-prod               │
  │                                                  │
  │  The following processors use this service:      │
  │  ● PutDatabaseRecord  (RUNNING → will STOP)      │
  │  ● QueryDatabaseTable (RUNNING → will STOP)      │
  │                                                  │
  │  [ ] Stop referencing processors first           │
  │                                                  │
  │                   [Cancel]  [Disable]            │
  └────────────────────────────────────────────────┘
```

---

## 9. Queue Inspection & Back-Pressure

Clicking a connection's queue depth number opens the **queue inspector** — one of NiFi's most powerful debugging tools.

### Queue Depth Indicator on Connection

```
                    ┌──────────────────┐
   ← connection ──▶ │   847 (3.2 MB)   │ ← click this number
                    │  ███████░░░░░░░   │ ← fill bar: 847/10000 = 8.5%
                    └──────────────────┘
                         ↑
                    back-pressure not triggered yet
                    (would trigger at 10,000 objects OR 1 GB)
```

### Back-Pressure States

```
  ░░░░░░░░░░░░░░░  0%       Normal — data flowing freely
  ████░░░░░░░░░░░  25%      Light load
  ████████░░░░░░░  55%      Moderate — watch this connection
  ████████████░░░  80%      High — consider scaling downstream
  ███████████████  100% ⚠   BACK-PRESSURE ACTIVE — upstream stopped
```

### Queue Inspector Dialog

```
┌────────────────────────────────────────────────────────────────────┐
│  Queue: GetFile → ConvertRecord (success)                    [×]   │
├────────────────────────────────────────────────────────────────────┤
│  Queued: 847 FlowFiles  (3.2 MB)    [Empty Queue]  [▶ List FFs]   │
│                                                                      │
│  POSITION  FILENAME          SIZE    QUEUED DURATION  PENALIZED     │
│  ────────────────────────────────────────────────────────────────  │
│  1         orders_001.csv    4.1 KB  00:00:03         no            │
│  2         orders_002.csv    3.9 KB  00:00:03         no            │
│  3         orders_003.csv    4.2 KB  00:00:03         no            │
│  ...                                                                 │
│  847       orders_847.csv    3.8 KB  00:00:05         no            │
│                                                                      │
│  Click any row to see full FlowFile attributes and content          │
└────────────────────────────────────────────────────────────────────┘
```

### FlowFile Detail View (Click a Row)

```
┌────────────────────────────────────────────────────────────────────┐
│  FlowFile Detail — orders_001.csv                            [×]   │
├─────────────────────────────────────────────────────────────────── │
│  [Attributes]  [Content]                                            │
├────────────────────────────────────────────────────────────────────┤
│  ATTRIBUTE                 VALUE                                    │
│  ─────────────────────────────────────────────────────────────    │
│  filename                  orders_001.csv                           │
│  path                      /data/input/                             │
│  uuid                      a1b2c3d4-e5f6-7890-...                  │
│  file.size                 4196                                     │
│  file.lastModifiedTime     2024-01-15T14:30:00Z                    │
│  file.permissions          rw-r--r--                                │
│  absolute.path             /data/input/orders_001.csv              │
│                                                                      │
│  ── Dynamic Attributes ────────────────────────────────────────   │
│  source.system             ERP                                      │
│  region                    EU                                       │
│                                                                      │
│  [Content Preview]  ← opens hex or text view of the data           │
└────────────────────────────────────────────────────────────────────┘
```

---

## 10. Data Provenance UI

Provenance is NiFi's full audit trail. Access it via the hamburger menu `[≡] → Data Provenance` or right-click a processor → `View Data Provenance`.

### Provenance Search Form

```
┌────────────────────────────────────────────────────────────────────────┐
│  Data Provenance Search                                          [×]   │
├────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Component Name:   [ ConvertRecord              ]                       │
│  Component Type:   [                            ]                       │
│  FlowFile UUID:    [                            ]                       │
│  Filename:         [ orders_001.csv             ]                       │
│  Attribute Name:   [ region          ] Value: [ EU ]                    │
│  Event Type:       [ (All)           ▼ ]                                │
│                    RECEIVE / FORK / CLONE / CONTENT_MODIFIED            │
│                    ATTRIBUTES_MODIFIED / DROP / SEND / EXPIRE           │
│  Start Date:       [ 01/15/2024 14:00 ]                                 │
│  End Date:         [ 01/15/2024 15:00 ]                                 │
│  Minimum File Size:[ 0 B   ]  Maximum: [       ]                        │
│                                                                          │
│                              [Search]                                   │
└────────────────────────────────────────────────────────────────────────┘
```

### Provenance Results Table

```
┌───────────────────────────────────────────────────────────────────────────────┐
│  Data Provenance Results  (1,248 events)                              [×]     │
├───────────────────────────────────────────────────────────────────────────────┤
│  TIME                  TYPE              FILENAME         COMPONENT           │
│  ─────────────────────────────────────────────────────────────────────────   │
│  14:32:01.453  [i] RECEIVE              orders_001.csv  GetFile               │
│  14:32:01.891  [i] CONTENT_MODIFIED     orders_001.csv  ConvertRecord         │
│  14:32:02.012  [i] ATTRIBUTES_MODIFIED  orders_001.csv  UpdateAttribute       │
│  14:32:02.244  [i] SEND                 orders_001.csv  PutS3Object            │
│  14:32:02.301  [i] DROP                 orders_001.csv  PutS3Object            │
│                                                                                 │
│  Click [i] to see full event → replay button available on RECEIVE events       │
└───────────────────────────────────────────────────────────────────────────────┘
```

### Provenance Event Detail (Click [i])

```
┌────────────────────────────────────────────────────────────────────────┐
│  Provenance Event Detail                                         [×]   │
├────────────────────────────────────────────────────────────────────────┤
│  Event ID:       48291                                                   │
│  Event Type:     CONTENT_MODIFIED                                        │
│  Event Time:     01/15/2024 14:32:01.891                                │
│  Duration:       48 ms                                                   │
│  Component:      ConvertRecord                                           │
│  Component Type: ConvertRecord                                           │
│  FlowFile UUID:  a1b2c3d4-e5f6-7890-abcd-ef1234567890                  │
│                                                                          │
│  ── Content ────────────────────────────────────────────────────        │
│  Input:  4,196 bytes  [Download]  [View]  [Replay from here ▶]          │
│  Output: 6,144 bytes  [Download]  [View]                                 │
│                                                                          │
│  ── Attribute Changes ──────────────────────────────────────            │
│  ATTRIBUTE     PREVIOUS VALUE      CURRENT VALUE                        │
│  ─────────────────────────────────────────────────                     │
│  mime.type     text/csv            application/json                     │
│                                                                          │
│  ← [Previous Event]                           [Next Event] →            │
└────────────────────────────────────────────────────────────────────────┘
```

### Lineage Graph (Click "Show Lineage" on any Event)

```
┌────────────────────────────────────────────────────────────────────────┐
│  Lineage for FlowFile: orders_001.csv                            [×]   │
├────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│       ┌──────────────────────────────────────────────────────┐         │
│  [+]──│  RECEIVE  GetFile  14:32:01  4,196 B                 │         │
│       └──────────────────────────────────────────────────────┘         │
│                              │                                           │
│                              ▼                                           │
│       ┌──────────────────────────────────────────────────────┐         │
│       │  CONTENT_MODIFIED  ConvertRecord  14:32:01  6,144 B  │         │
│       └──────────────────────────────────────────────────────┘         │
│                              │                                           │
│                              ▼                                           │
│       ┌──────────────────────────────────────────────────────┐         │
│       │  ATTRIBUTES_MODIFIED  UpdateAttribute  14:32:02      │         │
│       └──────────────────────────────────────────────────────┘         │
│                              │                                           │
│                              ▼                                           │
│       ┌──────────────────────────────────────────────────────┐         │
│       │  SEND  PutS3Object  14:32:02  6,144 B                │         │
│       └──────────────────────────────────────────────────────┘         │
│                              │                                           │
│                              ▼                                           │
│       ┌──────────────────────────────────────────────────────┐         │
│       │  DROP  14:32:02                                       │         │
│       └──────────────────────────────────────────────────────┘         │
│                                                                          │
│  [ ← Expand upstream ]            [ Expand downstream → ]               │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 11. Monitoring: Bulletins & Status Bar

### Status Bar (Bottom of Screen)

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  NiFi Flow  /  Kafka Ingestion  /  Transform                                   │
│                                                                                  │
│  [Version 7 ✓]  [★ Primary]     Active Threads: 12 / 200    Queued: 4.2K      │
│                                  ▲ total across all processors                  │
│                                                                                  │
│  [● Cluster: 3/3 nodes]   [⚠ 2 Bulletins]   ⏱ 2024-01-15 14:32:01             │
└────────────────────────────────────────────────────────────────────────────────┘
```

| Indicator | Meaning |
|---|---|
| `Version 7 ✓` | Flow is under version control and up to date |
| `Version 7 ✗` | Flow has unsaved local changes vs Registry |
| `★ Primary` | You are viewing the Primary Node |
| `Active Threads` | Threads in use / total pool size |
| `Queued` | Total FlowFiles waiting across all connections |
| `● 3/3 nodes` | All cluster nodes healthy |
| `● 2/3 nodes` | One node disconnected — click to see which one |
| `⚠ 2 Bulletins` | Recent errors/warnings — click to see them |

### Bulletin Board

Access via `[≡] → Bulletin Board` or click the `⚠` indicator.

```
┌────────────────────────────────────────────────────────────────────────┐
│  Bulletin Board                                                  [×]   │
├────────────────────────────────────────────────────────────────────────┤
│  [ Filter by component name... ]   Level: [ WARN ▼ ]  [Refresh ⟳]    │
├────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ⛔  ERROR  14:32:01  PutDatabaseRecord  [Orders Pipeline]              │
│     Failed to insert record: duplicate key value violates unique        │
│     constraint "orders_pkey"                                             │
│                                                                          │
│  ⚠   WARN  14:31:55  ConsumeKafka  [Kafka Ingestion]                   │
│     Unable to communicate with Kafka broker kafka-02:9092.              │
│     Will retry in 5 seconds.                                             │
│                                                                          │
│  ℹ   INFO  14:31:44  UpdateAttribute  [Transform Layer]                 │
│     No updates required for FlowFile: orders_001.csv                    │
│                                                                          │
└────────────────────────────────────────────────────────────────────────┘
```

### Summary Table — All Processors

`[≡] → Summary` gives a spreadsheet view of all processors and their stats. Essential for performance tuning.

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  NiFi Summary                                          [ Filter...        ]    │
├──────────────────┬──────────┬──────────┬──────────┬──────────┬────────────────┤
│  NAME            │  TYPE    │  GROUP   │  THREADS │  IN (5m) │  OUT (5m)      │
├──────────────────┼──────────┼──────────┼──────────┼──────────┼────────────────┤
│  ConsumeKafka    │ Consume  │ Kafka In │    4/6   │  10.2K   │  10.2K (82MB)  │
│  ConvertRecord   │ Convert  │ Transform│    4/4   │  10.2K   │  10.2K (91MB)  │
│  UpdateAttribute │ Update   │ Transform│    2/4   │  10.2K   │  10.2K (91MB)  │
│  PutS3Object     │ Put      │ Sink     │    2/4   │   9.8K   │   9.8K (91MB)  │
│  PutDatabaseRec  │ Put      │ Sink     │    0/2   │      0   │    ERROR ⚠     │
└──────────────────┴──────────┴──────────┴──────────┴──────────┴────────────────┘
```

---

## 12. NiFi Registry: Version Control

NiFi Registry provides **Git-like version control** for your flows. Connect via `[≡] → Controller Settings → Registry Clients`.

### Adding a Registry Client

```
┌──────────────────────────────────────────────────────────────────┐
│  Controller Settings                                       [×]   │
├──────────────────────────────────────────────────────────────────┤
│  [General] [Registry Clients] [Reporting Tasks] [Parameter Prov] │
├──────────────────────────────────────────────────────────────────┤
│  [ + Add Registry Client ]                                        │
│                                                                   │
│  NAME             TYPE                 URL                        │
│  ─────────────────────────────────────────────────────────────  │
│  NiFi Registry   NiFiRegistryFlowPers  https://registry:18443    │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

### Version Control — Process Group States

```
  State on canvas badge (bottom-left of Process Group):

  [Version 7 ✓]  Up to date — local == registry
  [Version 7 M]  Modified   — local has unsaved changes
  [Version 7 S]  Stale      — registry has a newer version
  [Version 7 MS] Both — local changes AND registry is ahead
  [Local]         Not under version control
```

### Commit a New Version

```
Right-click Process Group → Version → Save new version

┌──────────────────────────────────────────────────────────────────┐
│  Save Flow Version                                         [×]   │
├──────────────────────────────────────────────────────────────────┤
│  Registry:  NiFi Registry              (configured above)        │
│  Bucket:    [ production-flows    ▼ ]                            │
│  Flow Name: [ Kafka → S3 Pipeline ]                              │
│  Version:   8  (auto-incremented)                                │
│                                                                   │
│  Comments:  [ Add SLA breach alerting via PutSlack processor ]   │
│             ↑ write meaningful commit messages — same as git      │
│                                                                   │
│                              [Cancel]  [Save]                    │
└──────────────────────────────────────────────────────────────────┘
```

### Revert to a Previous Version

```
Right-click Process Group → Version → Change version

┌──────────────────────────────────────────────────────────────────┐
│  Flow Version History                                      [×]   │
├──────────────────────────────────────────────────────────────────┤
│  VERSION  DATE              AUTHOR    COMMENTS                   │
│  ──────────────────────────────────────────────────────────────  │
│  8 ◀curr  2024-01-15 14:00  ahad      Add SLA breach alerting    │
│  7        2024-01-14 09:30  ahad      Tune back-pressure to 5GB  │
│  6        2024-01-12 16:45  sara      Add retry logic on failure  │
│  5        2024-01-10 11:00  ahad      Initial production version  │
│                                                                   │
│  Select version 6 → [Change]  ← all processors will be replaced  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 13. Keyboard Shortcuts & Power-User Tips

### Keyboard Shortcuts

| Shortcut | Action |
|---|---|
| `Ctrl + A` | Select all components |
| `Ctrl + C` | Copy selected |
| `Ctrl + V` | Paste |
| `Ctrl + Z` | Undo |
| `Ctrl + Shift + Z` | Redo |
| `Delete` | Delete selected |
| `Ctrl + Scroll` | Zoom in/out |
| `Ctrl + Shift + F` | Fit flow to screen |
| `Ctrl + Shift + P` | Center canvas on selected |
| `Escape` | Deselect / close dialog |
| `R` (on canvas) | Refresh stats |

### Canvas Navigation Tips

```
  Zoom in/out:   Ctrl+Scroll  OR  use the magnifier in bottom-right corner
  Pan:           Click+drag on empty canvas area
  Fit to screen: Ctrl+Shift+F  OR  Navigator minimap in bottom-right

  ┌────────────────────────────────────────────────────┐
  │                                           ┌──────┐ │
  │                    Canvas                 │ mini │ │◀── Navigator minimap
  │                                           │ map  │ │    (always visible)
  │                                           │ [🔍] │ │
  │                                           └──────┘ │
  │  [–]──────────────────────────────────[+]  [↔]    │◀── Zoom slider
  └────────────────────────────────────────────────────┘
```

### Multi-Select & Group Operations

```
1. Hold Shift + click individual components to add to selection
2. Click + drag on empty canvas to rubber-band select a region
3. Right-click selection → Group → creates a Process Group from selection
4. Right-click selection → Start / Stop → controls all selected processors at once
```

### Label (Annotation) Best Practices

Drag `[T]` from the palette to add text labels on the canvas.

```
  ┌─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┐
                                                    
  │  ⚠ SLA: data must land in S3 < 30s from ingest│
     Contact: data-eng@company.com                  
  │  Last reviewed: 2024-01-15                     │
                                                    
  └─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┘
```

---

## Quick Reference: UI Element Cheat Sheet

```
  ELEMENT              HOW TO ADD           HOW TO OPEN/EDIT
  ─────────────────────────────────────────────────────────────────
  Processor            Drag ⚙ from palette  Double-click
  Connection           Hover source → drag  Double-click arrow
  Process Group        Drag ⬡ from palette  Double-click box
  Remote Process Group Drag ☁ from palette  Double-click box
  Input/Output Port    Drag →→ from palette Double-click
  Funnel               Drag ≡ from palette  (no config needed)
  Label                Drag T from palette  Double-click text
  ─────────────────────────────────────────────────────────────────
  Controller Services  ≡ menu → Controller Settings → CS tab
  Reporting Tasks      ≡ menu → Controller Settings → RT tab
  Parameter Contexts   ≡ menu → Parameter Contexts
  Data Provenance      ≡ menu → Data Provenance  OR  right-click processor
  Bulletin Board       ≡ menu → Bulletin Board  OR  click ⚠ in status bar
  Summary              ≡ menu → Summary
  NiFi Registry        ≡ menu → Controller Settings → Registry Clients
  System Diagnostics   ≡ menu → System Diagnostics
  ─────────────────────────────────────────────────────────────────
  Canvas Context Menu  Right-click on empty canvas area
  Component Menu       Right-click any processor / connection / group
```

---

*Built against Apache NiFi 1.23 / 2.x UI — canvas model is unchanged between versions.*
