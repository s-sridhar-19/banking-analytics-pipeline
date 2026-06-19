# ⚡ Real-Time Streaming Banking Analytics Pipeline

An enterprise-tier, event-driven Data Lakehouse architecture designed to ingest change-data-capture (CDC) logs from a transactional core banking system, stream them through a message broker, and process them inside a cloud data warehouse for business intelligence.

---

## 🎯 Business Intent & Core Value

In modern retail banking, tracking liquidity, financial risk, and account exposures cannot wait for overnight batch updates. Delayed visibility leads to unauthorized overdrafts, missed fraud windows, and degraded customer experiences.

This architecture solves these challenges by providing **near-real-time financial visibility**, engineered to:
* **Aggregate Real-Time Net Volume:** Track exactly how cash moves across account segments minute-by-minute.
* **Consolidate User Exposures:** Monitor point-in-time account balances and aggregate global velocity metrics directly at the user level.
* **Enable Downstream Risk Automation:** Provide an analytics-ready "Gold" layer to feed instant anomaly detection systems or fraud dashboards.

---

## 🏗️ System Architecture & Data Flow

The platform transforms raw transactional writes from an operational core database into clean business intelligence schemas through a modern decoupled infrastructure loop:

```text
  ┌──────────────────────┐
  │  Core PostgreSQL DB  │  ──► Real-Time Transactional System Writes
  └──────────────────────┘
             │
             ▼ [Debezium CDC Engine]
  ┌──────────────────────┐
  │    Apache Kafka      │  ──► Distributed Event Streaming Architecture
  └──────────────────────┘
             │
             ▼ [Python Core Consumers]
  ┌──────────────────────┐
  │  MinIO S3 Data Lake  │  ──► Immutable Local Raw JSON Event Store
  └──────────────────────┘
             │
             ▼ [Automated Pipeline Sync Tunnel]
  ┌──────────────────────┐
  │  Snowflake Storage   │  ──► Cloud Stage & Semi-Structured Raw Variant Tables
  └──────────────────────┘
             │
             ▼ [dbt Core (Data Build Tool)]
  ┌──────────────────────┐
  │ Analytics Gold Layer │  ──► Final Aggregated Business Marts
  └──────────────────────┘

## 🗂️ Cloud Lakehouse Warehouse Topography

| Warehouse Layer | Schema Name | Object Type | Transformation Purpose |
| :--- | :--- | :--- | :--- |
| **Bronze** | `RAW` | Variant Tables | Securely lands unparsed streaming JSON payloads with immutable ingest timestamps. |
| **Silver** | `STAGING` | Views | Unpacks nested variants, casts data types, deduplicates records, and exposes clean dimensions. |
| **Gold** | `MARTS` | Tables | Executes complex multi-entity analytical joins and computes business metrics (`FCT_USER_BALANCES`). |

---

## 🛠️ Engineering Complexities & Core Triumphs

Building a robust decoupled architecture introduces delicate synchronization and design patterns. Below are the key engineering hurdles encountered and solved during implementation:

### 1. Foreign Key Dependency Execution Ordering
* **The Complexity:** During the initial database seed phase, standard data generation scripts routinely crashed when building the transaction logs ledger due to strict relational constraints.
* **The Resolution:** Enforced an absolute transactional state hierarchy inside the ingestion orchestration engine. Schema creation was isolated to execute sequentially: `users` -> `accounts` -> `transactions`. This guarantee eliminated data generation deadlocks entirely.

### 2. Nested Variant CDC Path Resolution
* **The Complexity:** Raw data successfully landed in Snowflake's storage layer but surfaced as empty `NULL` rows inside the analytical staging views. Snowflake did not throw compile-time syntax errors; it quietly failed to resolve paths because Kafka events were wrapped inside complex root payloads by the Debezium engine.
* **The Resolution:** Rewrote the transformation logic to traverse the structural tree explicitly utilizing exact JSON path mapping (`json_data:payload:after:column_name::type`). This extracted and typed the records cleanly from the semi-structured variants without requiring compute-intensive transformation wrappers.

### 3. Submodule Git Index Desynchronization
* **The Complexity:** Running independent development environments inside specific subdirectories accidentally corrupted the master repository file tracking index, marking child modules as untracked disconnected code bases.
* **The Resolution:** Scrubbed nested `.git` directory structural footprints, reset the indexing cache across the local tree path, and centralized tracking under a single unified root `.gitignore` architecture. This unified all python microservices and transformation scripts under one source-of-truth portfolio.

---

## 🚀 Technical Stack Summary

* **Database Core:** PostgreSQL
* **Event Brokerage:** Apache Kafka + Debezium CDC
* **Storage Ingest Store:** MinIO (S3-Compatible Local Data Lake)
* **Cloud Platform Data Warehouse:** Snowflake
* **Data Modeling & Analytics:** dbt Core (Data Build Tool)
* **Orchestration / Glue Logic:** Python 3 (Boto3, Snowflake-Connector Engine)

