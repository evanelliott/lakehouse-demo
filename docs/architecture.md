# 🏗️ Lakehouse Architecture: Football Event Pipeline
This document outlines the Medallion Architecture implemented in this demo, optimised for the Apple Silicon M4 unified memory architecture.
## 1. System Overview
The stack demonstrates a modern, decoupled Lakehouse:

* Storage: MinIO (S3-compatible) stores raw and processed Parquet files.
* Metadata: Apache Iceberg provides ACID transactions and time travel.
* Catalog: Iceberg REST Catalog manages table state and schema evolution.
* Compute: Trino (SQL Engine) performs all data transformations.
* Orchestration: Apache Airflow manages the end-to-end lineage.

------------------------------
## 2. Data Flow (Medallion)
```
graph LR
    subgraph Ingestion
        A[Mock Python Script] -->|INSERT| B(MinIO: Bronze)
    end

    subgraph Transformation
        B --> C{Trino Engine}
        D[Airflow DAG] -->|Orchestrate| C
    end

    subgraph Consumption
        C -->|MERGE| E(MinIO: Silver)
        E --> F[Jupyter Analysis]
        E --> G[Trino UI Metrics]
    end

    style B fill:#f96,stroke:#333
    style E fill:#9f6,stroke:#333
    style C fill:#69f,stroke:#333
```
------------------------------
## 3. Layer Definitions
### 🥉 Bronze Layer (Raw)

* Format: Iceberg / Parquet.
* Strategy: Append-only ingestion of raw JSON/CSV events.
* Partitioning: day(event_time) via Iceberg hidden partitioning.
* Goal: Capture the full fidelity of source data with zero loss.

### 🥈 Silver Layer (Refined)

* Format: Iceberg / Parquet.
* Strategy: Idempotent MERGE (Upsert) based on event_id.
* Transformations:
* Data Type Casting (Strings to Timestamps/Integers).
   * Schema Enforcement.
   * Standardization (Upper-casing teams, filtering noise).
* Goal: Provide a high-performance, "Analytics-Ready" table.

------------------------------
## 4. Senior DE Design Decisions

   1. In-Place Compute: Instead of pulling data into Airflow memory, we use the TrinoOperator. This prevents OOM errors on the 16GB Mac Mini by keeping the heavy lifting inside the Trino engine.
   2. Hidden Partitioning: By using Iceberg’s day(event_time), we eliminate the "Partition Management" overhead. Trino handles the directory mapping automatically.
   3. Unified Memory Optimization: The stack is configured to leverage the high bandwidth of the M4 chip while strictly capping the JVM heap to prevent macOS system lag.

------------------------------
## 5. Metadata Observability
To inspect the Lakehouse health, run these queries in Trino:

* `SELECT * FROM "silver.match_events$snapshots"` (View table history)
* `SELECT * FROM "silver.match_events$history"` (View lineage of changes)

------------------------------