# lakehouse-demo
# ⚽ Premier League Lakehouse: High-Performance Football ETL
Optimized for Apple Silicon (M4) | 16GB RAM | Apache Iceberg

This project is a production-grade demonstration of a Modern Data Lakehouse architecture. It implements a Medallion Architecture (Bronze → Silver) by scraping real-world Premier League xG (Expected Goals) data from Understat and processing it via Trino and Apache Iceberg.

The stack is meticulously tuned for 16GB Unified Memory, ensuring stability while running a full suite of data engineering tools locally.

------------------------------
## 🏗️ Architecture & Data Flow
The system operates on the principle of Separation of Compute and Storage:

   1. Ingestion (Bronze): A resilient Airflow DAG scrapes Understat. It features hexadecimal-aware decoding (\xHH) and native exponential backoff to handle server-side rate limiting.
   2. Storage (The Lake): MinIO (S3-compatible) hosting Apache Iceberg tables in Parquet format.
   3. Compute (The Engine): Trino performs high-concurrency, distributed SQL transformations.
   4. Orchestration: Apache Airflow manages the lifecycle, utilizing XComs for metadata traceability and backfilling for historical data recovery.

------------------------------
## 🌟 Senior-Level Engineering Highlights
### 1. Robust Web Scraping & Encoding
Understat utilizes non-standard hexadecimal escaping (\x7B for {) within its embedded JSON. This project implements a defensive scraping strategy using codecs.decode and raw-string regex to ensure data integrity and prevent UnicodeDecodeErrors.
### 2. Native Airflow Resilience
Rather than overengineering with external libraries, this pipeline leverages Airflow Native Task Parameters:

* retry_exponential_backoff: Gracefully handles 429 Too Many Requests.
* max_active_runs=1: Ensures "Polite Scraping" during historical backfills.
* catchup=True: Automatically recovers data from the start of the 23/24 season.

### 3. Hardware-Aware Resource Management (M4)
To prevent macOS "Memory Pressure" on a 16GB Mac Mini:

* JVM Heap Capping: Trino is restricted to 3GB via TRINO_JVM_MAX_HEAP_SIZE.
* Query Guardrails: Global query limits (query.max-memory) are injected via Docker environment variables to prevent OOM (Out of Memory) crashes.
* I/O Optimization: Iceberg Hidden Partitioning (day(event_time)) is used to minimize S3/MinIO scan costs.

------------------------------
## 🚀 Quick Start

   1. Initialize:
   
   `cp .env.example .env  # Generate a Fernet Key as per docs/setup_guide.md`
   
   2. Launch Stack:
   
   `make up`
   
   `make health  # Wait for Airflow (Status 200) and Trino (Status 303)`
   
   3. Bootstrap Data:
   
   `make seed  # Create Iceberg schemas and tables`
   
   4. Trigger Backfill:
   Access Airflow at http://localhost:8085. Unpause understat_daily_matches_bronze to begin the historical ingestion.

------------------------------
## 📊 Access Matrix

| Service | Local URL | Role |
|---|---|---|
| Trino UI | localhost:8080 | Query Monitoring (Login required) |
| MinIO Console | localhost:9001 | S3 Object Browser (Verify Parquet files) |
| Airflow UI | localhost:8085 | ETL Orchestration & XCom Inspection |
| Jupyter Lab | localhost:8888 | Data Science & SQL Discovery |

------------------------------
## 📂 Project Structure
```
├── Makefile                # Standardized developer entry points
├── docker-compose.yml      # Fully abstracted service orchestration
├── .env.example            # Environment template (Security First)
├── airflow/                # Custom Dockerfile & Resilient DAGs
├── trino/                  # Catalog & Engine configuration
├── scripts/                # Health checks & Metadata seeding
└── docs/                   # Architecture & M4 Setup deep-dives
```
