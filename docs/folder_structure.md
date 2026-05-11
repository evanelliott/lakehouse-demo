# Folder Structure

```
.
├── .devcontainer/          # Environment parity for the team
├── docker-compose.yml      # The "Local Lakehouse" (MinIO, Iceberg, Airflow)
├── dags/
│   ├── main_pipeline.py    # Orchestrates Scraper -> Bronze -> Silver (Spark) -> Gold (DuckDB)
│   └── monitoring_dag.py   # PERIODIC: Smoke, Contract, Storage, & Metadata checks
├── src/
│   ├── scraper/            # Playwright scripts
│   ├── spark/              # PySpark logic: Ingestion & Entity Resolution (Silver)
│   ├── duckdb/             # SQL/Python: Silver to Gold & Ad-hoc queries
│   └── core/               # Shared logic (Schema enforcement, ER algorithms)
├── schemas/                # Source of Truth for Contract Testing
│   ├── bronze_json/
│   └── iceberg_tables/     # Definitions for Silver/Gold
├── tests/
│   ├── unit/               # PRE-COMMIT: Parser logic, ER logic, Idempotency
│   │   └── mock_data/      # Static HTML and small DataFrames
│   └── integration/        # PRE-PUSH: Service resilience & Airflow retry behavior
├── scripts/                # Git hook execution scripts
└── pyproject.toml
```
