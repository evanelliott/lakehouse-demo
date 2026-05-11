# Test Approach

## Summary

We have designed a "Shift Left" testing strategy tailored for a complex data engineering stack (Airflow, Playwright, Iceberg, PySpark, DuckDB). By moving logic and resiliency checks into Git hooks and shifting environmental monitoring to a periodic schedule, you ensure a high-velocity, low-redundancy workflow.

---

## 1. Local Environment

* Monolithic DevContainer: A single Docker environment containing the JVM (Spark), Python (Airflow, Playwright, DuckDB), and a pinned Chromium binary.
* Sandboxing: Devcontainer is sandboxed so it cannot access the internet.
* Infrastructure: MinIO and Iceberg REST Catalog accessed via `localhost` within the docker compose network.
* Central Schemas: A root `schemas/` directory defines the contracts for Bronze, Silver, and Gold layers.

---

## 2. Chronological Testing Workflow

## I. Pre-commit (Internal Logic Gate)
Context: These are "stateless" tests executed every time you commit code. They verify the logic of your code without requiring any external services (No Docker, No Internet).

* When: Git `pre-commit` hook.
* Where: Local DevContainer using `pytest`, `uv`, `ruff`.
* Test Type: Unit Tests (Isolated logic).
* Scope: A single test suite validating internal Python/SQL logic using mocks and static files. This includes:
  * Scraper Parsers: Extraction logic using static HTML files.
  * Data Transformations: PySpark/DuckDB functions using small JSON and CSV files.
  * Idempotency Logic: Verifying `MERGE/UPSERT` code handles duplicate inputs correctly.
  * Entity Resolution Logic: Verifying matching algorithms against "Golden" record sets.

## II. Pre-push (Infrastructure & Resilience Gate)
Context: Executed before pushing code to a remote. These tests verify the "Plumbing". They require the Docker test profile to be active.

* When: Git `pre-push` hook.
* Execution: Local Docker Compose (MinIO, Iceberg REST, Spark, Spark History Server, Airflow).
* Test Type: Integration Tests (Component connectivity).
* Scope: A suite verifying service-to-service communication. This includes:
  * Plumbing: Authenticating and writing to the local MinIO and Iceberg Catalog.
  * Negative Paths: Verifying the system handles service outages (e.g., MinIO port closed) without crashing.

## III. Pipeline Runtime (Entity Resolution Integrity Gate)

* When: During the execution of the main Airflow DAG.
* Where: Spark LocalExecutor.
* Constraint: Internal Hard-Fail.
* Scope: Within the Spark Silver-load job, a check for unresolved entities is performed. If `unresolved_count > 0`, the job raises an exception, the Iceberg snapshot is aborted, and all downstream Gold-layer processing is halted.

## IV. Periodic (Observability)

* When: Scheduled Airflow Monitoring DAG (Production/Live).
* Where: Prod environment Airflow Monitoring DAG (Continuous Observability).
* Test Types:
  * Smoke Tests (these do not test data logic; they test if the Infrastructure is healthy and ready for work):
    * Hitting live external sites to verify connectivity (IP block).
    * Basic service availability and health.
    * Internal connectivity (Ports/Health Endpoints).
    * Permissions (Volume/Secret Access).
    * Readiness (Cross-service Bridges).
    * Resources (Storage utilisation, CPU/Memory overhead)
  * Sanity/Contract Tests (these are "Happy Path" tests to prove that a minimal, perfect slice of data can travel from the real internet to the Gold layer without error):
    * Validating that existing Silver/Gold tables in live storage still match expected schema definitions.
    * Source Data stability (detecting DOM changes and validating JSON schema).
    * Data Quality compliance (measuring DQ metrics against pre-defined rules).
      * Completeness (Mandatory Columns)
      * Uniqueness (Natural Keys/PKs)
      * Validity (Type/Format Logic)
      * Consistency (3NF Reference/Orphans)
    * Environmental health checks, including Orphaned Metadata detection (Iceberg/MinIO desync).
---

## 3. Strategy Summary

| Stage | Execution | Category | Success Metric |
|---|---|---|---|
| `pre-commit` | Local | Unit | Internal logic, ER, and Idempotency logic pass. | 
| `pre-push` | Local | Integration | Connectivity and Negative paths pass. | 
| DAG Run | Runtime | Quality Gate | 100% Entity Resolution coverage (Hard-fail). |
| Periodic | Live | Observability | No schema drift, metadata rot, or storage spikes. |