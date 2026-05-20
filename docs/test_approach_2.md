# Refined "Shift Left" Test Strategy
## Summary
We have optimized this testing strategy to run entirely on the local developer machine with zero dependencies on remote CI servers, while keeping the inner feedback loop exceptionally fast. By decoupling code logic from live network dependencies using VCR cassettes, and utilizing both DuckDB and Spark Connect over ephemeral volumes directly within a unified environment, we eliminate container spin-up and network driver overhead.
Live external connections are handled strictly as a periodic observability task to catch upstream data drift before production runs, logging self-remediation steps directly into Airflow.

------------------------------
## 1. Local Environment Architecture

* Persistent DevContainer Stack: A unified Docker Compose network containing the JVM (Spark Connect server), Python (Airflow, Playwright, DuckDB), MinIO, and the Iceberg REST Catalog. It remains up and running in the background during development.
* Unified Execution: The Spark Connect server process, the in-memory DuckDB engine, and the testing runner share the exact same DevContainer instance. Client-server communication occurs over the local loopback interface (sc://localhost), eliminating Docker network bridge translation layers.
* Isolated Network & Storage: DevContainer unit testing is completely sandboxed from the internet. All Docker Compose storage volumes are configured as entirely anonymous and ephemeral (no host mounts) to maximize execution speed and remove cleanup overhead.

------------------------------
## 2. Testing Gates & Lifecycles## I. Pre-commit Gate (Stateless Logic Verification)

* When: Automatic Git pre-commit hook.
* Where: Inside the running local DevContainer.
* Performance: Execution target under 5 seconds.
* Scope:
* Code quality checks (ruff check, ruff format, and mypy type checking).
   * Scraper Parsers: Web extraction logic runs against local VCR Cassettes (captured network traffic). No live network requests are made.
   * Data Transformations: Both DuckDB SQL logic and PySpark business functions are verified via local mock data using the warm, running Spark Connect and local in-memory DuckDB instances over memory speeds.

## II. Pre-merge Gate (Stateful Infrastructure & Resilience Verification)

* When: Manual execution check run by the developer right before merging a branch or opening a pull request.
* Where: Local machine, communicating directly with the background Docker Compose stack.
* Performance: Execution target under 20 seconds (thanks to running services and ephemeral storage).
* Scope:
* Plumbing: Authenticating, executing metadata transactions, and writing data blocks directly into the local MinIO and Iceberg Catalog.
   * Idempotency Integration: Running a simulated pipeline slice twice to confirm MERGE/UPSERT logic handles duplicate inputs without duplicating records.
   * Negative Paths: Simulates infrastructure dropouts (e.g., stopping the MinIO container mid-job) to ensure graceful pipeline exception handling.

## III. Pipeline Runtime (Data Integrity Guard)

* When: During the execution of the main production Airflow DAG.
* Where: Production Spark cluster.
* Scope: Within the Spark Silver-load job, an Entity Resolution check runs. If unresolved entities are detected, rather than throwing a hard pipeline failure that stalls downstream tables, unresolved records are routed into a Dead Letter Queue (Quarantine Table) for asynchronous remediation.

## IV. Periodic Gate (Upstream Drift Observability)

* When: Scheduled Airflow Monitoring DAG running once daily (ideally 2 hours before the main production pipeline).
* Where: Live Production Environment.
* Scope:
* Smoke & Drift Testing: Playwright hits live external data sources to check for IP blocks and confirm the underlying DOM or API structure hasn't changed.
   * Failure Action: If a layout has shifted, the DAG task fails intentionally. No external alerts are fired. Instead, the error log explicitly prints the exact command required for remediation: ERROR: Upstream DOM drift detected. Run 'pytest --record-mode=rewrite' locally to refresh VCR cassettes.

------------------------------
## 3. Strategy Summary Matrix

| Stage | Trigger Environment | Data Source | Speed Target | Key Objective |
|---|---|---|---|---|
| pre-commit | Local DevContainer | VCR Cassettes / Local Spark / DuckDB | < 5 seconds | Verify internal code logic, syntax, formatting, type checks, DuckDB SQL, and PySpark transformations. |
| pre-merge | Local Host / Stack | Local Ephemeral MinIO & Iceberg | < 20 seconds | Validate end-to-end component plumbing and Spark Connect writes. |
| DAG Run | Production Pipeline | Live Ingested Batch | Production Scale | Route unresolved records to DLQ while passing clean data. |
| Periodic | Scheduled Daily DAG | Live External Web | Scheduled Interval | Detect upstream DOM changes or IP blocks; output VCR re-record instructions straight to the task logs. |

------------------------------
