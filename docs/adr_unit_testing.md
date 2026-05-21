# Architecture Decision Record (ADR): Tier 1 Local Unit Testing Architecture
## Context and Problem Statement
The platform orchestrates an end-to-end data lakehouse pipeline spanning multiple ingestion formats, cloud scheduling graphs, and complex multi-job data transformations. Running validations directly against live web endpoints, distributed Apache Spark clusters, or active metadata databases slows down local development velocity. It also creates environment instability and can cause hidden test regressions because individual engine behaviours differ (such as Null-propagation, type casting, and window framing).
A testing strategy is required to catch bugs locally within the development container entirely offline, executing in seconds while guaranteeing absolute code validity before commits are staged.

## Decision Drivers
* Execution Velocity: The entire local pre-commit quality gate must execute under a strict 4-second time limit to preserve a fast inner developer loop.
* Compute Parity: Data transformation rules must be audited by runtime engines matching production exactly to eliminate behavior blind spots.
* Zero Code Duplication: SQL queries must be tested directly from version-controlled production files rather than maintaining separate copies inside test blocks.
* Absolute Test Isolation: A clean table canvas must be guaranteed between test runs to verify cold-start schema initialisations accurately.

## Considered Options
   1. Option 1: Heavy Shared Integration Testing – Run all tests against a shared test database and a Dockerized local Spark cluster.
   2. Option 2: Single-Engine Mocking (DuckDB Simulating All) – Use an in-memory DuckDB database to test all scraper data, orchestration paths, and Spark transformations.
   3. Option 3: Stateless Compute-Split Architecture (Selected) – Separate testing strategies across multiple lightweight, isolated local engines matching production targets exactly.

## Decision Outcome
Chosen Option: Option 3: Stateless Compute-Split Architecture. This strategy separates verification across a 93-check quality gate, using the optimal engine for each architectural layer to balance speed with production parity.

## Implementation Details
```sh
                      ┌──────────────────────────────────────────────┐
                      │          VS Code Sandbox Container           │
                      │                                              │
                      │     ┌───────────────┐        ┌───────────┐   │
                      │     │  PyTest Core  │        │   Spark   │   │
                      │     └───────┬───────┘        │  Connect  │   │
                      │             │                └─────▲─────┘   │
                      │             │                      │ gRPC    │
                      │             │                      │ :15002  │
                      └─────────────┼──────────────────────┼─────────┘
                                    │                      │
          ┌─────────────────────────┴──────────────────────┼─────────────────────────┐
          ▼ (DuckDB In-Memory)                             ▼ (PySpark Connect Core)  
 ┌─────────────────────────────────┐              ┌─────────────────────────────────┐
 │       test_gold_queries.py      │              │ test_silver_transformations.py  │
 │   - Extracts src/schemas/*.sql  │              │  - Runs src/transforms/*.py DF  │
 │   - Validates serving views     │              │  - Validates SCD2/Lookback API  │
 └─────────────────────────────────┘              └─────────────────────────────────┘
```
### 1. Ingress Scraper Verification (54 Test Cases)
- Web ingestion scripts are decoupled from active servers using VCR.py network cassettes. Outbound HTTP handshakes are frozen into static, version-controlled YAML files. This permits testing client logic against real-world server responses, 429 rate limit errors, and gateway drops completely offline.
- BeautifulSoup and JSON extraction functions are verified against local data structures to ensure fields map cleanly to Bronze schemas.
### 2. Airflow Orchestration Verification (2 Test Cases)
- Pipeline topologies are analyzed 100% database-free in memory.
- Circular deadlocks are trapped using a depth-first search (DFS) dependency tree traversal.
- A unified parameter resolution helper scans the task hierarchy to confirm all collection operators inherit a strict 1-minute execution timeout and active exponential backoff retry profiles.
### 3. Silver Warehouse Model Verification (34 Test Cases)
- Data cleaning, entity resolution joins, Slowly Changing Dimensions (SCD) Type 2 timeline stitching, and fact constraints are written in pure PySpark. These scripts are executed over a local Spark Connect gRPC loopback daemon (sc://localhost:15002). This prevents engine-specific behavior discrepancies and guarantees 100% Spark syntax, type-casting, and Null-propagation parity.
- To prevent data contamination across tests, a function-scoped conftest.py fixture automatically intercepts the execution loop, drops historical database schemas using a CASCADE command, and provisions empty, clean metadata tables before every single individual test function runs.
### 4. Gold Serving View Verification (3 Test Cases)
- Analytical layout query verification runs inside a stateless, in-memory DuckDB database context. To eliminate code duplication, queries are not hardcoded inside test scripts. Instead, a regular expression anchor utility reads production .sql files directly from src/schemas/ at runtime, strips out Big Data infrastructure keywords (USING iceberg, PARTITIONED BY), and compiles the views on demand against empty test tables populated by explicit function fixtures.

## Consequences

* Good: The entire 93-check test framework executes completely offline inside the development container in 3.93 seconds.
* Good: Testing Spark code via PySpark Connect and Gold views via DuckDB ensures complete compute parity with production.
* Good: Abstracting DDL/DML code to src/schemas/ provides a single source of truth, guaranteeing any syntax errors on disk break local git hooks immediately.
* Bad: The test suite requires maintaining an offline schema-cleaning regular expression utility within the local testing conftest.py file to handle dialect variations between Iceberg and DuckDB.

------------------------------


