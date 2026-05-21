## Unit Testing Specification
Testing Tier: Tier 1 Core Quality Gates (Scrapers, Airflow Orchestration, Warehouse Transforms)
## 1. Executive Summary & Design Principles
The Tier 1 testing engine enforces a strict, database-free, and air-gapped quality shield inside the development container [docs/test_approach_3.md]. The framework guarantees that every structural code file, network parsing routine, and data warehouse computation passes validation completely offline before code branch changes can be staged.
The architecture relies on a Compute-Split Testing Strategy [docs/test_approach_unit_tests.md]. Rather than using a single database engine to simulate the entire environment, every line of production logic is evaluated by the exact same runtime engine that executes it in production:

* Web Scrapers & Clients: Tested using VCR.py network cassettes to freeze and play back raw HTTP transactions securely without hitting live endpoints [docs/test_approach_unit_tests.md].
* Airflow Directed Acyclic Graphs: Tested completely in-memory using custom environment-aware object stubs to enforce strict pipeline rules without spinning up a heavy metadata database [docs/test_approach_unit_tests.md].
* Silver Warehouse Models (Job 1 & 2): Written in pure PySpark and executed against a local Spark Connect loopback daemon (sc://localhost:15002) to preserve 100% Spark syntax, type-casting, and Null-propagation parity [docs/test_approach_unit_tests.md].
* Gold Serving Views (Job 3): Tested natively using an in-memory DuckDB connection, matching the exact vectorised engine that serves analytics queries to dashboards in production [docs/test_approach_unit_tests.md].

------------------------------
## 2. Directory Layout Architecture
```sh
src/
├── dags/                                 # Airflow Scheduler Pipeline Blueprints
│   └── pl_lakehouse_daily.py             # Unified E2E Workflow (Bronze -> Silver -> Gold)
├── schemas/                              # Single Source of Truth Declarative DDL Files
│   ├── silver_fact_match.sql             # Table structure with Iceberg partitioning definitions
│   └── gold_efficiency_anomalies.sql     # Serving analytical layout view script
└── transforms/                           # Production Big Data Processing Engines
    ├── normalisation.py                  # Spark Job 1: Ingest & normalise to 3NF Core
    └── metrics.py                        # Spark Job 2: Advanced window lookback calculations
```
```sh
tests/unit/
├── conftest.py                           # Global session fixtures (Spark & DuckDB pointers)
├── scraper/                              # Ingress Ingestion Quality Gate Tests
│   ├── test_scraper_data_extraction.py   # Validates JSON object flattening and DOM grids
│   ├── test_scraper_network_handshake.py # Validates HTTP resilience via frozen VCR cassettes
│   └── test_scraper_utils.py             # Validates batch deduplication and string trimming
├── orchestration/                        # Infrastructure Scheduler Quality Gate Tests
│   └── test_dags.py                      # Validates deadlocks, timeouts, and backoffs in memory
└── transforms/                           # Warehouse Core Processing Quality Gate Tests
    ├── conftest.py                       # Function-scoped unseeded table loader and seeder
    ├── test_entity_resolution_*.py       # Validates team and player name governance joins
    ├── test_scd2_injuries.py             # Validates historical timeline backfill stitching
    ├── test_silver_metrics.py            # Validates 5-match rolling windows and performance drag
    ├── test_silver_transformations.py    # Validates 3NF unique composite fact constraints
    ├── test_idempotency_*.py             # Validates multi-run no-op safety across Spark & DuckDB
    └── test_gold_queries.py              # Audits raw `.sql` view files directly from disk
```
------------------------------
## 3. Layered Compute Mapping & Testing Strategy
### Category A: Scraper & Ingestion Components (test_scraper_*.py)

* Infrastructure: Python 3.12, PyTest-Recording (vcr), BeautifulSoup4.
* Strategy: Network handshake tests intercept outbound socket calls and map them to static, version-controlled YAML cassettes [pyproject.toml]. This verifies that connection clients successfully handle website handshakes, cookies, and network errors (like 429 rate limits or server outages) with zero live traffic. Data extraction tests pass mock payloads into parsers to ensure deep JSON blocks and HTML grid rows flatten cleanly into uniform structures, dropping corrupted batches before data lands in the staging folder.

### Category B: Airflow Orchestration Components (test_dags.py)

* Infrastructure: Apache Airflow Core, Unittest.mock.
* Strategy: To keep tests database-free and fast, the framework bypasses heavy metadata lookups. When running locally under a unit-testing Docker Compose profile, the script catches missing provider modules (airflow.providers.apache) on the fly, substituting a custom operator stub that allows the SparkSubmitOperator to load in memory. To ensure code safety, a depth-first search (DFS) algorithm traverses task dependencies to catch circular deadlocks before deployment. Finally, a parameter resolver helper scans task configurations to confirm all data collection steps have a strict 1-minute timeout and exponential backoff retries actively assigned.

### Category C: Silver Data Transformations (test_silver_*.py / test_scd2_*.py / test_entity_*.py)

* Infrastructure: PySpark Connect Core, Local gRPC Daemon Port 15002.
* Strategy: Data cleaning and history-stitching algorithms are fed mock PySpark Row lists to validate data warehouse operations. Entity resolution tests check that raw names match smoothly with governance mapping tables. Slowly Changing Dimension (SCD) Type 2 tests verify that the timeline logic handles open-ended records, state transitions, and historical backfills cleanly without creating data gaps. Fact table tests enforce composite primary key constraints to block intra-batch duplication, and idempotency tests run the jobs consecutively to verify that a secondary execution functions as a safe no-op.

### Category D: Serving Views Analytics (test_gold_queries.py)

* Infrastructure: DuckDB Vectorised Columnar Engine.
* Strategy: To prevent query copies from drifting from production code, test_gold_queries.py avoids hardcoding SQL text. Instead, a regular expression anchor utility reads actual production .sql files directly from src/schemas/ at runtime, strips out distributed cloud keywords (USING iceberg), and runs the raw views against an empty database canvas. This confirms the exact serving queries join tables correctly and compute metrics—such as expected points mismatch differentials—accurately.

------------------------------
## 4. Operational Environment Constraints
### Rule 1: Automated Table Provisioning (transforms/conftest.py)
To isolate tests and prevent data from leaking between modules, tests/unit/transforms/conftest.py uses a function-scoped hook (scope="function"). Every time a test function runs, this fixture programmatically loops over the schema folder, drops all existing entities using a CASCADE statement, and re-creates clean, empty table layouts on a blank canvas. This ensures the Silver transformations always begin with an empty table to test cold-start initializations accurately.
### Rule 2: Separation of Seed Data
Data is never seeded automatically. The Gold serving views tests must explicitly include the function-scoped seeded_metrics_data fixture parameter inside the individual function signatures. This inserts mock data rows on demand only for that localized view layout test, completely protecting the upstream normalization and cold-start tests from data contamination.
### Rule 3: Single Source of Truth Query Testing
The Gold queries must stay abstracted in src/schemas/. The view loader utility parses the file string, normalizes whitespace, and extracts only the text following the top-level CREATE VIEW AS signature. This isolates the inner query body and ensures the tests evaluate actual production scripts, meaning any broken syntax on disk will instantly trigger a local git hook failure.

------------------------------
## 5. Master Architecture Test Registry Matrix (93-Check Gate)
The following matrix documents the complete execution footprint of the local pre-commit inner loop, fanning out across all 12 test files to run 93 distinct, isolated validations in 3.93 seconds:

| ID Block | Category | Subcategory / File Target | Key Assertion Scope | True Test Count |
|---|---|---|---|---|
| U-DAG-xx | Airflow | orchestration/test_dags.py | Traps deadlocks, task identity collisions, and 1-min timeout defenses | 2 |
| U-EXT-xx | Scraper | scraper/test_scraper_data_extraction.py | Parameterises deep JSON and HTML positional table structures | 31 |
| U-NET-xx | Scraper | scraper/test_scraper_network_handshake.py | Playback verification of HTTP contracts and rate limits | 14 |
| U-UTL-xx | Scraper | scraper/test_scraper_utils.py | Checks core deduplication, string sanitisation, and column cleaning | 9 |
| U-ENT-xx | Data Transformation | transforms/test_entity_resolution_players.py | Validates player text name joins over Spark Connect | 2 |
| U-ENT-xx | Data Transformation | transforms/test_entity_resolution_teams.py | Validates club text name joins over Spark Connect | 2 |
| U-GLD-xx | Serving Views | transforms/test_gold_queries.py | Runs regex view code file extractions from disk | 3 |
| U-IDM-xx | Data Transformation | transforms/test_idempotency_duckdb_queries.py | Audits rerun view deployment mechanics | 3 |
| U-IDM-xx | Data Transformation | transforms/test_idempotency_spark_jobs.py | Verifies multi-run dataset parity across both Spark jobs | 6 |
| U-SCD-xx | Data Transformation | transforms/test_scd2_injuries.py | Hardens chronological timeline state transitions | 6 |
| U-MTR-xx | Data Transformation | transforms/test_silver_metrics.py | Evaluates rolling windows, days lost sums, and drag scores | 8 |
| U-FCT-xx | Data Transformation | transforms/test_silver_transformations.py | Enforces 3NF structural boundaries and unique constraints | 7 |
| Total Gate | | | Unified Local Pre-Commit Quality Shield | 93 |

------------------------------

