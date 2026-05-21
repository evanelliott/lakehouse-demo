Here is your comprehensive, production-grade Architectural Specification for the Data Warehouse Testing Layer.
This document defines the stateless, dual-engine testing strategy that keeps your pre-commit gates running under 3 seconds while guaranteeing 100% compute parity with your production Apache Spark and DuckDB environments [docs/test_approach_3.md, docs/test_approach_unit_tests.md].
------------------------------
## Data Warehouse Testing Strategy & Technical Specification
Data Warehouse Layer: Tier 1 Core Transformations (Silver Normalisation, Silver Metrics, Gold Serving)
## 1. Executive Summary & Design Principles
This framework enforces an air-gapped, stateless testing loop designed to catch regression anomalies inside your DevContainer in milliseconds without external database dependencies [docs/test_approach_3.md, docs/test_approach_unit_tests.md].
The architecture relies on a Compute-Split Testing Strategy. Rather than forcing one database engine to simulate the entire lakehouse, every line of production code is tested by the exact same runtime engine that executes it in production. This eliminates engine-specific behavior blindsided traps (e.g. Null handling, window boundary defaults, casting differences) while protecting your 16GB host RAM performance limits [pyproject.toml].

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
│   - Extracts src/schemas/*.sql  │              │  - Runs src/warehouse/*.py DF   │
│   - Validates serving views     │              │  - Validates SCD2/Lookback API  │
└─────────────────────────────────┘              └─────────────────────────────────┘

------------------------------
## 2. Directory Layout Architecture

src/
├── schemas/                            # Central Single Source of Truth DDL/DML Registry
│   ├── silver_dim_injury.sql           # Base SCD2 layout (Unpartitioned Iceberg)
│   ├── silver_fact_match.sql           # Transactional baseline (Partitioned by season)
│   ├── silver_fact_shot.sql            # Event baseline (Partitioned by bucket hash)
│   └── gold_efficiency_anomalies.sql   # Serving analytical view SQL file
│
└── warehouse/                          # Production Transformation Engines Modules
    ├── normalisation.py                # Spark Job 1: Ingest & Standardise to 3NF Core
    └── metrics.py                      # Spark Job 2: Advanced Window Metric Precomputations

tests/unit/
└── warehouse/                          # Isolated Warehouse Quality Assurance Folder
    ├── conftest.py                   # Module-scoped DDL Provisioner & Sandbox Seeder
    ├── test_silver_transformations.py # Audits Job 1 Core via PySpark Connect Core (U-SCD/U-ENT)
    ├── test_silver_metrics.py         # Audits Job 2 Metrics via PySpark Connect Core (U-MTR)
    └── test_gold_queries.py           # Audits Job 3 Serving Layouts via DuckDB (U-GLD)

------------------------------
## 3. Tiered Compute Mapping & Testing Strategy## Layer 1: Pure Normalized Silver (Spark Job 1 $\rightarrow$ U-SCD, U-ENT, U-FCT)

* Production Engine: Apache Spark 3.5 executing writes to Apache Iceberg metadata catalogs.
* Testing Infrastructure: PySpark Connect Local Loopback Daemon (sc://localhost:15002) [tests/unit/conftest.py].
* Strategy: Raw extracted dataframes are routed into your normalisation.py functions. The test script verifies entity matching loops against governance boundaries, initial cold-start SCD2 setups, and backfill stitching completely offline with real Spark behavior [docs/test_approach_unit_tests.md].

## Layer 2: Reusable Silver Metrics (Spark Job 2 $\rightarrow$ U-MTR)

* Production Engine: Apache Spark 3.5 processing background window metrics lookup tasks.
* Testing Infrastructure: PySpark Connect Local Loopback Daemon.
* Strategy: Isolates your metrics definitions (e.g., rolling 5-match expected goals drag or career total days lost summaries) into independent tables. Tests evaluate complex unbounded running sums and bounded historical window arrays natively.

## Layer 3: Gold Serving Analytics (Serving View Engine $\rightarrow$ U-GLD)

* Production Engine: DuckDB Columnar Vectorised Engine query queries over Silver Parquet layers.
* Testing Infrastructure: In-Memory Stateless DuckDB Instance (duck_con) [tests/unit/conftest.py].
* Strategy: To achieve 100% query accuracy without code duplication, test_gold_queries.py reads your actual production files from src/schemas/ at runtime, strips out Big-Data parameters (USING iceberg), and executes the exact view SQL against an in-memory database mock canvas [src/schemas/gold_player_resilience_index.sql].

------------------------------
## 4. Warehouse Test Case Registry## 🥈 Silver Ingestion & Normalisation (Job 1)

| Test ID | Focus Area | Type | Expected Behaviour / Key Assertion |
|---|---|---|---|
| U-ENT-01 | Player Resolution | Positive | Matches source text names to Golden IDs via your Governance map layer. |
| U-ENT-02 | Unmapped Entity | Negative | Finding an unmapped source profile drops execution and raises an UnresolvedEntityError. |
| U-SCD-01 | Cold-Start Init | Positive | Initializes a brand new injury timeline interval with open-ended 9999-12-31 dates. |
| U-SCD-02 | Chronological Stitch | Positive | Pre-dated backfill updates split old intervals without creating data gaps. |
| U-SCD-03 | Timeline Collision | Negative | Overlapping interval date records inside an incoming batch raise an SCD2TimelineException. |
| U-SCD-04 | Future Boundary | Negative | Ingesting records dated past the active logical context runtime raises a FutureDateError. |
| U-FCT-01 | Composite Uniqueness | Negative | Duplicate composite natural keys inside fact tables drop the execution batch, throwing an IntraBatchDuplicateError. |

## 📊 Silver Metrics Lookback Precomputations (Job 2)

| Test ID | Focus Area | Type | Expected Behaviour / Key Assertion |
|---|---|---|---|
| U-MTR-01 | Spell Duration | Positive | Calculates exact single-spell length via DATEDIFF, clamping active records safely to the processing execution date context. |
| U-MTR-02 | Cumulative Career | Positive | Computes a running sum (ROWS UNBOUNDED PRECEDING) of career days sidelined up to each unique record date context. |
| U-MTR-03 | Availability Index | Positive | Derives an availability_coefficient factor (0.00 to 1.00) based on career days lost vs total calendar season days. |
| U-MTR-04 | Sidelined Headcount | Positive | Evaluates absolute headcount counts of unavailable players for a specific fixture date context by joining injury timelines with rosters. |
| U-MTR-05 | Player Baseline | Positive | Computes an individual's prior performance baseline using a bounded 5-match lookback window (ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING). |
| U-MTR-06 | Attacking Drag Sum | Positive | Aggregates individual historical baselines across all injured players to calculate the team's total attacking metric deficit (total_squad_xg_drag). |
| U-MTR-07 | Defensive Stability | Positive | Aggregates missing performance values across defensive nodes to calculate the total squad stability penalty score metric (total_squad_xga_drag). |
| U-MTR-08 | Future Context Guard | Negative | Passing processing context run-dates that are younger than the data's timelines halts execution, raising a FutureDateError. |

## 🥇 Gold View Layout Assertions (Job 3)

| Test ID | Target View | Focus Area | Type | Expected Behaviour / Key Assertion |
|---|---|---|---|---|
| U-GLD-01 | player_resilience_index | View Presentation | Positive | Flattens injury metrics to group and rank active player availability profiles without metrics duplication. |
| U-GLD-02 | squad_health_drag | View Presentation | Positive | Flattens match facts to isolate attacking drag values alongside actual points. |
| U-GLD-03 | efficiency_anomalies | View Presentation | Positive | Computes pts - xpts performance mismatch differentials and outputs them side-by-side with squad drag scores. |

------------------------------
## 5. Governance & Operational Environment Constraints## Rule 1: Isolation and Clean Slate Mandate (warehouse/conftest.py)
To ensure an absolute clean slate between test modules, your directory uses a module-scoped hook (scope="module"). At the start of every test file, it programmatically loops over src/schemas/, drops all historical tables/views via a CASCADE statement, and re-provisions clean, unseeded structures. This ensures Silver initialization tests always start with 100% empty tables.
## Rule 2: Explicit Data Seeding Separation
Data is never seeded automatically. Your Gold layer views tests must explicitly pass your function-scoped seeded_metrics_data fixture parameter into their individual test arguments. This inserts target mock metrics profiles on demand just for that localized check, completely protecting your upstream normalisation tests from data contamination.
## Rule 3: File-Driven Query Extraction
To ensure your unit tests audit your actual production code scripts, test_gold_queries.py uses an internal .split(" AS ") text extractor module. It slices out the inner query body from your production view file at runtime, executing the raw logic directly against your test data canvas. This guarantees any syntax error introduced to a file on disk instantly breaks your local git hooks before code reaches your main branch repositories.
------------------------------
## 🚀 What Component Are We Hardening Next?
This completes the entire technical specification for your warehouse data quality assurance firewall. Every tier is fully mapped, type-safe, and ready to go.
Would you like to proceed with writing the actual production code for your Job 1 core 3NF transformations (src/warehouse/normalisation.py), or look at configuring the Airflow scheduler pipeline tasks next? Use the guidelines to advance our journey!

