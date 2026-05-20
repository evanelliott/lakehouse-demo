# Unit Test Specification
This specification defines the Unit Testing & Logic Gate for the Lakehouse. It ensures all scraper and transformation logic is verified in a stateless, air-gapped environment before code is pushed.

------------------------------
## 1. Architectural Principles

* Statelessness: Tests must not require any external services (No Docker, No MinIO, No Internet).
* Engine Parity: Use Spark Connect (remote gRPC) for all Spark logic to ensure 100% parity with production without JVM startup latency.
* Visibility: Mock data for transformations must be hard-coded within the test files to ensure the "Given" state is immediately readable.
* Fail-Fast: Any failure in extraction (DOM) or business logic (ER/SCD2) results in a non-zero exit code, blocking the Git commit.

------------------------------
## 2. Directory Structure
```sh
tests/unit/
├── conftest.py                   # GLOBAL CONFIG & SPARK FIXTURE
├── scraper/
│   ├── fixtures/                 # Static .html snapshots of source websites
│   ├── test_understat.py         # Parser logic for Understat
│   └── test_premier_injuries.py  # Parser logic for PremierInjuries
├── scd2/
│   └── test_injuries.py          # History stitching & Timeline logic
├── entity_resolution/
│   ├── test_teams.py             # Standardise Team ID across sources
│   └── test_players.py           # Standardise Player ID across sources
└── idempotency/
    ├── test_spark_jobs.py        # Global Spark idempotency contract
    └── test_duckdb_queries.py    # Global DuckDB idempotency contract
```
------------------------------
## 3. Testing Categories
## A. Scraper Parsers (DOM & Extraction)
Tests the Python logic required to navigate HTML and extract a raw dictionary.

* Input: Static .html files located in scraper/fixtures/.
* Assertion: Equality check against a hard-coded "Expected Object."
* Hard-Fail: IntraBatchDuplicateError if an entity appears twice in one scrape.

## B. SCD Type 2 (Temporal Logic)
Tests the "Smart Merge" (Option B) logic used to manage injury history.

* Key Logic: Timeline "stitching" for backfills and "no-op" for unchanged status.
* Hard-Fail: SCD2TimelineException for logical overlaps and FutureDateError for post-dated records.

## C. Entity Resolution (ER)
Tests the strict mapping between raw source names and the internal team_id.

* Input: Mock Spark DataFrames created in-test.
* Hard-Fail: UnresolvedEntityError if a name is missing from the lookup table.

## D. Global Idempotency Contract
A mandatory pattern for every transformation function.

* The Logic: A "Double-Run" where the output of the first pass is fed back into the same function.
* Assertion: The second pass must yield an identical state (byte-for-byte).

------------------------------
## 4. Technical Implementation
## Spark Connect Fixture
To avoid the 20-second "Spark Tax" during pre-commit, tests connect to a background Spark service.
```python
# tests/conftest.py
@pytest.fixture(scope="session")def spark():
    return RemoteSparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```
## Idempotency Pattern Template
```python
def test_job_idempotency(spark):
    initial_target = spark.createDataFrame([...], schema=SCHEMA)
    batch = spark.createDataFrame([...], schema=SCHEMA)

    # Pass 1: Initial Write
    first_run = my_transform_job(initial_target, batch)
    
    # Pass 2: Re-run (Contract Check)
    second_run = my_transform_job(first_run, batch)
    assert first_run.collect() == second_run.collect()
```
------------------------------
## 5. Test Case Registry (The Gate)

| ID | Focus | Type | Key Assertion |
|---|---|---|---|
| U-ENT-01,03 | ER Success | Positive | Returns correct team_id |
| U-ENT-02,04 | ER Gate | Negative | Raises UnresolvedEntityError |
| U-SCD-01,02,03 | SCD2 Logic | Positive | Handles New and Status Transitions |
| U-SCD-04 | Backfills | Positive | Correctly "stitches" historical records |
| U-SCD-05,06 | Timeline | Negative | Raises TimelineException / FutureDateError |
| U-IDM-01 | Idempotency | Positive | Second execution is a No-Op |

------------------------------
## Data Transformation Unit Test descriptions
| ID | Name | Area | Type | Gherkin |
|---|---|---|---|---|
| U-ENT-01 | Team ID Success | Entity Resolution | Positive | Given a raw input DataFrame and a lookup DataFrame fixture When an equality join is performed via Spark Connect Then the resulting DataFrame should contain the mapped team_id. |
| U-ENT-02 | Team ID Hard-Fail | Entity Resolution | Negative | Given an input DataFrame with a name missing from the lookup DataFrame When the transformation logic detects a null join result Then it must raise an UnresolvedEntityError immediately. |
| U-ENT-04 | Player ID Success | Entity Resolution | Positive | Given a raw input DataFrame and a lookup DataFrame fixture When an equality join is performed via Spark Connect Then the resulting DataFrame should contain the mapped player_id. |
| U-ENT-04 | Player ID Hard-Fail | Entity Resolution | Negative | Given an input DataFrame with a name missing from the lookup DataFrame When the transformation logic detects a null join result Then it must raise an UnresolvedEntityError immediately. |
| U-SCD-01 | SCD2 New Player Initialization | SCD2 | Positive | Given a player that doesn't exist When processed Then it creates a new record with valid_from = logical_date and an open valid_to. |
| U-SCD-02 | SCD2 Injury Status Unchanged | SCD2 | Positive | Given a batch that matches the current active state in the timeline When processed via Spark Connect Then no rows are changed or created (No-op). |
| U-SCD-03 | SCD2 Injury Status Transition | SCD2 | Positive | Given a player status change (e.g. "Fit" to "Injured") When processed Then it must close the active record and open a new one with no temporal gaps. |
| U-SCD-04 | SCD2 Historic "Stitch" | SCD2 | Positive | Given an incoming record with a date that falls before the current active record When processed Then it should update the historical valid_to and re-stitch the timeline to reflect the new "truth". |
| U-SCD-05 | SCD2 Injury Timeline Discrepancy | SCD2 | Negative | Given an incoming record that creates a logical conflict with an existing timeline (e.g. overlapping active periods with different statuses) When processed Then it must raise an SCD2TimelineException to prevent history corruption. |
| U-SCD-06 | SCD2 Injury Future-Date Gate | SCD2 | Negative | Given a record with an effective_date that is in the future relative to the processing date When processed Then it should raise a FutureDateError and halt the Spark job. |
| U-IDM-01 | Global Idempotency Contract | Idempotency | Positive | Given the transformation function transform_func When it is executed twice against the same mock data via Spark Connect Then the second run must yield a state identical to the first. |

------------------------------
...d
------------------------------

# Scraper Unit Test Specification Matrix.
It reflects your three-character module architecture (UTL, NET, SCR), isolates your shared core utilities, splits your positive extraction tests, parameterizes negative gates, and enforces your required error-handling responses.

------------------------------
## 🛠️ 1. Shared Core Utilities Test Registry (U-UTL-xx)

* Scope: Universal data validation and sanitisation engines.
* Execution: Tested once globally; shared across all ingestion pipelines to guarantee DRY compliance.

| ID | Focus Area | Type | Expected Behaviour / Key Assertion |
|---|---|---|---|
| U-UTL-01 | Full Duplicates | Positive | Identical overlapping dictionary rows are cleanly deduplicated without alerts. |
| U-UTL-02 | PK Duplicates | Negative | Primary key collisions with differing attributes raises an IntraBatchDuplicateError. |
| U-UTL-03 | String Sanitisation | Positive | Central sanitiser cleanly trims trailing \n line breaks and strips HTML whitespaces. |

------------------------------
## 📡 2. Network Handshake Unit Test Registry (U-NET-xx)

* Scope: Cookie lifecycle execution, custom browser header compliance, and network status gates.
* Execution: 100% offline verification utilizing pre-recorded VCR cassettes via pytest-recording.

| ID | Data Source | Focus Area | Type | Expected Behaviour / Key Assertion |
|---|---|---|---|---|
| U-NET-01 | Understat | Handshake Success | Positive | Returns a 200 OK status code with the correct JSON mime-type payload. |
| U-NET-02 | Understat | Session Rejected | Negative | Intercept non-200 response and raise explicit security exception. |
| U-NET-03 | Understat | Header Drop | Negative | Catch the 403 forbidden status code and raise a clear exception. |
| U-NET-04 | Understat | Rate Limited | Negative | Detect the rate limit, reject the payload, and raise a specific exception. |
| U-NET-05 | Understat | Server Outage | Negative | Catch the timeout or server error and raise a clear exception. |
| U-NET-06 | Premier Injuries | Handshake Success | Positive | Returns a 200 OK status code with the correct HTML text payload. |
| U-NET-07 | Premier Injuries | Header Drop | Negative | Catch the 403 forbidden status code and raise a clear exception. |
| U-NET-08 | Premier Injuries | Rate Limited | Negative | Detect the rate limit, reject the payload, and raise a specific exception. |
| U-NET-09 | Premier Injuries | Server Outage | Negative | Catch the timeout or server error and raise a clear exception. |

------------------------------
## ⚽ 3. Data Extraction Unit Test Registry (U-SCR-xx)

* Scope: Payload envelope validation, dictionary mapping, schema stability, and DOM layout drift monitoring.
* Execution: 100% stateless execution utilizing local static fixture snapshots (.json and .html).

| ID | Data Source | Focus / Dataset | Type | Expected Behaviour / Key Assertion |
|---|---|---|---|---|
| U-EXT-01 | Understat | Ingestion: League | Positive | Maps raw JSON to clean Bronze schemas for Teams, Players, and Dates. |
| U-EXT-02 | Understat | Ingestion: Match | Positive | Maps raw JSON to clean Bronze schemas for Rosters and Shots. |
| U-EXT-03 | All | Missing ID Key | Negative | Missing core envelope root keys in responses raises a SourceDataMissingError. |
| U-EXT-04 | All | Missing Fields | Negative | Missing internal dictionary schema items drops the batch and raises a ValueError. |
| U-EXT-05 | All | Empty Payload | Negative | Completely empty data arrays inside responses raises a ValueError. |
| U-EXT-06 | AllS | Schema Type | Negative | Mismatched data types that cannot be safely cast throws an explicit TypeError. |
| U-EXT-07 | Premier Injuries | Grid Ingestion | Positive | Maps DOM table structure rows cleanly into structured player injury records. |
| U-EXT-08 | Premier Injuries | Table Layout Drift | Negative | Table class or grid node modifications catch empty selector lists and raise SelectorNotFoundError. |

------------------------------
## 🚀 Ready to Transition to Implementation
Your architectural plans are complete. We have successfully addressed formatting boundaries, decoupled the networking layers, and mapped out edge cases.
Let me know how you would like to proceed:

* Do you want to implement the shared utility code for test_deduplication.py (U-UTL-01 to 03) using PyTest parameters?
* Or would you prefer to see the network mock structure for test_understat.py (U-NET-01 to 05)?


