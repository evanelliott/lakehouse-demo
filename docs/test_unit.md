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
| U-SCR-01,02 | Extraction | Positive | Matches Expected Object |
| U-SCR-03,04 | DOM Drift | Negative | Raises SourceDataMissingError |
| U-SCR-05,06 | Data Duplication | Negative | Raises IntraBatchDuplicateError |
| U-ENT-01,03 | ER Success | Positive | Returns correct team_id |
| U-ENT-02,04 | ER Gate | Negative | Raises UnresolvedEntityError |
| U-SCD-01,02,03 | SCD2 Logic | Positive | Handles New and Status Transitions |
| U-SCD-04 | Backfills | Positive | Correctly "stitches" historical records |
| U-SCD-05,06 | Timeline | Negative | Raises TimelineException / FutureDateError |
| U-IDM-01 | Idempotency | Positive | Second execution is a No-Op |

------------------------------
## Detailed Unit Test descriptions
| ID | Name | Area | Type | Gherkin |
|---|---|---|---|---|
| U-SCR-01 | Understat Match Extraction | Scraper | Positive | Given a valid Understat HTML snapshot When the parser runs Then the output dictionary must match a hard-coded "Expected Object" representing the snapshot's data. |
| U-SCR-02 | PremierInjuries Table Extraction | Scraper | Positive | Given a valid PremierInjuries HTML snapshot When the parser runs Then the output list must match a hard-coded "Expected List" representing the snapshot's data. |
| U-SCR-03 | Understat DOM Contract Change | Scraper | Negative | Given an Understat page where the data script tag is missing/renamed When the selector fails to find the block Then it should raise a SourceDataMissingError. |
| U-SCR-04 | PremierInjuries DOM Contract Change | Scraper | Negative | Given a PremierInjuries page where the table classes have changed When the row selector returns 0 items Then it should raise a SelectorNotFoundError. |
| U-SCR-05 | Understat Intra-Batch Duplicate | Scraper | Negative | Given an Understat match report where an entity (e.g. a specific shot) appears twice When the parser extracts the payload Then it must raise an IntraBatchDuplicateError and halt extraction. |
| U-SCR-06 | PremierInjuries Intra-Batch Duplicate | Scraper | Negative | Given a PremierInjuries page where a player is listed twice in the same injury table When the parser extracts the payload Then it must raise an IntraBatchDuplicateError and halt extraction. |
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
