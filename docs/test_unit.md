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
### Scraper Network Handshake

* Scope: Cookie lifecycle execution, custom browser header compliance, and network status gates.
* Execution: 100% offline verification utilizing pre-recorded VCR cassettes via pytest-recording.

### Scraper Parsers (DOM & Extraction)
Tests the Python logic required to navigate HTML and extract a raw dictionary.

* Input: Static .html files located in scraper/fixtures/.
* Assertion: Equality check against a hard-coded "Expected Object."
* Hard-Fail: IntraBatchDuplicateError if an entity appears twice in one scrape.

### SCD Type 2 (Temporal Logic)
Tests the "Smart Merge" (Option B) logic used to manage injury history.

* Key Logic: Timeline "stitching" for backfills and "no-op" for unchanged status.
* Hard-Fail: SCD2TimelineException for logical overlaps and FutureDateError for post-dated records.

### Entity Resolution (ER)
Tests the strict mapping between raw source names and the internal team_id.

* Input: Mock Spark DataFrames created in-test.
* Hard-Fail: UnresolvedEntityError if a name is missing from the lookup table.

### Global Idempotency Contract
A mandatory pattern for every transformation function.

* The Logic: A "Double-Run" where the output of the first pass is fed back into the same function.
* Assertion: The second pass must yield an identical state (byte-for-byte).

### Airflow DAGs
* Scope: Validates Python compilation graph topology, task configurations, and mathematical wiring.
* Environment: 100% in-memory execution inside the sandboxed container; completely database-free (read_dags_from_db=False).

------------------------------
## 4. Technical Implementation
TBC

------------------------------
## 5. Test Case Registry




------------------------------
## 🛡️ Local Test Suite Overview
The test suite runs inside the development container, executing 93 separate tests in under 4 seconds entirely offline. It uses a split strategy: Spark code is tested using a local Spark session, while SQL views are tested using an in-memory DuckDB database. This approach keeps the tests fast while ensuring the code behaves exactly as it will in production.
## 📡 Scraper Layer (54 Tests)
The scraper tests ensure that data collection from external websites is reliable and safe.
- The Network Handshake group checks that the code handles website connections, cookies, and common network issues like rate limits or connection dropouts.
- The Data Extraction group checks that the code correctly reads nested JSON data from Understat and HTML tables from Premier Injuries, preventing corrupted or incomplete data from slipping through. Finally, the Utilities group handles text cleaning and prevents duplicate records within the same batch.
## ⚙️ Data Transformation Layer (34 Tests)
The data transformation tests check the data cleaning and calculations that happen inside the data warehouse. These tests run on a local Spark session.
- The Entity Resolution tests confirm that raw team and player names match correctly to central governance IDs.
- The Slowly Changing Dimensions tests check that injury timelines are updated accurately when new information arrives, including handling historical backfills without creating timeline gaps or overlaps.
- The Fact Table tests block duplicate rows, while the Metrics and Idempotency tests ensure that complex lookbacks calculate correctly and that re-running a job produces identical results.
## 🥇 Serving Views Layer (3 Tests)
The serving views tests verify the final analytical views by running them inside an in-memory DuckDB database. To ensure the tests match production exactly, the test loader reads the actual .sql files directly from disk. It strips away big-data storage keywords and runs the queries against empty tables populated with mock data.
- This setup confirms that the calculations—such as points mismatch differentials—join the correct tables and output the right metrics.
## 🎈 Airflow Orchestration Layer (2 Tests)
The Airflow tests act as a guardrail for workflow scheduling and pipeline structure without needing a live metadata database.
- The code checks the pipeline in memory to catch circular dependency deadlocks before deployment. It also ensures that all DAG files load without syntax errors or task name collisions.
- Finally, it programmatically scans the workflow to confirm that all data collection tasks have a strict 1-minute timeout and an active retry strategy to handle transient network drops safely.




------------------------------
## 🏟️ Final Aggregated Platform Unit Test Registry (93-Check Gate)

| ID Block | Category | Subcategory / File Target | Key Assertion Scope | Test Case Count |
|---|---|---|---|---|
| U-NET-xx | Scraper | Network Handshake | Playback verification of HTTP contracts and anti-bot drops | 14 |
| U-EXT-xx | Scraper | Data Extraction | Parameterises deep JSON and BS4 positional table structures | 31 |
| U-UTL-xx | Scraper | Utils | Checks core deduplication, string sanitisation, and column cleaning | 9 |
| U-ENT-xx | Data Transformation | Entity Resolution | Validates ID standardisation joins over Spark Connect | 4 |
| U-SCD-xx | Data Transformation | SCD Type 2 | Hardens chronological timeline state transitions | 6 |
| U-FCT-xx | Data Transformation | Silver Transformations | Enforces 3NF structural boundaries and unique constraints | 7 |
| U-MTR-xx | Data Transformation | Silver Metrics | Evaluates rolling windows, days lost sums, and drag scores | 8 |
| U-GLD-xx | Data Transformation | Gold Queries | Runs regex view code file extractions from disk | 3 |
| U-IDM-xx | Data Transformation | Idempotency | Verifies multi-run dataset parity across both Spark jobs and DuckDB queries | 9 |
| U-DAG-xx | Airflow | orchestration/test_dags.py | Traps deadlocks, task identity collisions, and 1-min timeout defenses | 2 |
| Total Gate | | | Unified Local Pre-Commit Quality Shield | 93 |


------------------------------

| ID | Category | Subcategory | Group Name | Type | Expected Behaviour / Key Assertion | Gherkin |
|---|---|---|---|---|---|---|
| U-NET-01 | Scraper | Network Handshake | Inbound Handshake Success | Positive | Completes 2-step handshake, handles cookie jar tokens, and extracts 200 OK. Accepts application/json or text/javascript. | Given a network handshake request<br>When targeting Understat endpoints with cookies present<br>Then the client must handle session keys and return a 200 OK status. |
| U-NET-02,03,07 | Scraper | Network Handshake | Client Credential Validation Gates | Negative | Catches missing tokens, expired session cookies, or naked automated headers; throws client exceptions to block invalid requests. | Given an outbound connection request<br>When mandatory credentials or spoofing headers are omitted or expired<br>Then the system must immediately raise a specific client block exception. |
| U-NET-04,05,08,09 | Scraper | Network Handshake | Remote Infrastructure Circuit Breakers | Negative | Traps 429 rate limit status, remote network outages, connection drops, and server timeouts to prevent infinite loops. | Given automated worker threads executing loops<br>When remote hosts return 429 rate blocks or thread connection drops<br>Then the system must halt execution and throw an InfrastructureTimeoutError. |
| U-EXT-01,02,07 | Scraper | Data Extraction | Document Object Model Parsers | Positive | Flattens DOM grid cells, cell-decomposed rows, and deep nested JSON arrays into structured Bronze schemas over Spark Connect. | Given raw unstructured page tables or nested JSON files from sources<br>When evaluated by the targeted data parsing extraction engine<br>Then it must output uniform schemas for Teams, Players, Matches, and Injuries. |
| U-EXT-03,04,05,06,08 | Scraper | Data Extraction | Input Envelope Payload Firewalls | Negative | Drops corrupted batches and traps missing keys, uncastable types, empty page lists, or broken selectors to prevent downstream pollution. | Given a raw dataset batch containing invalid or missing schema layouts<br>When parsed against mandatory constraint structures or layout selectors<br>Then the engine must drop the data and throw a specific layout validation exception. |
| U-UTL-01,03 | Scraper | Utilities | String Sanitisation & Trimming | Positive | Trims trailing \n line breaks, removes non-breaking spaces (\u00a0), and combines overlapping identical dictionary rows with no alerts. | Given an extraction batch containing duplicate rows or messy text blocks<br>When passed through the global string sanitiser and deduplication utility<br>Then all records must be cleaned and combined seamlessly with no alerts. |
| U-UTL-02 | Scraper | Utilities | Intra-Batch Identity Drift Locks | Negative | Primary key collisions with mismatched attributes or drifting values trigger an immediate data quality alert. | Given a incoming ingestion batch mapping key collisions to differing attributes<br>When checked against primary key uniqueness constraints within the collection<br>Then the utility must halt execution and throw an IntraBatchDuplicateError. |
| U-ENT-01,03 | Data Transformation | Entity Resolution | Relational Name Matching Success | Positive | Performs equality joins over Spark Connect to resolve raw source variants (e.g. "K. De Bruyne") cleanly to structured Golden IDs. | Given a raw input performance DataFrame and a lookup governance fixture<br>When an equality join is processed over a local Spark Connect session<br>Then the result must map text attributes cleanly to uniform entity IDs. |
| U-ENT-02,04 | Data Transformation | Entity Resolution | Unresolved Identity Hard Fallbacks | Negative | Unmapped or unresolved entity profiles drop the active execution batch immediately to prevent data corruption. | Given an incoming batch payload with a player/team missing from lookups<br>When the identity logic processes the records and detects a null join result<br>Then the transformation block must raise an UnresolvedEntityError immediately. |
| U-SCD-01,02,03 | Data Transformation | Slowly Changing Dimensions | Temporal Timeline Lifecycle Logic | Positive | Initializes open-ended timelines, handles cold-starts (9999-12-31), executes no-ops, and manages closed interval state transitions. | Given a player injury timeline dimension and an active state status change<br>When evaluated through your silver_modelling history transformation job<br>Then it must seamlessly close active tracking records and open new open-ended rows. |
| U-SCD-04 | Data Transformation | Slowly Changing Dimensions | Chronological Backfill Stitching | Positive | Correctly "stitches" pre-dated historical records back into timeline gaps, expiring old boundaries without data gaps. | Given an incoming update record with an effective date older than active data<br>When processed over your historical Spark transformation logic lines<br>Then it must dynamically adjust historical boundaries and re-stitch the timeline. |
| U-SCD-05,06 | Data Transformation | Slowly Changing Dimensions | Sequential Interval Safety Gates | Negative | Overlapping active intervals or effective updates dated in the future raise anomalies to protect history tracking. | Given an update record creating date overlaps or extending into the future<br>When validated against chronological history boundary constraints<br>Then the engine must reject the batch and throw a SCD2TimelineException / FutureDateError. |
| U-FCT-01 | Data Transformation | Fact Table Integrity | Composite Uniqueness Protections | Negative | Enforces strict composite primary key unique constraints across Match, Shot, and Roster layers to block intra-batch duplication. | Given an incoming fact data batch containing duplicate composite keys<br>When evaluated against natural composite primary key constraints (player_id + match_id + xg)<br>Then the engine must drop the execution batch and raise an IntraBatchDuplicateError. |
| U-MTR-01,02,03 | Data Transformation | Metrics | Individual Player Injury Synthetics | Positive | Computes spell durations, career cumulative days lost, and derives normalized player availability coefficients (0.00 to 1.00). | Given open-ended active injury history timelines inside the database sandbox<br>When processed by a window partition running sum loop inside silver_analytics<br>Then it precomputes exact spell lengths, career totals, and coefficients. |
| U-MTR-04,05,06,07 | Data Transformation | Metrics | Team Squad Deficit Window Trackers | Positive | Computes bounded 5-match lookback windows, tracks missing headcounts, and calculates team attacking/defensive goals drag. | Given active injury dimensions, fixture rosters, and granularity tracking facts<br>When individual player historical baselines are summed by the metrics job<br>Then it outputs active missing headcounts and total squad performance drag scores. |
| U-MTR-08 | Data Transformation | Metrics | Processing Timeline Anchors | Negative | Passing processing clock parameters older than data timelines blocks execution to prevent historical corruption. | Given metrics precomputation lookup job configuration arguments<br>When an entry possesses a timeline timestamp younger than the execution clock<br>Then the system must flag an anomaly and throw an explicit FutureDateError. |
| U-GLD-01,02,03 | Data Transformation | Gold Queries | Serving views analytical layouts | Positive | Anchor regex parses view files on disk to validate analytical layout code, joining tables and views completely in-memory. | Given an unseeded, in-memory DuckDB database metadata sandbox canvas<br>When the extracted view SQL queries are executed directly from your source files<br>Then the result row tuples must group and flatten dimensions and calculate metrics exactly. 
| U-IDM-01 | Data Transformation | Idempotency | Engine Re-Run Reliability | Positive | Second execution yields an identical state with no state drift or duplicate row creation across both Spark and DuckDB execution runs. | Given an active data engineering transformation module function<br>When executed twice against the exact same data variables via Spark Connect<br>Then the second run output state must be completely identical to the first. |
| U-DAG-01 | Airflow | Directed Acyclic Graphs | Graph Deadlock Resolvers | Negative | DFS graph dependency tree search catches circular loop deadlocks completely in-memory without database lookups. | Given a parsed Airflow DAG structural graph mapping<br>When scanned via an in-memory topological DFS dependency traversal node search<br>Then the system must compile with zero circular deadlocks or return an error. |
| U-DAG-02,03 | Airflow | Directed Acyclic Graphs | Schema Compilation Checkpoints | Negative | Catches syntax import errors, missing provider dependencies, unmapped macro expressions, and task_id name collisions. | Given a local repository workspace containing workflow configuration files<br>When processed through an isolated in-memory DagBag directory parser instance<br>Then the environment must report exactly zero compilation import errors or collisions. |
| U-DAG-04 | Airflow | Directed Acyclic Graphs | Environment-Aware Boundary Mandates | Negative | Scopes profile boundaries to mandate 1-minute execution timeouts and exponential backoff retry profiles across ingestion and silver operators. | Given a compiled pipeline, a fallback dictionary, and a docker-compose profile<br>When auditing task parameters across the Airflow lookup hierarchy tree<br>Then all ingestion and silver operators must define a 1-minute timeout and backoff. |


------------------------------


------------------------------
## 📡 1. Scraper Layer

### Scraper - Network Handshake (NET)

| ID | Data Source | Focus Area | Type | Expected Behaviour / Key Assertion | Gherkin |
|---|---|---|---|---|---|
| U-NET-01 | Understat | Handshake Success | Positive | Completes 2-step handshake, handles cookie jar tokens, and extracts a 200 OK status code. Accepts application/json or text/javascript. | Given an active network client handshake request When targeting Understat endpoints with cookies present Then the client must handle session keys and return a 200 OK status. |
| U-NET-02 | Understat | Session Rejected | Negative | Intercepts expired or missing PHPSESSID arrays and raises a SecurityContractException. | Given an outbound connection request to Understat When the PHPSESSID token is missing or expired Then the network manager must throw a SecurityContractException. |
| U-NET-03 | Understat | Header Drop | Negative | Omitting X-Requested-With triggers an immediate local intercept block, raising a ScraperBlockedException. | Given an Understat AJAX transaction envelope When the mandatory X-Requested-With header is omitted Then the internal validator must raise a ScraperBlockedException. |
| U-NET-04 | Understat | Rate Limited | Negative | Detecting a 429 Too Many Requests response rejects empty data blocks and raises a ScraperBlockedException. | Given a high-frequency scrape loop targeting Understat When the remote server hits a WAF firewall threshold and throws a 429 Then the client must reject the block and raise a ScraperBlockedException. |
| U-NET-05 | Understat | Server Outage | Negative | Catching connection timeouts or 502/503 gateway drops halts execution and raises an InfrastructureTimeoutError. | Given an ingestion routine querying Understat When the server encounters connection timeouts or 503 drops Then the execution must stop and throw an InfrastructureTimeoutError. |
| U-NET-06 | Premier Injuries | Handshake Success | Positive | Passes browser User-Agent strings to verify a standard 200 OK response returning a text/html buffer. | Given an active connection task targeting Premier Injuries When authenticating using valid browser spoofing headers Then the system must return a 200 OK status code containing an HTML buffer. |
| U-NET-07 | Premier Injuries | Header Drop | Negative | Detecting naked automated requests blocks the client locally, raising a ScraperBlockedException. | Given a direct scrap loop hitting Premier Injuries When request parameters drop default browser spoofing signatures Then the client firewall must trigger and raise a ScraperBlockedException. |
| U-NET-08 | Premier Injuries | Rate Limited | Negative | Intercepts a 429 rate limit block, drops execution parameters, and raises a ScraperBlockedException. | Given an automated extraction scraper stream When Premier Injuries rate limits the worker threads with a 429 status code Then the scraper must discard parameter arrays and throw a ScraperBlockedException. |
| U-NET-09 | Premier Injuries | Server Outage | Negative | Catching 502 Bad Gateway responses or thread connection timeouts raises an InfrastructureTimeoutError. | Given a Premier Injuries data polling transaction task When socket handshakes crash on a 502 Bad Gateway error Then the execution loop must fail immediately and raise an InfrastructureTimeoutError. |

### Scraper - Data Extraction (EXT)

| ID | Data Source | Focus / Dataset | Type | Expected Behaviour / Key Assertion | Gherkin |
|---|---|---|---|---|---|
| U-EXT-01 | Understat | Ingestion: League | Positive | Maps raw JSON to clean Bronze schemas for Teams, Players, and Dates over Spark Connect. | Given raw un-nested Understat league JSON files When processed through the extraction ingestion client Then it must output uniform Bronze schemas for Teams, Players, and Dates. |
| U-EXT-02 | Understat | Ingestion: Match | Positive | Maps raw JSON to clean Bronze schemas for Rosters and Shots over Spark Connect. | Given deep nested Home/Away match JSON files from Understat When mapped over a local loopback Spark Connect server session Then it must correctly flatten and structure Rosters and Shots. |
| U-EXT-03 | All | Missing ID Key | Negative | Missing core envelope root keys or structural table layout markers in responses raises a SourceDataMissingError. | Given an un-structured response data envelope payload When mandatory root keys or table layout blocks are absent Then the engine must stop execution and throw a SourceDataMissingError. |
| U-EXT-04 | All | Missing Fields | Negative | Missing internal dictionary schema items drops the batch and raises a ValueError. | Given an active extraction file ingestion transaction data block When required internal field parameters are missing Then the active batch must be dropped and throw a ValueError. |
| U-EXT-05 | All | Empty Payload | Negative | Completely empty data arrays or tables containing 0 record objects inside responses raises a ValueError. | Given a completed network API call response buffer payload When the data container contains zero records or empty tables Then the processing client must trap the state and raise a ValueError. |
| U-EXT-06 | All | Schema Type | Negative | Mismatched uncastable data types (e.g. lists inside primary keys) throws an explicit TypeError. | Given an extraction payload containing type-corrupted attributes When fields cannot be safely mapped or cast to the schema Then the transformation job must break and throw a TypeError. |
| U-EXT-07 | Premier Injuries | Grid Ingestion | Positive | Parses cell-decomposed structures, drops mobile responsive text fragments, and maps rows cleanly into structured player injury records. | Given raw positional mobile-responsive HTML tables When processed using the cell-decomposed BeautifulSoup parser module Then it must parse rows cleanly into structured player injury dictionaries. |
| U-EXT-08 | Premier Injuries | Table Layout Drift | Negative | Table class or grid node modifications catch empty selector lists and raise a SelectorNotFoundError. | Given a changed Premier Injuries web page structure template When CSS table classes or grid layout parameters alter natively on the web Then the DOM locator must catch empty lists and throw a SelectorNotFoundError. |

### Scraper - Utilities (UTL)

| ID | Focus Area | Type | Expected Behaviour / Key Assertion | Gherkin |
|---|---|---|---|---|
| U-UTL-01 | Full Duplicates | Positive | Identical overlapping dictionary rows are cleanly deduplicated without alerts. | Given a data lake ingestion batch containing overlapping rows When records are evaluated by the identity deduplication utility Then exact duplicate rows are combined cleanly with no warnings emitted. |
| U-UTL-02 | PK Duplicates | Negative | Primary key collisions with differing attributes raises an IntraBatchDuplicateError. | Given an incoming batch dataset with matching primary keys When columns map to different values for the same key entity Then the utility must halt compilation and raise an IntraBatchDuplicateError. |
| U-UTL-03 | String Sanitisation | Positive | Central sanitiser cleanly trims trailing \n line breaks, tabs, and converts HTML non-breaking spaces (\u00a0). | Given raw source data strings containing white spaces and breaks When formatted by the global string cleaning sanitisation normaliser Then trailing breaks, tabs, and \u00a0 characters are stripped cleanly. |

------------------------------
## ⚙️ 2. Data Transformations Layer
### Data Transformation - Entity Resolution (ENT)

| ID | Name | Area | Type | Expected Behaviour / Key Assertion | Gherkin |
|---|---|---|---|---|---|
| U-ENT-01 | Team ID Success | Entity Resolution | Positive | Maps raw team text names to structured IDs via equality joins over Spark Connect. | Given a raw team input DataFrame and a lookup DataFrame fixture When an equality join is performed via Spark Connect Then the resulting DataFrame should contain the mapped team_id. |
| U-ENT-02 | Team ID Hard-Fail | Entity Resolution | Negative | Missing team governance mapping identities triggers an immediate UnresolvedEntityError. | Given an input DataFrame with a team name missing from the lookup DataFrame When the transformation logic detects a null join result Then it must raise an UnresolvedEntityError immediately. |
| U-ENT-03 | Player ID Success | Entity Resolution | Positive | Maps raw player text names to structured Golden IDs via equality joins over Spark Connect. | Given a raw player input DataFrame and a lookup DataFrame fixture When an equality join is performed via Spark Connect Then the resulting DataFrame should contain the mapped player_id. |
| U-ENT-04 | Player ID Hard-Fail | Entity Resolution | Negative | Missing player governance mapping identities triggers an immediate UnresolvedEntityError. | Given an input DataFrame with a player name missing from the lookup DataFrame When the transformation logic detects a null join result Then it must raise an UnresolvedEntityError immediately. |

### Data Transformation - Slowly Changing Dimensions (SCD)

| ID | Name | Area | Type | Expected Behaviour / Key Assertion | Gherkin |
|---|---|---|---|---|---|
| U-SCD-01 | SCD2 New Player Initialization | SCD2 | Positive | Initializes a brand new injury timeline interval with open-ended 9999-12-31 dates. | Given an injury update for a player that does not exist in the dimension When processed through the silver_modelling engine Then it creates a new record with valid_from = logical_date and open valid_to. |
| U-SCD-02 | SCD2 Injury Status Unchanged | SCD2 | Positive | Incoming records matching active records exactly result in a safe execution no-op. | Given a batch that matches the current active state in the timeline When processed via Spark Connect Then no rows are changed or created (No-op). |
| U-SCD-03 | SCD2 Injury Status Transition | SCD2 | Positive | Closes old active records and opens new ones with zero temporal day gaps. | Given a player status change (e.g. "Fit" to "Injured") When processed through the silver_modelling tracking engine Then it must close the active record and open a new one with no temporal gaps. |
| U-SCD-04 | SCD2 Historic "Stitch" | SCD2 | Positive | Pre-dated backfill updates split old intervals without creating data path gaps. | Given an incoming record with a date that falls before the current active record When processed through Spark Connect Then it should update the historical valid_to and re-stitch the timeline. |
| U-SCD-05 | SCD2 Injury Timeline Discrepancy | SCD2 | Negative | Overlapping interval date records inside an incoming batch raise an SCD2TimelineException. | Given an incoming record creating a logical conflict with an existing timeline When compiled through Spark Connect Then it must raise an SCD2TimelineException to prevent history corruption. |
| U-SCD-06 | SCD2 Injury Future-Date Gate | SCD2 | Negative | Ingesting records dated past the active logical context runtime raises a FutureDateError. | Given a record with an effective_date that is in the future relative to the system clock When processed through the pipeline Then it should raise a FutureDateError and halt the Spark job. |

### Data Transformation - Metrics (MTR)

| ID | Target Table | Focus Area | Type | Expected Behaviour / Key Assertion | Gherkin |
|---|---|---|---|---|---|
| U-MTR-01 | player_injury_summary | Spell Duration Calculation | Positive | Computes exact single-spell length via DATEDIFF, clamping active records safely to the processing context run-date. | Given an open-ended active injury history snapshot record When evaluated by the silver_analytics precomputation engine Then it clamps the record to the run-date context via an exact DATEDIFF. |
| U-MTR-02 | player_injury_summary | Cumulative Career Window | Positive | Computes a running sum (ROWS UNBOUNDED PRECEDING) of career days sidelined up to each unique record date context. | Given historical player summary dimensions inside the database sandbox When compiled using an unbounded partition window running sum loop Then it outputs the complete career days lost parameters up to each record. |
| U-MTR-03 | player_injury_summary | Availability Index | Positive | Derives an availability_coefficient factor (0.00 to 1.00) based on career days lost vs total calendar season days. | Given a calculated total count of career days lost for an entity When computed against a baseline total calendar season constraint Then it derives a normalized player availability coefficient factor between 0 and 1. |
| U-MTR-04 | match_squad_health | Sidelined Headcount | Positive | Evaluates absolute headcount counts of unavailable players for a specific fixture date context by joining injury timelines with rosters. | Given an active match fixture date context variable When joining active injury records with match availability rosters Then it outputs the absolute headcount of unavailable squad elements. |
| U-MTR-05 | match_squad_health | Player Baseline Window | Positive | Computes an individual's prior performance baseline using a bounded 5-match lookback window (ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING). | Given historical performance logs for individual player nodes When evaluated via a bounded 5-match lookback window partition array Then it outputs an accurate baseline performance metric. |
| U-MTR-06 | match_squad_health | Total Squad Attacking Drag | Positive | Aggregates individual historical baselines across all injured players to calculate the team's total attacking metric deficit (total_squad_xg_drag). | Given an active collection of injured players for a club match When individual historical baselines are summed by the metrics pipeline Then it precomputes the total attacking performance drag score (total_squad_xg_drag). |
| U-MTR-07 | match_squad_health | Defensive Penalty Score | Positive | Aggregates missing performance values across defensive nodes to calculate the total squad stability penalty score metric (total_squad_xga_drag). | Given an active collection of injured defensive nodes for a club fixture When historical variance metrics are aggregated by the metrics engine Then it outputs the overall squad stability penalty score (total_squad_xga_drag). |
| U-MTR-08 | All Metrics Tables | Temporal Anchor Guard | Negative | Passing processing context run-dates that are younger than the data's timelines halts execution, raising a FutureDateError. | Given metrics precomputation job processing arguments When an entry possesses a timeline timestamp younger than the execution clock Then the system must flag an anomaly and throw a FutureDateError. |

### Data Transformation - Serving Views (GLD)

| ID | Target View | Focus Area | Type | Expected Behaviour / Key Assertion | Gherkin |
|---|---|---|---|---|---|
| U-GLD-01 | gold_player_resilience_index | View Output Validation | Positive | Verifies that a regular expression anchor parser extracts and runs the true view file on disk to cleanly present player availability coefficients. | Given an unseeded, in-memory DuckDB database metadata sandbox canvas When the extracted gold_player_resilience_index.sql file query is executed Then the result row tuple must map accurately to player resilience variables. |
| U-GLD-02 | gold_squad_health_drag | View Output Validation | Positive | Verifies that the view file correctly joins the match facts table to isolate the precalculated squad attacking goals metrics drag. | Given an unseeded, in-memory DuckDB database metadata sandbox canvas When the extracted gold_squad_health_drag.sql file query is executed Then the result row tuple must cleanly output match IDs and squad health drag scores. |
| U-GLD-03 | gold_efficiency_anomalies | View Output Validation | Positive | Verifies that the view file accurately calculates pts - xpts mismatch differentials side-by-side with squad injury scores. | Given an unseeded, in-memory DuckDB database metadata sandbox canvas When the extracted gold_efficiency_anomalies.sql file query is executed Then the result row tuple must calculate points mismatch differentials accurately. |

### Data Transformation - Idempotency (IDM)

| ID | Name | Area | Type | Expected Behaviour / Key Assertion | Gherkin |
|---|---|---|---|---|---|
| U-IDM-01 | Global Idempotency Contract | Idempotency | Positive | Running identical data transformations sequentially results in identical output states. | Given an active data transformation module function When executed twice against the exact same data variables via Spark Connect Then the second run output state must be completely identical to the first. |

------------------------------
## 温 3. Orchestration & Serving Layers
### Airflow - Directed Acyclic Graphs (DAG)

| ID | Focus Area | Type | Expected Behaviour / Key Assertion | Gherkin |
|---|---|---|---|---|
| U-DAG-01 | Cycle Detection | Negative | Traps circular dependency deadlocks completely in-memory using an optimized DFS structural graph topological search. | Given a parsed Airflow DAG structural graph mapping When scanned via an in-memory topological DFS dependency traversal node search Then the system must compile with zero circular deadlocks or return an error. |
| U-DAG-02 | Compilation Integrity | Negative | Asserts that DagBag loads with zero active import errors, dynamic macro issues, or broken syntax files. | Given a local repository workspace containing workflow configuration files When processed through an isolated in-memory DagBag directory parser instance Then the environment must report exactly zero compilation import errors. |
| U-DAG-03 | Task ID Uniqueness | Negative | Scans task arrays within the workflow tree to assert that every single operator node possesses a completely unique task_id. | Given a compiled daily lakehouse processing graph pipeline context When tracking individual operator nodes across task collections strings Then every single instantiated task operator must possess a unique task_id. |
| U-DAG-04 | Defensive Production Gates | Negative | Uses a dynamic _resolve_task_param hierarchy scraper loop to ensure all ingestion and silver_modelling / silver_analytics tasks enforce a strict 1-minute timeout boundary and exponential backoff retries. | Given a compiled pipeline, a fallback dictionary, and an environment profile When auditing task parameters across the Airflow lookup hierarchy tree Then all ingestion and silver operators must define a 1-minute timeout and backoff. |
