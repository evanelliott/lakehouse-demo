Here is your updated, production-grade Architectural Specification for the Tier 1 Unit Test Gate.
This document aligns with your 16GB host RAM constraints, strict 100-character line lengths, and rigid air-gapped container boundaries [docs/test_approach_3. specification, pyproject.toml]. It has been updated to reflect the 3-character functional layer consolidation (UTL, NET, EXT), universal parameterised matrix gates, and the positional bs4 extraction engine refactors.
------------------------------
## Local Unit Testing Strategy & Architectural Specification
Data Platform Engine: Premier League E2E Lakehouse (Tier 1 Gate)
## 1. Executive Summary
This specification defines the stateless, air-gapped unit testing framework for the data platform. Operating under strict local isolation constraints, it guarantees that 100% of scraper validation logic, core utilities, data-lake extraction firewalls, and temporal business rules run completely offline inside the sandboxed DevContainer process namespace in under 3 seconds [docs/test_approach_3. specification, docs/test_approach_unit_tests.md].

                  ┌────────────────────────────────────────────────────────┐
                  │                 VS Code DevContainer                   │
                  │                                                        │
                  │   ┌────────────────────────┐    gRPC    ┌──────────┐   │
                  │   │      PyTest Exec       │ ─────────► │  Spark   │   │
                  │   │   -m "utl or net or ext"│   :15002  │ Connect  │   │
                  │   └───────────┬────────────┘            └──────────┘   │
                  └───────────────┼────────────────────────────────────────┘
                                  ├───────────────────────┐
                                  ▼ (Offline Playback)    ▼ (Stateless Fixtures)
                       ┌──────────────────────┐  ┌──────────────────────┐
                       │  cassettes/**/*.yaml │  │  fixtures/**/*.{json,│
                       │   (HTTP Responses)   │  │   html} (Mocks)      │
                       └──────────────────────┘  └──────────────────────┘

------------------------------
## 2. Directory Structure Mappings

tests/unit/
├── conftest.py                       # Session-scoped loopback Spark Connect factory
│
├── core_utils/                       # Shared Core Utilities Portfolio
│   └── test_deduplication.py         # Handles U-UTL-01 to U-UTL-03
│
└── scraper/                          # Data Ingestion & Extraction Core
    ├── fixtures/                     # Permanent, immutable data contract mocks
    │   ├── raw_league_response.json  # Nested dictionary list sample
    │   ├── raw_match_response.json   # Deep Home/Away nested split array sample
    │   └── injury_table.html         # Positional mobile responsive HTML table grid
    │
    ├── cassettes/                    # Frozen plain-text text HTTP records
    │   └── test_scraper_network_handshake/
    │       ├── test_understat_handshake_success[...].yaml
    │       └── test_premier_injuries_handshake_success.yaml
    │
    ├── test_scraper_utils.py             # Layer tracking for validation helpers (UTL)
    ├── test_scraper_network_handshake.py # Layer tracking for session sequences (NET)
    └── test_scraper_data_extraction.py   # Layer tracking for payload processing (EXT)

------------------------------
## 3. Core Functional Testing Layers## Layer A: Shared Core Utilities (U-UTL-xx)

* Scope: Validates global data-lake structural firewalls, deduplication engines, and string preprocessing format normalisers.
* Storage Requirement: 100% in-memory python variable arrays.
* Design Rule: Tested once globally; imported and shared across all active ingestion pipelines to enforce DRY development standards.

## Layer B: Network Handshake Sequence (U-NET-xx)

* Scope: Evaluates multi-step cookie lifecycles, browser User-Agent spoofing, custom header enforcements, and anti-bot WAF status interceptions.
* Storage Requirement: Relies entirely on optimized, text-based YAML playback cascades (pytest-recording) checked into Git version control.
* Design Rule: Dynamic mock parameters intercept 403, 429, and 5xx status codes to verify clean domain error translation rules before touching remote interfaces.

## Layer C: Data Ingestion & Extraction (U-EXT-xx)

* Scope: Enforces payload envelope validation, dictionary key mapping safety, dictionary vs flat array flattening, empty dataset barriers, and data type casting validation.
* Compute Layer: Invokes the local loopback Spark Connect gRPC background server daemon (sc://localhost:15002) to guarantee data-lake schema DataFrame compatibility without JVM startup latency [docs/test_approach_3. specification, tests/unit/conftest.py].
* Design Rule: Leverages PyTest Parameterisation matrices to map universal rules across all five Understat JSON datasets and Premier Injuries HTML tables in compressed, low-overhead files.

------------------------------
## 4. Finalised Unit Test Registry Mappings## 🛠️ Shared Core Utilities (U-UTL-xx)

| ID | Focus Area | Type | Expected Behaviour / Key Assertion |
|---|---|---|---|
| U-UTL-01 | Full Duplicates | Positive | Exact identical overlapping record dictionaries are filtered and skipped seamlessly. |
| U-UTL-02 | PK Duplicates | Negative | Primary key collisions possessing mismatched attribute values triggers an IntraBatchDuplicateError. |
| U-UTL-03 | String Sanitisation | Positive | Central regex normaliser strips trailing line breaks (\n), tabs, and converts HTML non-breaking spaces (\u00a0). |

## 📡 Network Handshake Layer (U-NET-xx)

| ID | Data Source | Focus Area | Type | Expected Behaviour / Key Assertion |
|---|---|---|---|---|
| U-NET-01 | Understat | Handshake Success | Positive | Completes 2-step page hit, handles cookie jar tokens, and extracts 200 OK. Accepts application/json or text/javascript. |
| U-NET-02 | Understat | Session Rejected | Negative | Intercepts expired or missing PHPSESSID arrays and raises a SecurityContractException. |
| U-NET-03 | Understat | Header Drop | Negative | Omitting X-Requested-With triggers an immediate local intercept block, raising a ScraperBlockedException. |
| U-NET-04 | Understat | Rate Limited | Negative | Detecting a 429 Too Many Requests response rejects empty data blocks and raises a ScraperBlockedException. |
| U-NET-05 | Understat | Server Outage | Negative | Catching connection timeouts or 503 gateway drops halts execution and raises an InfrastructureTimeoutError. |
| U-NET-06 | Premier Injuries | Handshake Success | Positive | Passes browser User-Agent strings to verify a standard 200 OK response returning a text/html buffer. |
| U-NET-07 | Premier Injuries | Header Drop | Negative | Detecting naked automated requests blocks the client locally, raising a ScraperBlockedException. |
| U-NET-08 | Premier Injuries | Rate Limited | Negative | Intercepts a 429 rate limit block, drops execution parameters, and raises a ScraperBlockedException. |
| U-NET-09 | Premier Injuries | Server Outage | Negative | Catching 502 Bad Gateway responses or thread connection timeouts raises an InfrastructureTimeoutError. |

## ⚽ Data Ingestion & Extraction Layer (U-EXT-xx)

| ID | Data Source | Focus / Dataset | Type | Expected Behaviour / Key Assertion |
|---|---|---|---|---|
| U-EXT-01 | Understat | Ingestion: League | Positive | Maps raw JSON layers to uniform schemas for Teams, Players, and Dates over Spark Connect. |
| U-EXT-02 | Understat | Ingestion: Match | Positive | Flattens deep Home/Away branches to map uniform schemas for Rosters and Shots over Spark Connect. |
| U-EXT-03 | All | Missing ID Key | Negative | Missing core envelope root keys or structural table layout markers raises a SourceDataMissingError. |
| U-EXT-04 | All | Missing Fields | Negative | Missing mandatory row fields (e.g. id, player, datetime) drops the active payload batch, raising a ValueError. |
| U-EXT-05 | All | Empty Payload | Negative | Blank payload arrays or tables containing 0 record objects are trapped instantly, raising a ValueError. |
| U-EXT-06 | All | Schema Type | Negative | Mismatched uncastable data types (e.g. lists inside primary keys) drop the data batch and raise a TypeError. |
| U-EXT-07 | Premier Injuries | Grid Ingestion | Positive | Parses cell-decomposed structures, drops mobile responsive text fragments, and maps rows to valid injury dictionaries. |
| U-EXT-08 | Premier Injuries | Table Layout Drift | Negative | Table class modifications or grid structural drift items trigger a layout validation check and raise a SelectorNotFoundError. |

------------------------------
## 5. Automation and Governance Operational Rules## Rule 1: Symmetrical PySpark Row Conversions
To clear strict type-checking parameterizations [pyproject.toml], all happy path extraction test assertions must maps dictionary results lists into native pyspark.sql.Row objects before running spark.createDataFrame(). This bypasses strict generic RowLike type-variable constraints and preserves compilation stability.
## Rule 2: Fail-Safe Network Hotswapping
To regenerate cassette logs without compromising air-gapped isolation networks, engineers use a automated hotswap wrapper script (scripts/generate_cassettes.sh) from the host OS terminal. This temporarily connects the default docker bridge, triggers pytest --record-mode=once, and utilizes an internal shell trap sequence to guarantee the bridge is instantly severed even if a test block crashes or terminates prematurely.
## Rule 3: Git Tracking Matrix Integrity
To satisfy the completely offline commit contract requirement, all files matching tests/unit/scraper/fixtures/* and tests/unit/scraper/cassettes/**/*.yaml are explicitly whitelisted using leading exclamation markers inside the project .gitignore. They are safely tracked and compressed by Git to preserve runtime history parity across isolated developer deployments.
------------------------------
