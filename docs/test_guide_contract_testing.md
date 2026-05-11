# Technical Specification: Contract Testing
- This specification defines the Data Contract Gate, which sits alongside your unit tests to verify the integrity of your schema definitions. It ensures your "laws" (schemas) are enforceable before your "police" (code) attempt to apply them to live data.
------------------------------
## 1. Architectural Strategy: "Engine-Native Contracts"
Rather than forcing a single format across the lakehouse, we use the format most compatible with each engine's "consumer" logic. This eliminates complex translation code and ensures 1:1 parity with production behavior.

| Layer | Engine | Format | Validation Tool |
|---|---|---|---|
| Bronze | Scraper (Python) | JSON Schema | jsonschema library |
| Silver | Spark (Iceberg) | StructType JSON | pyspark.sql.types |
| Gold | DuckDB (SQL) | SQL DDL | duckdb SQL Engine |

------------------------------
## 2. Directory Structure
Schemas are stored in a central schemas/ directory, while the contract tests live in a top-level tests/contracts/ folder to separate "Logic" from "Compliance."
```sh
/
├── schemas/
│   ├── bronze/understat.json      # Standard JSON Schema
│   ├── silver/dim_injury.json     # Spark-native Schema
│   └── gold/team_form.sql         # CREATE TABLE/VIEW DDL
└── tests/
    └── contracts/
        ├── test_bronze.py         # Validates Scraper output shape
        ├── test_silver.py         # Validates Iceberg table constraints
        └── test_gold.py           # Validates Analytical view structures
```
------------------------------
## 3. Implementation Patterns## A. Bronze: Structural Integrity
These tests verify that the JSON Schema correctly flags structural issues in the raw dictionaries extracted by the scrapers.

* Test Focus: Nullability, Type-safety, and UTF-8 Regex (e.g., "Ødegaard").
* Pattern: Pass "Good" and "Bad" dictionaries directly to the schema validator.

## B. Silver: Iceberg Metadata Contract
These tests verify the Spark StructType that dictates how data is written to Iceberg.

* Test Focus: Non-nullable Primary Keys, Precision of Decimal types (xG), and Column naming.
* Pattern: Load the JSON via StructType.fromJson() and inspect the field attributes.

## C. Gold: Analytical View Contract
These tests verify the SQL DDL that defines your DuckDB views/tables.

* Test Focus: Presence of calculated metrics, Window function compatibility, and Column Aliasing.
* Pattern: Execute the .sql file in an in-memory DuckDB instance and run PRAGMA table_info.

------------------------------
## 4. Contract Test Case Registry

| ID | Name | Area | Type | Key Assertion |
|---|---|---|---|---|
| C-BRZ-01 | Bronze Regex Gate | Bronze | Positive | Schema allows UTF-8 diacritics in player/team names. |
| C-BRZ-02 | Bronze Type Check | Bronze | Negative | Schema rejects string values for numeric fields (e.g. xG). |
| C-SIL-01 | Silver Null Guard | Silver | Negative | Schema enforces nullable=False for IDs and valid_from. |
| C-SIL-02 | Silver SCD2 Keys | Silver | Positive | Schema contains mandatory valid_from, valid_to, is_current. |
| C-GLD-01 | Gold DDL Integrity | Gold | Positive | SQL DDL executes without error and yields expected columns. |

------------------------------
## 5. Why this is "Elite" for your Repo

   1. Zero Translation Faff: Spark gets the JSON it expects; DuckDB gets the SQL it expects.
   2. Stateless Compliance: You can verify all 3 layers of your data contracts in milliseconds without starting MinIO or Airflow.
   3. Self-Healing Documentation: If a developer adds a column to a Gold view but forgets to update the DDL, the contract test fails, acting as a mandatory documentation check.
