# 🏟️ Premier League Lakehouse: Injury & Performance Analytics

An end-to-end Data Engineering platform bridging disparate football data sources into a high-performance **Iceberg Lakehouse**. This project leverages **Entity Resolution** to correlate squad health (Premier Injuries) with performance metrics (Understat).

---

## 🏗️ Architecture & Strategy

*   **Ingestion:** "Hybrid Handshake" (Playwright-Stealth + Requests) to bypass anti-bot shields.
*   **Storage:** MinIO (S3-Compatible) with a **Medallion Architecture** (Bronze, Silver, Gold).
*   **Governance:** Human-in-the-loop **Entity Resolution** to link player identities.
*   **Shift-Left Quality:** Tiered testing gates (Pre-commit/Pre-push) using `prek`, `mypy`, and stateless Docker environments.
*   **Modeling:** Spark for **SCD Type 2** history and 3NF facts; DuckDB for Gold layer analytics.

---

## 🚀 Getting Started

### 1. Environment Setup
This project is designed for **VS Code DevContainers**. 
*   Open the project in VS Code.
*   Click **"Reopen in Container"** when prompted.
*   The `scripts/dev_setup.sh` will automatically sync dependencies via `uv` and install Git hooks.

### 2. Infrastructure Initialization
Run the automated setup to create buckets and apply **Least-Privilege** security policies:
```bash
./scripts/setup_infra.sh
```

### 3. The "Golden Mapping" (Governance)
Because Source A ("K. De Bruyne") and Source B ("Kevin De Bruyne") use different names, you must verify the mapping:
1.  Open `notebooks/player_resolution.ipynb`.
2.  Run cells to generate `player_mapping_candidates.csv`.
3.  Review and save as `src/governance/player_mapping.parquet`.

---

## 🧪 Quality Gates (Shift-Left)

We use a two-phase testing strategy to ensure the Lakehouse never breaks in production.

### Phase 1: The Inner Loop (Pre-commit)
**Goal:** Instant logic validation (< 5s).
*   **Ruff:** Linting & Formatting.
*   **Mypy:** Strict type-checking of scraper and Spark logic.
*   **Pytest (Unit):** Validates the hierarchical DOM parser and SCD2 logic via fixtures.

### Phase 2: The Infrastructure Gate (Pre-push)
**Goal:** Full-stack validation with zero "stale data" noise.
*   Triggered automatically on `git push`.
*   Spins up a **Stateless MinIO** (no volumes) to ensure a clean-room test.
*   Executes the full pipeline: **Scrape → Silver → DQ Gate → Gold**.
*   Verifies **S3 Security Policies** (Consumer isolation).

---

## 📊 Data Layers


| Layer | Path | Description |
| :--- | :--- | :--- |
| **Bronze** | `bronze/` | Raw Parquet files (Understat & Premier Injuries). |
| **Governance** | `governance/` | Verified `player_mapping.parquet` for entity resolution. |
| **Silver** | `silver/` | 3NF normalized tables with **SCD Type 2** injury history. |
| **Gold** | `gold/` | Analytical views (e.g., `gold_team_health_performance`). |

---

## 🛠️ Tech Stack

*   **Orchestration:** Airflow / `prek`.
*   **Compute:** Apache Spark 3.5, DuckDB.
*   **Storage:** MinIO, Apache Iceberg (REST Catalog).
*   **Quality:** Mypy, Ruff, Pytest, Boto3-Stubs.
*   **Scraping:** Playwright (Stealth), BeautifulSoup4.
