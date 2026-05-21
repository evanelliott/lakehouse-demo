# tests/unit/transforms/conftest.py
import os
import re
from typing import TYPE_CHECKING, List

import pytest

if TYPE_CHECKING:
    import duckdb

SCHEMAS_DIR = "src/schemas"


@pytest.fixture(scope="function", autouse=True)
def base_warehouse_tables(duck_con: "duckdb.DuckDBPyConnection") -> None:
    """
    Function-scoped autouse fixture ensuring an absolute, complete blank slate.

    Automatically runs at the start of EVERY individual test function block to
    instantiate the physical table structures from disk with zero cross-test leakage.
    """
    if not os.path.exists(SCHEMAS_DIR):
        raise FileNotFoundError(f"Schema registry path missing: {SCHEMAS_DIR}")

    all_schema_files: List[str] = os.listdir(SCHEMAS_DIR)

    # Programmatically drop every defined table and view to reset the canvas completely
    for file_name in all_schema_files:
        if file_name.endswith(".sql"):
            entity_name = file_name.replace(".sql", "")
            if entity_name.startswith("gold_"):
                duck_con.execute(f"DROP VIEW IF EXISTS {entity_name} CASCADE;")
            else:
                duck_con.execute(f"DROP TABLE IF EXISTS {entity_name} CASCADE;")

    # Partition schema definitions to ensure correct relational compilation order
    table_files = sorted(
        [f for f in all_schema_files if f.startswith("silver_") and f.endswith(".sql")]
    )
    view_files = sorted(
        [f for f in all_schema_files if f.startswith("gold_") and f.endswith(".sql")]
    )

    # Provision ALL tables and views as an unseeded, empty metadata graph
    for file_name in table_files + view_files:
        file_path = os.path.join(SCHEMAS_DIR, file_name)
        with open(file_path, "r", encoding="utf-8") as f:
            ddl_content = f.read().strip()

        if ddl_content:
            cleaned_ddl = ddl_content

            # FIX: If it has Iceberg keywords, slice up to the main block's closing bracket
            if "USING iceberg" in cleaned_ddl or "using iceberg" in cleaned_ddl.lower():
                # Split at 'USING' to isolate the clean CREATE TABLE (...) body prefix
                parts = re.split(r"\bUSING\b", cleaned_ddl, flags=re.IGNORECASE)
                if len(parts) >= 1:
                    # Keep everything before 'USING', ensure it finishes with a proper closing brace
                    cleaned_ddl = parts[0].strip()

            # Ensure syntax formatting ends cleanly with a standard single tracking semicolon
            cleaned_ddl = cleaned_ddl.strip().rstrip(";") + ";"

            try:
                duck_con.execute(cleaned_ddl)
            except Exception as e:
                raise RuntimeError(
                    f"Schema compilation error in sandbox file '{file_name}': {str(e)}"
                )


@pytest.fixture(scope="function")
def seeded_metrics_data(duck_con: "duckdb.DuckDBPyConnection") -> None:
    """
    Function-scoped explicit data seeder fixture.

    Does NOT run automatically. Must be requested explicitly by Gold query tests
    to safely populate metrics summaries on top of the blank tables.
    """
    # Populate Core Dimension Identity Profiles
    duck_con.execute("INSERT INTO silver_dim_player VALUES (1164, 'Adama Traore Diarra');")
    duck_con.execute("INSERT INTO silver_dim_team VALUES (23, 'West Ham United');")

    # Seed Job 2 Precomputed Metrics Records
    duck_con.execute(
        """
        INSERT INTO silver_analytics_player_injury_summary VALUES 
        (1164, '2026-05-10', '9999-12-31', 5, 10, 0.966, true);
        """
    )
    duck_con.execute("INSERT INTO silver_fact_match VALUES (29088, 23, 89, '2025/2026', 1, 1.65);")
    duck_con.execute(
        "INSERT INTO silver_analytics_match_squad_health_deficit VALUES (29088, 23, 1, 0.35, 0.15);"
    )
