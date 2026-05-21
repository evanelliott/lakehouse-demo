# tests/unit/idempotency/test_duckdb_queries.py
from typing import TYPE_CHECKING, Callable

import pandas as pd
import pytest

from src.queries.gold import build_injury_summary, build_player_stats, build_team_form
from src.queries.schemas import GOLD_SCHEMAS

if TYPE_CHECKING:
    import duckdb


def execute_idempotency_loop(
    duck_con: "duckdb.DuckDBPyConnection",
    table_name: str,
    silver_relation: "duckdb.DuckDBPyRelation",
    query_callable: Callable[
        ["duckdb.DuckDBPyConnection", "duckdb.DuckDBPyRelation"], "duckdb.DuckDBPyRelation"
    ],
) -> None:
    """
    Generic execution harness to validate true data idempotency.
    Creates a clean target table using external schema registries,
    runs double upsert writes, and verifies table state consistency.
    """
    # 1. Initialize the target table from our centralized schema registry
    duck_con.execute(f"DROP TABLE IF EXISTS {table_name};")
    duck_con.execute(GOLD_SCHEMAS[table_name])

    # 2. Run execution pass 1 (Establish baseline state)
    res_1 = query_callable(duck_con, silver_relation)
    # FIX: Register the relation as a temporary view.
    # This keeps Ruff happy because 'res_1' is explicitly used as an argument.
    duck_con.register("v_batch_data_1", res_1)
    duck_con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM v_batch_data_1")
    baseline_state = duck_con.table(table_name).fetchall()

    # 3. Run execution pass 2 (Test double-write idempotency handling)
    res_2 = query_callable(duck_con, silver_relation)
    duck_con.register("v_batch_data_2", res_2)
    duck_con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM v_batch_data_2")
    current_state = duck_con.table(table_name).fetchall()

    # 4. Enforce state isolation contract
    assert current_state == baseline_state, (
        f"CRITICAL STATE DEVIATION: Table '{table_name}' mutated or "
        "duplicated data records during duplicate pipeline executions."
    )


# --- REFACTORED IDEMPOTENCY TESTS ---


@pytest.mark.idempotency
def test_gold_injury_summary_true_idempotency(duck_con: "duckdb.DuckDBPyConnection") -> None:
    """U-IDM-07: Verifies team injury aggregation contains no side-effects."""
    # FIX E501: Multi-line formatting keeps this under the 100 character threshold
    mock_df = pd.DataFrame(
        [
            {
                "team_id": 1,
                "player_id": 7,
                "status": "Injured",
                "is_current": True,
            }
        ]
    )
    silver_relation = duck_con.from_df(mock_df)

    execute_idempotency_loop(duck_con, "gold_injury_summary", silver_relation, build_injury_summary)


@pytest.mark.idempotency
def test_gold_team_form_true_idempotency(duck_con: "duckdb.DuckDBPyConnection") -> None:
    """U-IDM-08: Verifies rolling xG and windowed form metrics remain fully consistent."""
    mock_df = pd.DataFrame(
        [
            {
                "team_id": 1,
                "match_id": 101,
                "xg": 1.5,
                "date": "2024-01-01",
            }
        ]
    )
    silver_relation = duck_con.from_df(mock_df)

    execute_idempotency_loop(duck_con, "gold_team_form", silver_relation, build_team_form)


@pytest.mark.idempotency
def test_gold_player_stats_true_idempotency(duck_con: "duckdb.DuckDBPyConnection") -> None:
    """U-IDM-09: Verifies running player statistics do not inflate on re-run."""
    mock_df = pd.DataFrame(
        [
            {
                "player_id": 7,
                "shot_id": 1001,
                "xg": 0.45,
            }
        ]
    )
    silver_relation = duck_con.from_df(mock_df)

    execute_idempotency_loop(duck_con, "gold_player_stats", silver_relation, build_player_stats)
