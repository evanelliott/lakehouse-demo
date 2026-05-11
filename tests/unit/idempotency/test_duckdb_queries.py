from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.queries.gold import build_injury_summary, build_player_stats, build_team_form

if TYPE_CHECKING:
    import duckdb

# --- IDEMPOTENCY CHECK UTILITY ---


def assert_duck_equal(rel_a: "duckdb.DuckDBPyRelation", rel_b: "duckdb.DuckDBPyRelation") -> None:
    """
    Standard SQL-based identity check using the Relation API.
    If the symmetric difference is empty, the relations are identical.
    """
    # .except_() finds rows in A not in B; .union() combines with rows in B not in A
    diff = rel_a.except_(rel_b).union(rel_b.except_(rel_a)).fetchone()
    assert diff is None, "DuckDB Gold query diverged on second run."


# --- TESTS ---


@pytest.mark.idempotency
def test_gold_injury_summary_idempotency(duck_con: "duckdb.DuckDBPyConnection") -> None:
    """
    U-IDM-07: Injury status summary by team.
    """
    # 1. ARRANGE: Hard-coded mock data converted to a DuckDB Relation
    mock_df = pd.DataFrame(
        [{"team_id": 1, "player_id": 7, "status": "Injured", "is_current": True}]
    )
    silver_injuries_rel = duck_con.from_df(mock_df)

    # 2. ACT: Run the query twice
    pass_1 = build_injury_summary(duck_con, silver_injuries_rel)
    pass_2 = build_injury_summary(duck_con, silver_injuries_rel)

    # 3. ASSERT: Contract Verification
    assert_duck_equal(pass_1, pass_2)


@pytest.mark.idempotency
def test_gold_team_form_idempotency(duck_con: "duckdb.DuckDBPyConnection") -> None:
    """
    U-IDM-08: Rolling xG and form metrics.
    """
    # 1. ARRANGE
    mock_df = pd.DataFrame([{"team_id": 1, "match_id": 101, "xg": 1.5, "date": "2024-01-01"}])
    silver_matches_rel = duck_con.from_df(mock_df)

    # 2. ACT
    pass_1 = build_team_form(duck_con, silver_matches_rel)
    pass_2 = build_team_form(duck_con, silver_matches_rel)

    # 3. ASSERT
    assert_duck_equal(pass_1, pass_2)


@pytest.mark.idempotency
def test_gold_player_stats_idempotency(duck_con: "duckdb.DuckDBPyConnection") -> None:
    """
    U-IDM-09: Player-level aggregate stats.
    """
    # 1. ARRANGE
    mock_df = pd.DataFrame([{"player_id": 7, "shot_id": 1001, "xg": 0.45}])
    silver_shots_rel = duck_con.from_df(mock_df)

    # 2. ACT
    pass_1 = build_player_stats(duck_con, silver_shots_rel)
    pass_2 = build_player_stats(duck_con, silver_shots_rel)

    # 3. ASSERT
    assert_duck_equal(pass_1, pass_2)
