# src/queries/gold.py
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


def build_injury_summary(
    duck_con: "duckdb.DuckDBPyConnection", silver_injuries_rel: "duckdb.DuckDBPyRelation"
) -> "duckdb.DuckDBPyRelation":
    """
    U-IDM-07: Aggregates current injury statuses grouped by team.

    Queries directly from the passed in-memory relation to prevent session state
    pollution and guarantee pure function reproducibility.
    """
    return duck_con.sql(
        """
        SELECT 
            team_id,
            count(player_id) AS total_injured_players
        FROM silver_injuries_rel
        WHERE status = 'Injured' AND is_current = true
        GROUP BY team_id
        """
    )


def build_team_form(
    duck_con: "duckdb.DuckDBPyConnection", silver_matches_rel: "duckdb.DuckDBPyRelation"
) -> "duckdb.DuckDBPyRelation":
    """
    U-IDM-08: Computes rolling xG windowed metrics grouped by team.
    """
    return duck_con.sql(
        """
        SELECT 
            team_id,
            match_id,
            xg,
            date,
            avg(xg) OVER (
                PARTITION BY team_id 
                ORDER BY date 
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS rolling_avg_xg
        FROM silver_matches_rel
        """
    )


def build_player_stats(
    duck_con: "duckdb.DuckDBPyConnection", silver_shots_rel: "duckdb.DuckDBPyRelation"
) -> "duckdb.DuckDBPyRelation":
    """
    U-IDM-09: Generates analytical player-level aggregate performance stats.
    """
    return duck_con.sql(
        """
        SELECT 
            player_id,
            count(shot_id) AS total_shots,
            sum(xg) AS total_expected_goals,
            avg(xg) AS avg_shot_quality
        FROM silver_shots_rel
        GROUP BY player_id
        """
    )
