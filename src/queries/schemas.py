# src/queries/schemas.py
from typing import Dict

# Dictionary mapping target Gold tables to their strict DDL schema definitions
GOLD_SCHEMAS: Dict[str, str] = {
    "gold_injury_summary": """
        CREATE TABLE IF NOT EXISTS gold_injury_summary (
            team_id INTEGER PRIMARY KEY,
            total_injured_players BIGINT
        );
    """,
    "gold_team_form": """
        CREATE TABLE IF NOT EXISTS gold_team_form (
            team_id INTEGER,
            match_id INTEGER,
            xg DOUBLE,
            date VARCHAR,
            rolling_avg_xg DOUBLE,
            PRIMARY KEY (team_id, match_id)
        );
    """,
    "gold_player_stats": """
        CREATE TABLE IF NOT EXISTS gold_player_stats (
            player_id INTEGER PRIMARY KEY,
            total_shots BIGINT,
            total_expected_goals DOUBLE,
            avg_shot_quality DOUBLE
        );
    """,
}
