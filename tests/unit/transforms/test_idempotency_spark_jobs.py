from typing import Any, Iterable, cast

import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from src.jobs.silver import (
    load_silver_injuries,
    load_silver_matches,
    load_silver_players,
    load_silver_rosters,
    load_silver_shots,
    load_silver_teams,
)

# --- IDEMPOTENCY PATTERN: RUN TWICE, ASSERT ONCE ---


@pytest.mark.idempotency
def test_silver_matches_idempotency(spark: SparkSession) -> None:
    initial = spark.createDataFrame(
        [], schema="match_id INT, home_id INT, away_id INT, score STRING"
    )
    data = [
        {
            "match_id": 789,
            "home_team": "Arsenal",
            "away_team": "Man City",
            "score": "2-1",
        }
    ]
    # Cast to Iterable[Any] to bypass the RowLike type-var check
    batch = spark.createDataFrame(cast(Iterable[Any], data))

    pass_1 = load_silver_matches(initial, batch)
    pass_2 = load_silver_matches(pass_1, batch)
    assertDataFrameEqual(pass_1, pass_2)


@pytest.mark.idempotency
def test_silver_injuries_idempotency(spark: SparkSession) -> None:
    initial_data = [
        {
            "player_id": 1,
            "status": "Fit",
            "valid_from": "2024-01-01",
            "valid_to": "9999-12-31",
        }
    ]
    batch_data = [
        {
            "player_id": 1,
            "status": "Injured",
            "effective_date": "2024-01-05",
        }
    ]

    initial = spark.createDataFrame(cast(Iterable[Any], initial_data))
    batch = spark.createDataFrame(cast(Iterable[Any], batch_data))

    pass_1 = load_silver_injuries(initial, batch)
    pass_2 = load_silver_injuries(pass_1, batch)
    assertDataFrameEqual(pass_1, pass_2)


@pytest.mark.idempotency
def test_silver_teams_idempotency(spark: SparkSession) -> None:
    initial_data = [
        {
            "team_id": 1,
            "raw_name": "Arsenal",
        }
    ]
    batch_data = [
        {"raw_name": "Arsenal"},
        {"raw_name": "Man City"},
    ]

    initial = spark.createDataFrame(cast(Iterable[Any], initial_data))
    batch = spark.createDataFrame(cast(Iterable[Any], batch_data))

    pass_1 = load_silver_teams(initial, batch)
    pass_2 = load_silver_teams(pass_1, batch)
    assertDataFrameEqual(pass_1, pass_2)


@pytest.mark.idempotency
def test_silver_players_idempotency(spark: SparkSession) -> None:
    initial_data = [
        {
            "player_id": 7,
            "raw_name": "B. Saka",
        }
    ]
    batch_data = [
        {"raw_name": "B. Saka"},
        {"raw_name": "N. Nwaneri"},
    ]

    initial = spark.createDataFrame(cast(Iterable[Any], initial_data))
    batch = spark.createDataFrame(cast(Iterable[Any], batch_data))

    pass_1 = load_silver_players(initial, batch)
    pass_2 = load_silver_players(pass_1, batch)
    assertDataFrameEqual(pass_1, pass_2)


@pytest.mark.idempotency
def test_silver_shots_idempotency(spark: SparkSession) -> None:
    initial = spark.createDataFrame([], schema="shot_id INT, match_id INT, player_id INT, xG FLOAT")
    batch_data = [
        {
            "shot_id": 1001,
            "match_id": 789,
            "player_id": 7,
            "xG": 0.45,
        }
    ]
    batch = spark.createDataFrame(cast(Iterable[Any], batch_data))

    pass_1 = load_silver_shots(initial, batch)
    pass_2 = load_silver_shots(pass_1, batch)
    assertDataFrameEqual(pass_1, pass_2)


@pytest.mark.idempotency
def test_silver_rosters_idempotency(spark: SparkSession) -> None:
    initial = spark.createDataFrame([], schema="team_id INT, player_id INT, season STRING")
    batch_data = [
        {
            "team_id": 1,
            "player_id": 7,
            "season": "2024/25",
        }
    ]
    batch = spark.createDataFrame(cast(Iterable[Any], batch_data))

    pass_1 = load_silver_rosters(initial, batch)
    pass_2 = load_silver_rosters(pass_1, batch)
    assertDataFrameEqual(pass_1, pass_2)
