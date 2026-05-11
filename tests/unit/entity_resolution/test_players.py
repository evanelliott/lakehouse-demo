# tests/unit/entity_resolution/test_players.py
from typing import Any, Iterable, cast

import pytest
from pyspark.sql import SparkSession
from src.transforms.entity_resolution import resolve_player_entities

from src.exceptions import UnresolvedEntityError


@pytest.mark.entity_resolution
def test_player_join_success(spark: SparkSession) -> None:
    """
    U-ENT-03 (Extended): Positive test for strict player mapping.
    Ensures 'B. Saka' maps to PlayerID: 7 via an equality join.
    """
    # 1. Arrange: Mock Golden Reference for Players
    lookup_data = [
        {"raw_player_name": "B. Saka", "player_id": 7},
        {"raw_player_name": "M. Ødegaard", "player_id": 8},
    ]
    lookup_df = spark.createDataFrame(cast(Iterable[Any], lookup_data))

    # 2. Arrange: Raw Input Data (e.g., from Understat)
    input_data = [{"player_name": "B. Saka", "xg": 0.85}]
    input_df = spark.createDataFrame(cast(Iterable[Any], input_data))

    # 3. Act
    result = resolve_player_entities(input_df, lookup_df)
    actual = result.collect()[0].asDict()

    # 4. Assert
    assert actual["player_id"] == 7
    assert "player_name" not in actual  # Raw name resolved to ID


@pytest.mark.entity_resolution
def test_player_join_hard_fail(spark: SparkSession) -> None:
    """
    U-ENT-04 (Extended): Negative test for missing players.
    Ensures the job halts if a player is missing from the lookup table.
    """
    # 1. Arrange
    lookup_data = [{"raw_player_name": "B. Saka", "player_id": 7}]
    lookup_df = spark.createDataFrame(cast(Iterable[Any], lookup_data))

    # 2. Arrange: New player not yet in our reference data
    input_data = [{"player_name": "A. New Player", "xg": 0.05}]
    input_df = spark.createDataFrame(cast(Iterable[Any], input_data))

    # 3. Act & Assert
    with pytest.raises(UnresolvedEntityError) as exc:
        resolve_player_entities(input_df, lookup_df)

    assert "Unresolved entity detected: A. New Player" in str(exc.value)
