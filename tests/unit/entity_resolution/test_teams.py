# tests/unit/entity_resolution/test_teams.py
from typing import Any, Iterable, cast

import pytest
from pyspark.sql import SparkSession

from src.exceptions import UnresolvedEntityError
from src.transforms.entity_resolution import resolve_team_entities


@pytest.mark.entity_resolution
def test_team_join_success(spark: SparkSession) -> None:
    """
    U-ENT-01: Positive test for strict team mapping.
    Ensures 'Man City' maps to TeamID: 1 via an equality join.
    """
    # 1. Arrange: Mock Lookup Table (The "Golden" Reference)
    lookup_data = [
        {"raw_name": "Man City", "team_id": 1},
        {"raw_name": "Arsenal", "team_id": 2},
    ]
    lookup_df = spark.createDataFrame(cast(Iterable[Any], lookup_data))

    # 2. Arrange: Raw Input Data
    input_data = [{"team_name": "Man City", "goals": 3}]
    input_df = spark.createDataFrame(cast(Iterable[Any], input_data))

    # 3. Act
    result = resolve_team_entities(input_df, lookup_df)

    # 4. Assert
    actual = result.collect()[0].asDict()
    assert actual["team_id"] == 1
    assert "team_name" not in actual  # Asserting that standardisation cleaned the raw field


@pytest.mark.entity_resolution
def test_team_join_hard_fail(spark: SparkSession) -> None:
    """
    U-ENT-02: Negative test for missing teams.
    Ensures the job halts if a team is missing from the lookup table.
    """
    # 1. Arrange
    lookup_data = [{"raw_name": "Arsenal", "team_id": 2}]
    lookup_df = spark.createDataFrame(cast(Iterable[Any], lookup_data))

    # 2. Arrange: 'Unknown XI' is not in our reference set
    input_data = [{"team_name": "Unknown XI", "goals": 0}]
    input_df = spark.createDataFrame(cast(Iterable[Any], input_data))

    # 3. Act & Assert
    with pytest.raises(UnresolvedEntityError) as exc:
        resolve_team_entities(input_df, lookup_df)

    assert "Unresolved entity detected: Unknown XI" in str(exc.value)
