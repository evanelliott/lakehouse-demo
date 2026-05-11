from typing import Any, Iterable, cast

import pytest
from pyspark.sql import SparkSession
from src.transforms.injuries import update_injury_history

from src.exceptions import FutureDateError, SCD2TimelineException


@pytest.mark.scd2
def test_scd2_new_player_initialization(spark: SparkSession) -> None:
    """
    U-SCD-01: Positive test for first-time entity creation.
    """
    initial_state = spark.createDataFrame(
        [], schema="player_id INT, status STRING, valid_from STRING, valid_to STRING"
    )
    data = [{"player_id": 1, "status": "Fit", "effective_date": "2024-01-01"}]
    new_batch = spark.createDataFrame(cast(Iterable[Any], data))

    result = update_injury_history(initial_state, new_batch)
    actual = [row.asDict() for row in result.collect()]

    expected = [
        {"player_id": 1, "status": "Fit", "valid_from": "2024-01-01", "valid_to": "9999-12-31"}
    ]
    assert actual == expected


@pytest.mark.scd2
def test_scd2_injury_status_unchanged(spark: SparkSession) -> None:
    """
    U-SCD-02: Positive test for no-op idempotency.
    """
    initial_data = [
        {"player_id": 1, "status": "Fit", "valid_from": "2024-01-01", "valid_to": "9999-12-31"}
    ]
    batch_data = [{"player_id": 1, "status": "Fit", "effective_date": "2024-01-05"}]

    initial_state = spark.createDataFrame(cast(Iterable[Any], initial_data))
    new_batch = spark.createDataFrame(cast(Iterable[Any], batch_data))

    result = update_injury_history(initial_state, new_batch)
    assert result.count() == 1
    assert result.collect()[0]["valid_from"] == "2024-01-01"  # Should not shift


@pytest.mark.scd2
def test_scd2_injury_status_transition(spark: SparkSession) -> None:
    """
    U-SCD-03: Positive test for status change (Fit -> Injured).
    """
    initial_data = [
        {"player_id": 1, "status": "Fit", "valid_from": "2024-01-01", "valid_to": "9999-12-31"}
    ]
    batch_data = [{"player_id": 1, "status": "Injured", "effective_date": "2024-01-05"}]

    initial_state = spark.createDataFrame(cast(Iterable[Any], initial_data))
    new_batch = spark.createDataFrame(cast(Iterable[Any], batch_data))

    result = update_injury_history(initial_state, new_batch).orderBy("valid_from")
    actual = [row.asDict() for row in result.collect()]

    expected = [
        {"player_id": 1, "status": "Fit", "valid_from": "2024-01-01", "valid_to": "2024-01-05"},
        {"player_id": 1, "status": "Injured", "valid_from": "2024-01-05", "valid_to": "9999-12-31"},
    ]
    assert actual == expected


@pytest.mark.scd2
def test_scd2_historic_stitch(spark: SparkSession) -> None:
    """
    U-SCD-04: Positive test for Option B backfilling.
    """
    initial_data = [
        {"player_id": 1, "status": "Fit", "valid_from": "2024-01-01", "valid_to": "2024-01-10"},
        {
            "player_id": 1,
            "status": "Injured",
            "valid_from": "2024-01-10",
            "valid_to": "9999-12-31",
        },
    ]
    batch_data = [{"player_id": 1, "status": "Injured", "effective_date": "2024-01-05"}]

    initial_state = spark.createDataFrame(cast(Iterable[Any], initial_data))
    new_batch = spark.createDataFrame(cast(Iterable[Any], batch_data))

    result = update_injury_history(initial_state, new_batch).orderBy("valid_from")
    actual = [row.asDict() for row in result.collect()]

    # Jan 1st record should be shortened to Jan 5th
    assert actual[0]["valid_to"] == "2024-01-05"
    # New 'Injured' slice from Jan 5th to Jan 10th
    assert actual[1] == {
        "player_id": 1,
        "status": "Injured",
        "valid_from": "2024-01-05",
        "valid_to": "2024-01-10",
    }


@pytest.mark.scd2
def test_scd2_injury_timeline_discrepancy_fails(spark: SparkSession) -> None:
    """
    U-SCD-05: Negative test for logical overlap conflicts.
    """
    initial_data = [
        {"player_id": 1, "status": "Fit", "valid_from": "2024-01-01", "valid_to": "2024-01-10"}
    ]
    batch_data = [{"player_id": 1, "status": "Unknown", "effective_date": "2024-01-05"}]

    initial_state = spark.createDataFrame(cast(Iterable[Any], initial_data))
    new_batch = spark.createDataFrame(cast(Iterable[Any], batch_data))

    with pytest.raises(SCD2TimelineException):
        update_injury_history(initial_state, new_batch)


@pytest.mark.scd2
def test_scd2_injury_future_date_gate_fails(spark: SparkSession) -> None:
    """
    U-SCD-06: Negative test for future-dated records.
    """
    initial_state = spark.createDataFrame(
        [], schema="player_id INT, status STRING, valid_from STRING, valid_to STRING"
    )

    data = [{"player_id": 1, "status": "Injured", "effective_date": "3000-01-01"}]
    new_batch = spark.createDataFrame(cast(Iterable[Any], data))

    with pytest.raises(FutureDateError):
        update_injury_history(initial_state, new_batch)
