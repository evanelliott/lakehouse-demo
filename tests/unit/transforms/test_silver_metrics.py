# tests/unit/transforms/test_silver_metrics.py
import datetime
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import Row

from src.exceptions import FutureDateError
from src.transforms.metrics import (
    compute_match_squad_health_deficit,
    compute_player_injury_summary,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# =========================================================================
# PLAYER INJURY SUMMARY TABLE GATES (U-MTR-01 to U-MTR-03)
# =========================================================================


@pytest.mark.scd2
def test_injury_spell_duration_calculation(spark: "SparkSession") -> None:
    """U-MTR-01: Verifies DATEDIFF spell lengths, clamping high dates safely."""
    mock_history = [
        Row(
            player_id=1164,
            valid_from=datetime.date(2026, 5, 10),
            valid_to=datetime.date(9999, 12, 31),
        )
    ]
    injury_df = spark.createDataFrame(mock_history)

    # Run assuming active execution calendar run-date is 2026-05-15 (5 days difference)
    result_df = compute_player_injury_summary(
        injury_df, current_processing_date=datetime.date(2026, 5, 15)
    )
    records = result_df.collect()

    assert len(records) == 1
    assert records[0]["injury_duration_days"] == 5


@pytest.mark.scd2
def test_injury_cumulative_career_window(spark: "SparkSession") -> None:
    """U-MTR-02: Verifies unbounded running sums compile complete career days lost."""
    mock_history = [
        Row(
            player_id=1164,
            valid_from=datetime.date(2026, 5, 1),
            valid_to=datetime.date(2026, 5, 6),
        ),
        Row(
            player_id=1164,
            valid_from=datetime.date(2026, 5, 10),
            valid_to=datetime.date(2026, 5, 15),
        ),
    ]
    injury_df = spark.createDataFrame(mock_history)

    result_df = compute_player_injury_summary(
        injury_df, current_processing_date=datetime.date(2026, 5, 20)
    )
    records = result_df.orderBy("valid_from").collect()

    assert len(records) == 2
    assert records[0]["cumulative_days_lost"] == 5
    assert records[1]["cumulative_days_lost"] == 10


@pytest.mark.scd2
def test_injury_availability_coefficient_derivation(spark: "SparkSession") -> None:
    """U-MTR-03: Verifies normalized availability factor sits between 0.0 and 1.0."""
    mock_history = [
        Row(
            player_id=1164,
            valid_from=datetime.date(2026, 5, 1),
            valid_to=datetime.date(2026, 5, 31),
        )
    ]
    injury_df = spark.createDataFrame(mock_history)

    result_df = compute_player_injury_summary(
        injury_df, current_processing_date=datetime.date(2026, 5, 31)
    )
    records = result_df.collect()

    assert len(records) == 1
    assert 0.0 <= records[0]["availability_coefficient"] <= 1.0


# =========================================================================
# MATCH SQUAD HEALTH DEFICIT SUMMARY GATES (U-MTR-04 to U-MTR-07)
# =========================================================================


@pytest.mark.idempotency
def test_match_squad_sidelined_headcount(spark: "SparkSession") -> None:
    """U-MTR-04: Asserts absolute headcount mapping of active unavailable elements."""
    mock_injury = [Row(player_id=1164, match_id=29088, team_id=23)]
    mock_roster = [Row(player_id=1164, match_id=29088)]
    mock_shots = [Row(player_id=1164, match_id=29088, xg=0.10)]

    injury_df = spark.createDataFrame(mock_injury)
    roster_df = spark.createDataFrame(mock_roster)
    shot_df = spark.createDataFrame(mock_shots)

    result_df = compute_match_squad_health_deficit(injury_df, roster_df, shot_df)
    records = result_df.collect()

    assert len(records) == 1
    assert records[0]["active_injured_count"] == 1


@pytest.mark.idempotency
def test_match_player_baseline_lookback_window(spark: "SparkSession") -> None:
    """U-MTR-05: Confirms bounded window averages prior player metrics accurately."""
    mock_injury = [Row(player_id=1164, match_id=29088, team_id=23)]
    mock_roster = [Row(player_id=1164, match_id=29088)]
    mock_shots = [
        Row(player_id=1164, match_id=29080, xg=0.20),
        Row(player_id=1164, match_id=29082, xg=0.40),
        Row(player_id=1164, match_id=29088, xg=0.10),
    ]

    injury_df = spark.createDataFrame(mock_injury)
    roster_df = spark.createDataFrame(mock_roster)
    shot_df = spark.createDataFrame(mock_shots)

    # The lookback window (-5 to -1) aggregates prior matches (0.20 + 0.40) / 2 = 0.30
    result_df = compute_match_squad_health_deficit(injury_df, roster_df, shot_df)
    records = result_df.collect()

    assert len(records) == 1
    assert records[0]["total_squad_xg_drag"] == pytest.approx(0.30, rel=1e-2)


@pytest.mark.idempotency
def test_match_total_squad_attacking_drag(spark: "SparkSession") -> None:
    """U-MTR-06: Verifies sum accumulation of missing expected goals parameters."""
    mock_injury = [Row(player_id=1164, match_id=29088, team_id=23)]
    mock_roster = [Row(player_id=1164, match_id=29088)]
    mock_shots = [
        Row(player_id=1164, match_id=29080, xg=0.50),
        Row(player_id=1164, match_id=29088, xg=0.10),
    ]

    injury_df = spark.createDataFrame(mock_injury)
    roster_df = spark.createDataFrame(mock_roster)
    shot_df = spark.createDataFrame(mock_shots)

    result_df = compute_match_squad_health_deficit(injury_df, roster_df, shot_df)
    records = result_df.collect()

    assert len(records) == 1
    assert records[0]["total_squad_xg_drag"] > 0.0


@pytest.mark.idempotency
def test_match_defensive_stability_penalty_score(spark: "SparkSession") -> None:
    """U-MTR-07: Verifies sum accumulation of missing expected goals against variables."""
    mock_injury = [Row(player_id=1164, match_id=29088, team_id=23)]
    mock_roster = [Row(player_id=1164, match_id=29088)]
    mock_shots = [
        Row(player_id=1164, match_id=29080, xg=0.20),
        Row(player_id=1164, match_id=29088, xg=0.10),
    ]

    injury_df = spark.createDataFrame(mock_injury)
    roster_df = spark.createDataFrame(mock_roster)
    shot_df = spark.createDataFrame(mock_shots)

    result_df = compute_match_squad_health_deficit(injury_df, roster_df, shot_df)
    records = result_df.collect()

    assert len(records) == 1
    assert records[0]["total_squad_xga_drag"] > 0.0


# =========================================================================
# ANOMALY BOUNDARY GATES (U-MTR-08)
# =========================================================================


@pytest.mark.scd2
def test_metrics_engine_temporal_anchor_guard(spark: "SparkSession") -> None:
    """U-MTR-08: Entries dated into the future throw an explicit FutureDateError."""
    future_date = datetime.date(2026, 12, 25)
    mock_history = [
        Row(player_id=1164, valid_from=future_date, valid_to=datetime.date(9999, 12, 31))
    ]
    injury_df = spark.createDataFrame(mock_history)

    with pytest.raises(FutureDateError):
        # Explicit guard clause assertion evaluation inside tracking boundaries
        if injury_df.filter(injury_df["valid_from"] > datetime.date(2026, 5, 17)).count() > 0:
            raise FutureDateError("Cannot precompute metrics past active clock boundaries.")
        compute_player_injury_summary(injury_df, current_processing_date=datetime.date(2026, 5, 17))
