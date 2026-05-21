# tests/unit/transforms/test_silver_transformations.py
import datetime
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import Row

from src.exceptions import (
    FutureDateError,
    IntraBatchDuplicateError,
    SCD2TimelineException,
    UnresolvedEntityError,
)
from src.transforms.normalisation import (
    process_injury_scd2,
    resolve_player_identities,
    validate_fact_composite_keys,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# =========================================================================
# GOVERNANCE PLAYER IDENTITY RESOLUTION GATES (U-ENT-01 & U-ENT-02)
# =========================================================================


@pytest.mark.warehouse
def test_player_resolution_success(spark: "SparkSession") -> None:
    """U-ENT-01: Confirms source text names successfully match Golden Governance maps."""
    raw_data = [Row(raw_name="Adama Traore Diarra")]
    mapping_data = [Row(raw_name="Adama Traore Diarra", player_id=1164)]

    raw_df = spark.createDataFrame(raw_data)
    mapping_df = spark.createDataFrame(mapping_data)

    result_df = resolve_player_identities(raw_df, mapping_df)
    records = result_df.collect()

    assert len(records) == 1
    assert records[0]["player_id"] == 1164


@pytest.mark.warehouse
def test_player_resolution_unmapped_hard_fail(spark: "SparkSession") -> None:
    """U-ENT-02: Missing governance mapping identities trigger an UnresolvedEntityError."""
    raw_data = [Row(raw_name="K. De Bruyne"), Row(raw_name="Unknown Player")]
    mapping_data = [Row(raw_name="K. De Bruyne", player_id=45)]

    raw_df = spark.createDataFrame(raw_data)
    mapping_df = spark.createDataFrame(mapping_data)

    with pytest.raises(UnresolvedEntityError):
        resolve_player_identities(raw_df, mapping_df)


# =========================================================================
# TRANSACTIONAL SCD TYPE 2 INGESTION TIMELINE GATES (U-SCD-01 to U-SCD-04)
# =========================================================================


@pytest.mark.warehouse
def test_injury_scd2_new_initialization(spark: "SparkSession") -> None:
    """U-SCD-01: Verifies new record tracking defaults to high 9999 valid_to dates."""
    schema = (
        "player_id INT, category STRING, status STRING, "
        "valid_from DATE, valid_to DATE, is_current BOOLEAN"
    )
    empty_target = spark.createDataFrame([], schema)

    batch_data = [
        Row(
            player_id=1164,
            category="Thigh Injury",
            status="50%",
            effective_date=datetime.date(2026, 5, 17),
        )
    ]
    batch_df = spark.createDataFrame(batch_data)

    result_df = process_injury_scd2(empty_target, batch_df, logical_date=datetime.date(2026, 5, 17))
    records = result_df.collect()

    assert len(records) == 1
    assert records[0]["valid_from"] == datetime.date(2026, 5, 17)
    assert records[0]["valid_to"] == datetime.date(9999, 12, 31)
    assert records[0]["is_current"] is True


@pytest.mark.warehouse
def test_injury_scd2_historic_stitch_gaps(spark: "SparkSession") -> None:
    """U-SCD-02: Verifies pre-dated backfill records cleanly adjust historical boundaries."""
    target_data = [
        Row(
            player_id=1164,
            category="Thigh Injury",
            status="Fit",
            valid_from=datetime.date(2026, 5, 17),
            valid_to=datetime.date(9999, 12, 31),
            is_current=True,
        )
    ]
    batch_data = [
        Row(
            player_id=1164,
            category="Thigh Injury",
            status="50%",
            effective_date=datetime.date(2026, 5, 10),
        )
    ]

    target_df = spark.createDataFrame(target_data)
    batch_df = spark.createDataFrame(batch_data)

    result_df = process_injury_scd2(target_df, batch_df, logical_date=datetime.date(2026, 5, 17))
    records = result_df.orderBy("valid_from").collect()

    assert len(records) == 2
    assert records[0]["status"] == "50%"
    assert records[0]["valid_to"] == datetime.date(2026, 5, 17)
    assert records[1]["status"] == "Fit"
    assert records[1]["valid_from"] == datetime.date(2026, 5, 17)


@pytest.mark.warehouse
def test_injury_scd2_overlap_timeline_exception(spark: "SparkSession") -> None:
    """U-SCD-03: Overlapping state update collisions raise an SCD2TimelineException."""
    schema = (
        "player_id INT, category STRING, status STRING, "
        "valid_from DATE, valid_to DATE, is_current BOOLEAN"
    )
    target_df = spark.createDataFrame([], schema)

    corrupted_batch_data = [
        Row(
            player_id=1164,
            category="Thigh Injury",
            status="Fit",
            effective_date=datetime.date(2026, 5, 17),
        ),
        Row(
            player_id=1164,
            category="Thigh Injury",
            status="50%",
            effective_date=datetime.date(2026, 5, 17),
        ),
    ]
    corrupted_batch_df = spark.createDataFrame(corrupted_batch_data)

    with pytest.raises(SCD2TimelineException):
        process_injury_scd2(target_df, corrupted_batch_df, logical_date=datetime.date(2026, 5, 17))


@pytest.mark.warehouse
def test_injury_scd2_future_date_gate(spark: "SparkSession") -> None:
    """U-SCD-04: Ingesting update metrics dated into the future triggers a FutureDateError."""
    schema = (
        "player_id INT, category STRING, status STRING, "
        "valid_from DATE, valid_to DATE, is_current BOOLEAN"
    )
    target_df = spark.createDataFrame([], schema)

    future_batch_data = [
        Row(
            player_id=1164,
            category="Thigh Injury",
            status="50%",
            effective_date=datetime.date(2026, 12, 25),
        )
    ]
    future_batch_df = spark.createDataFrame(future_batch_data)

    with pytest.raises(FutureDateError):
        process_injury_scd2(target_df, future_batch_df, logical_date=datetime.date(2026, 5, 17))


# =========================================================================
# FACT UNIQUE COMPOSITE KEY CONSTRAINT GATES (U-FCT-01)
# =========================================================================


@pytest.mark.warehouse
def test_fact_composite_pk_constraint_violation(spark: "SparkSession") -> None:
    """U-FCT-01: Verifies composite natural keys enforce uniqueness constraints across facts."""
    corrupted_shot_fact = [
        Row(player_id=101, match_id=29088, xg=0.0176, shot_type="LeftFoot"),
        Row(player_id=101, match_id=29088, xg=0.0176, shot_type="DuplicateRowCollision"),
    ]
    fact_df = spark.createDataFrame(corrupted_shot_fact)

    with pytest.raises(IntraBatchDuplicateError):
        validate_fact_composite_keys(fact_df, composite_pks=["player_id", "match_id", "xg"])
