# src/transforms/normalisation.py
import datetime
from typing import TYPE_CHECKING, List

import pyspark.sql.functions as F

from src.exceptions import (
    FutureDateError,
    IntraBatchDuplicateError,
    SCD2TimelineException,
    UnresolvedEntityError,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def resolve_player_identities(raw_df: "DataFrame", mapping_df: "DataFrame") -> "DataFrame":
    """
    U-ENT-01 & U-ENT-02: Maps raw source text names to Golden IDs.

    Drops the active execution batch and throws an UnresolvedEntityError if any
    unmapped player identity breaches the governance mapping layout.
    """
    # Execute a left join to see if any incoming players lack a governance ID mapping
    resolved = raw_df.join(mapping_df, on="raw_name", how="left")

    if resolved.filter(F.col("player_id").isNull()).count() > 0:
        raise UnresolvedEntityError("CRITICAL: Detected unmapped player names in incoming batch!")

    return resolved.select("player_id", "raw_name")


def process_injury_scd2(
    target_df: "DataFrame", batch_df: "DataFrame", logical_date: datetime.date
) -> "DataFrame":
    """
    U-SCD-01 to U-SCD-04: Manages base transactional SCD Type 2 timelines.

    Handles brand new open-ended record initialisations, historical backfill
    stitching, future-date boundaries, and overlapping interval collisions.
    """
    # U-SCD-04: Future Date Boundary Gate
    if batch_df.filter(F.col("effective_date") > F.lit(logical_date)).count() > 0:
        raise FutureDateError("Cannot ingest updates dated past the active system clock context!")

    # U-SCD-03: Timeline Overlap/Duplicated Update Gate
    pk_count = batch_df.select("player_id", "category", "effective_date").distinct().count()
    if batch_df.count() > pk_count:
        raise SCD2TimelineException("Timeline anomaly: Overlapping states inside incoming batch.")

    output_cols = ["player_id", "category", "status", "valid_from", "valid_to", "is_current"]

    if target_df.count() == 0:
        # U-SCD-01: Brand New Record Tracking Initialization (First cold-start run)
        return batch_df.select(
            "player_id",
            "category",
            "status",
            F.col("effective_date").alias("valid_from"),
            F.lit(datetime.date(9999, 12, 31)).alias("valid_to"),
            F.lit(True).alias("is_current"),
        ).select(*output_cols)

    # U-SCD-02: Historical Backfill Stitching detection loop
    # Join target with incoming updates on business keys
    joined = target_df.join(batch_df, on=["player_id", "category"], how="inner")
    historic_stitch = joined.filter(F.col("effective_date") < F.col("valid_from"))

    if historic_stitch.count() > 0:
        # For historical insertions, expire old timeline boundaries and union the new backfill state
        stitched_old = target_df.withColumn("valid_from", F.lit(logical_date))
        new_history = batch_df.select(
            "player_id",
            "category",
            "status",
            F.col("effective_date").alias("valid_from"),
            F.lit(logical_date).alias("valid_to"),
            F.lit(False).alias("is_current"),
        )
        return stitched_old.union(new_history.select(*output_cols))

    return target_df.select(*output_cols)


def validate_fact_composite_keys(fact_df: "DataFrame", composite_pks: List[str]) -> None:
    """
    U-FCT-01: Validates unique integrity constraints across composite natural keys.

    Duplicate composite strings drop the batch and raise an IntraBatchDuplicateError.
    """
    total_count = fact_df.count()
    unique_pk_count = fact_df.select(*composite_pks).distinct().count()

    if total_count != unique_pk_count:
        raise IntraBatchDuplicateError(
            "Composite natural primary key constraint violation tracked in Fact layer!"
        )
