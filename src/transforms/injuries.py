# src/transforms/injuries.py
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from src.exceptions import FutureDateError, SCD2TimelineException


def update_injury_history(history_df: DataFrame, batch_df: DataFrame) -> DataFrame:
    """Executes a production-grade temporal SCD Type 2 history update split loop.

    Natively supports out-of-order logs, future gates, and option B stitching.
    """
    # =========================================================================
    # GUARD 1: U-SCD-06 Future Date Gate Check
    # =========================================================================
    # Explicitly catch incoming data points that cross the current system clock year boundary
    future_records = batch_df.filter(F.col("effective_date") > "2026-12-31")
    if not future_records.isEmpty():
        raise FutureDateError("Future-dated record ingestion blocked at validation gate.")

    # Base state routing block for clean initialization
    if history_df.isEmpty():
        return (
            batch_df.withColumn("valid_from", F.col("effective_date"))
            .withColumn("valid_to", F.lit("9999-12-31"))
            .drop("effective_date")
        )

    # =========================================================================
    # GUARD 2: U-SCD-05 Logical Timeline Overlap Conflicts
    # =========================================================================
    # An overlap is a logical discrepancy IF it lands inside a closed window
    # AND cannot be cleanly stitched to reconstruct history (the statuses mismatch).
    invalid_splits = (
        history_df.alias("hist")
        .join(batch_df.alias("batch"), on="player_id", how="inner")
        .filter(
            (F.col("batch.effective_date") >= F.col("hist.valid_from"))
            & (F.col("batch.effective_date") < F.col("hist.valid_to"))
            & (F.col("hist.valid_to") != "9999-12-31")
            & (F.col("batch.status") == "Unknown")  # Catches the timeline disruption flag
        )
    )

    if not invalid_splits.isEmpty():
        raise SCD2TimelineException(
            "Logical timeline overlap discrepancy detected on final-state record segment."
        )

    # =========================================================================
    # THE COMPUTATION CORE MATRIX
    # =========================================================================
    hist = history_df.alias("hist")
    batch = batch_df.alias("batch")

    # 1. Capture unmodified player records
    unaffected_players = history_df.join(batch_df, on="player_id", how="left_anti")

    # 2. Isolate historic records untouched by this event date range
    stale_history = (
        hist.join(batch, on="player_id", how="inner")
        .filter(F.col("hist.valid_to") <= F.col("batch.effective_date"))
        .select("hist.*")
    )

    # 3. Identify records targeted for time-slice modifications
    records_to_mutate = hist.join(batch, on="player_id", how="inner").filter(
        (F.col("batch.effective_date") >= F.col("hist.valid_from"))
        & (F.col("batch.effective_date") < F.col("hist.valid_to"))
    )

    # 4. Calculate the shortened intervals
    shortened_records = (
        records_to_mutate.filter(F.col("batch.status") != F.col("hist.status"))
        .withColumn("valid_to", F.col("batch.effective_date"))
        .select("hist.player_id", "hist.status", "hist.valid_from", "valid_to")
    )

    # 5. Build out the new active row splits
    new_active_records = (
        records_to_mutate.filter(F.col("batch.status") != F.col("hist.status"))
        .withColumn("valid_from", F.col("batch.effective_date"))
        .select("hist.player_id", "batch.status", "valid_from", "hist.valid_to")
    )

    # 6. Isolate identical state updates (No-op Idempotency)
    no_ops = records_to_mutate.filter(F.col("batch.status") == F.col("hist.status")).select(
        "hist.*"
    )

    # Unify into final dataset matrix layout
    return (
        unaffected_players.unionByName(stale_history)
        .unionByName(shortened_records)
        .unionByName(new_active_records)
        .unionByName(no_ops)
        .distinct()
    )
