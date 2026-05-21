# src/transforms/metrics.py
import datetime
from typing import TYPE_CHECKING

import pyspark.sql.functions as F
from pyspark.sql.window import Window

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def compute_player_injury_summary(
    injury_df: "DataFrame", current_processing_date: datetime.date
) -> "DataFrame":
    """
    U-MTR-01, U-MTR-02, U-MTR-03: Precomputes historical player injury summaries.

    Clamps open-ended timelines, builds career cumulative days lost, and derives
    normalised player availability coefficients.
    """
    duration_expr = F.when(
        F.col("valid_to") == F.lit(datetime.date(9999, 12, 31)),
        F.datediff(F.lit(current_processing_date), F.col("valid_from")),
    ).otherwise(F.datediff(F.col("valid_to"), F.col("valid_from")))

    df_with_duration = injury_df.withColumn("injury_duration_days", duration_expr)

    player_window = (
        Window.partitionBy("player_id")
        .orderBy("valid_from")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df_with_cumulative = df_with_duration.withColumn(
        "cumulative_days_lost", F.sum("injury_duration_days").over(player_window)
    )

    total_season_days = 300
    return df_with_cumulative.withColumn(
        "availability_coefficient",
        (F.lit(total_season_days) - F.col("cumulative_days_lost")) / F.lit(total_season_days),
    )


def compute_match_squad_health_deficit(
    injury_df: "DataFrame", roster_df: "DataFrame", shot_df: "DataFrame"
) -> "DataFrame":
    """
    U-MTR-04 to U-MTR-07: Precomputes match squad health deficits and performance drag.

    Removes ambiguous reference paths by explicitly using matching array condition lists.
    """
    player_match_window = Window.partitionBy("player_id").orderBy("match_id").rowsBetween(-5, -1)

    # 1. Isolate baselines, keeping references clear
    player_baselines = (
        shot_df.withColumn("rolling_player_xg_5match", F.avg("xg").over(player_match_window))
        .withColumn(
            "rolling_player_xga_5match",
            F.avg("xg").over(player_match_window) * 1.1,
        )
        .select("player_id", "match_id", "rolling_player_xg_5match", "rolling_player_xga_5match")
        .distinct()
    )

    # 2. FIX: Join on explicit list arrays to merge overlapping columns into a single reference node
    sidelined_players = injury_df.join(roster_df, on=["player_id", "match_id"], how="inner")

    # 3. FIX: Join drag metrics on combined columns to prevent duplicate column retention
    drag_metrics = sidelined_players.join(
        player_baselines, on=["player_id", "match_id"], how="inner"
    )

    return drag_metrics.groupBy("match_id", "team_id").agg(
        F.count("player_id").alias("active_injured_count"),
        F.sum("rolling_player_xg_5match").alias("total_squad_xg_drag"),
        F.sum("rolling_player_xga_5match").alias("total_squad_xga_drag"),
    )
