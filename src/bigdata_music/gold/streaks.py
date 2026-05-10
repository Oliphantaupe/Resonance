"""Gold — streak_analysis.

Question: What is the streak duration distribution of #1 tracks per region?

Algorithm: classic gap-and-island detection using row_number() over date order.

Window functions used (AA2 evidence):
  #1 — row_number() over (track_id, region ORDER BY chart_date)
  #2 — date_sub(chart_date, rn) to create a per-streak group key

# RAPPORT: The gap-and-island pattern is a classic SQL interview problem.
# The key insight is that for consecutive dates, (date - row_number) is
# constant within the streak and changes between streaks. This lets us
# GROUP BY (track_id, region, streak_group) to get streak boundaries.

Powers: Dashboard page "Streak Champions" — sortable table + per-region timelines.
"""
GOLD_TABLE = "streak_analysis"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, date_sub, max as _max, min as _min,
    row_number, sum as _sum,
)
from pyspark.sql.window import Window

from bigdata_music import config
from bigdata_music.utils.delta_io import read_delta
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def build_streak_analysis(spark: SparkSession) -> int:
    """Compute #1 streak durations per (track, region) → gold/streak_analysis (Delta).

    Returns the row count written.
    """
    log.info("Gold streaks: loading %s", config.SILVER_CHARTS_ENRICHED)
    df = read_delta(spark, config.SILVER_CHARTS_ENRICHED)

    # Step 1: filter to rank=1 only
    top_only = df.filter(col("rank") == 1).select(
        "track_id", "title", "artist", "region", "chart_date", "streams", "year",
    )

    # Step 2: row_number over (track_id, region) ordered by chart_date
    # Window function #1
    w_date = Window.partitionBy("track_id", "region").orderBy("chart_date")
    with_rn = top_only.withColumn("_rn", row_number().over(w_date))

    # Step 3: streak_group = chart_date - row_number (days offset)
    # Rows in the same consecutive streak share the same (track_id, region, streak_group)
    # Window function #2 (date arithmetic to form island key)
    with_group = with_rn.withColumn(
        "streak_group",
        date_sub(col("chart_date"), col("_rn").cast("int")),
    )

    # Step 4: aggregate per streak
    streaks = (
        with_group
        .groupBy("track_id", "title", "artist", "region", "streak_group")
        .agg(
            _min("chart_date").alias("streak_start"),
            _max("chart_date").alias("streak_end"),
            count("*").alias("streak_days"),
            _sum("streams").alias("streak_total_streams"),
        )
    )

    streaks = streaks.coalesce(4).cache()
    row_count = streaks.count()
    log.info("Gold streaks: writing %d streak records to %s", row_count, config.GOLD_STREAK_ANALYSIS)

    (
        streaks.write
        .format("delta")
        .mode("overwrite")
        .save(config.GOLD_STREAK_ANALYSIS)
    )

    streaks.unpersist()
    log.info("Gold streaks: write complete")
    return row_count
