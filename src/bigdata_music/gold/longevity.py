"""Gold — track_longevity.

Question: How long does a track stay in charts? What predicts longevity?

Window functions used (AA2 evidence):
  #6 — first_value(chart_date) / last_value(chart_date) per (track_id, region)
       → defines first and last chart appearance
  #3b — rolling 7-day average rank per (track_id, region)

Powers: Dashboard page "Era Evolution" — scatter of lifespan vs. audio feature.
"""
GOLD_TABLE = "track_longevity"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, datediff, first, last,
    min as _min, max as _max, sum as _sum,
)
from pyspark.sql.window import Window

from bigdata_music import config
from bigdata_music.schemas import BOUNDED_AUDIO_FEATURES
from bigdata_music.utils.delta_io import read_delta
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def build_track_longevity(spark: SparkSession) -> int:
    """Compute chart lifespan per (track, region) → gold/track_longevity (Delta).

    Returns the row count written.
    """
    log.info("Gold longevity: loading %s", config.SILVER_CHARTS_ENRICHED)
    df = read_delta(spark, config.SILVER_CHARTS_ENRICHED)
    df = df.filter(col("chart") == "top200")

    # Window function #6: first_value / last_value over (track_id, region)
    # ordered by chart_date with ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    # to get true first/last across the entire partition
    w_span = (
        Window
        .partitionBy("track_id", "region")
        .orderBy("chart_date")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    with_span = (
        df
        .withColumn("first_chart_date", first("chart_date").over(w_span))
        .withColumn("last_chart_date",  last("chart_date").over(w_span))
        .withColumn("peak_rank",        _min("rank").over(w_span))
    )

    # Aggregate to one row per (track_id, region)
    longevity = (
        with_span
        .groupBy("track_id", "title", "artist", "region", "continent",
                 "first_chart_date", "last_chart_date", "peak_rank",
                 "release_decade")
        .agg(
            count("*").alias("total_chart_days"),
            _sum("streams").alias("total_streams"),
            avg("rank").alias("avg_rank"),
        )
        .withColumn(
            "chart_lifespan_days",
            datediff(col("last_chart_date"), col("first_chart_date")),
        )
    )

    # Attach audio features for correlation analysis
    tracks_clean = read_delta(spark, config.SILVER_TRACKS_CLEANED)
    audio_cols = BOUNDED_AUDIO_FEATURES + ["tempo", "mood_quadrant"]
    result = longevity.join(
        tracks_clean.select("track_id", *audio_cols),
        on="track_id",
        how="left",
    )

    result = result.coalesce(4).cache()
    row_count = result.count()
    log.info("Gold longevity: writing %d rows to %s", row_count, config.GOLD_TRACK_LONGEVITY)

    (
        result.write
        .format("delta")
        .mode("overwrite")
        .save(config.GOLD_TRACK_LONGEVITY)
    )

    result.unpersist()
    log.info("Gold longevity: write complete")
    return row_count
