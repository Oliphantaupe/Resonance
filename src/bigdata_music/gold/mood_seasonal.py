"""Gold — regional_mood_seasonal.

Question: Which mood quadrant dominates each region in each season?

Operations used:
  - when/otherwise chain for season derivation from month
  - Group by (region, year, season, mood_quadrant)
  - Window function: sum(streams) as fraction of total seasonal streams
    (mood_share) via SUM OVER PARTITION

Powers: Dashboard page "Regional Mood Map" — animated choropleth + season slider.
"""
GOLD_TABLE = "regional_mood_seasonal"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, when,
)
from pyspark.sql.window import Window

from bigdata_music import config
from bigdata_music.utils.delta_io import read_delta
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def derive_season(df):
    """Map calendar month → meteorological season (Northern Hemisphere)."""
    return df.withColumn(
        "season",
        when(col("month").isin(12, 1, 2), "Winter")
        .when(col("month").isin(3, 4, 5),  "Spring")
        .when(col("month").isin(6, 7, 8),  "Summer")
        .otherwise("Autumn"),
    )


def build_regional_mood_seasonal(spark: SparkSession) -> int:
    """Aggregate enriched Silver → gold/regional_mood_seasonal (Delta).

    Returns the row count written.
    """
    log.info("Gold mood_seasonal: loading %s", config.SILVER_CHARTS_ENRICHED)
    df = read_delta(spark, config.SILVER_CHARTS_ENRICHED)

    df = derive_season(df)

    # Aggregate per (region, year, season, mood_quadrant)
    agg = (
        df.groupBy("region", "continent", "year", "season", "mood_quadrant")
        .agg(
            _sum("streams").alias("total_streams"),
            countDistinct("track_id").alias("distinct_tracks"),
            count("*").alias("chart_entries"),
        )
    )

    # mood_share: fraction of seasonal streams for this mood quadrant in this region/year
    w_season = Window.partitionBy("region", "year", "season")
    result = agg.withColumn(
        "mood_share",
        col("total_streams") / _sum("total_streams").over(w_season),
    )

    result = result.coalesce(4).cache()
    row_count = result.count()
    log.info("Gold mood_seasonal: writing %d rows to %s", row_count, config.GOLD_REGIONAL_MOOD)

    (
        result.write
        .format("delta")
        .mode("overwrite")
        .save(config.GOLD_REGIONAL_MOOD)
    )

    result.unpersist()
    log.info("Gold mood_seasonal: write complete")
    return row_count
