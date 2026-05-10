"""Gold — global_top_artists.

Question: Who are the most consistent global hitmakers, and how does
their sound evolve over time?

Window functions used (AA2 evidence):
  #3c — Rolling 90-day stream sum per artist (rangeBetween with interval)
  #4  — dense_rank() for monthly leaderboards (demonstrates difference
        from rank() and row_number())

Powers: Dashboard page "Top Artists" + drill-down.
"""
GOLD_TABLE = "global_top_artists"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, countDistinct, dense_rank,
    sum as _sum, trunc,
)
from pyspark.sql.window import Window

from bigdata_music import config
from bigdata_music.schemas import BOUNDED_AUDIO_FEATURES
from bigdata_music.utils.delta_io import read_delta
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def build_top_artists(spark: SparkSession) -> int:
    """Compute artist rolling popularity + monthly ranks → gold/global_top_artists (Delta).

    Returns the row count written.
    """
    log.info("Gold top_artists: loading %s", config.SILVER_CHARTS_ENRICHED)
    df = read_delta(spark, config.SILVER_CHARTS_ENRICHED)
    df = df.filter(col("chart") == "top200")

    # Window function #3c: rolling 90-day stream sum per artist
    # rangeBetween uses seconds for date-typed columns; 90 days = 90 * 86400 seconds
    w_rolling = (
        Window
        .partitionBy("artist")
        .orderBy(col("chart_date").cast("timestamp").cast("long"))
        .rangeBetween(-90 * 86400, 0)
    )

    artist_rolling = (
        df
        .withColumn("rolling_90d_streams", _sum("streams").over(w_rolling))
        .withColumn("month_start", trunc(col("chart_date"), "month"))
    )

    # Monthly artist aggregates
    monthly = (
        artist_rolling
        .groupBy("artist", "month_start")
        .agg(
            _sum("streams").alias("monthly_streams"),
            countDistinct("track_id").alias("distinct_tracks"),
            count("*").alias("chart_entries"),
            avg("rolling_90d_streams").alias("avg_rolling_90d_streams"),
        )
    )

    # Window function #4: dense_rank() for monthly artist leaderboard
    # dense_rank() vs rank(): rank() leaves gaps after ties (1,1,3,4);
    # dense_rank() does not (1,1,2,3) — correct for a "top-N artists" UI.
    w_monthly = Window.partitionBy("month_start").orderBy(col("monthly_streams").desc())
    monthly_ranked = monthly.withColumn("monthly_rank", dense_rank().over(w_monthly))

    # Keep only top-100 per month to limit Gold table size
    result = monthly_ranked.filter(col("monthly_rank") <= 100)

    # Attach median audio feature vector per artist (from track-level Silver)
    tracks_clean = read_delta(spark, config.SILVER_TRACKS_CLEANED)
    audio_cols = BOUNDED_AUDIO_FEATURES + ["tempo"]

    artist_audio = (
        df.select("artist", "track_id").distinct()
        .join(tracks_clean.select("track_id", *audio_cols), on="track_id", how="left")
        .groupBy("artist")
        .agg(*[avg(c).alias(f"avg_{c}") for c in audio_cols])
    )

    result = result.join(artist_audio, on="artist", how="left")

    result = result.coalesce(4).cache()
    row_count = result.count()
    log.info("Gold top_artists: writing %d rows to %s", row_count, config.GOLD_TOP_ARTISTS)

    (
        result.write
        .format("delta")
        .mode("overwrite")
        .save(config.GOLD_TOP_ARTISTS)
    )

    result.unpersist()
    log.info("Gold top_artists: write complete")
    return row_count
