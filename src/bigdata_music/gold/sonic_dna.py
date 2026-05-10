"""Gold — sonic_dna_by_rank_tier.

Question: Do hit tracks (top decile) have measurably different audio profiles
than mid-tier tracks?

Window functions used (AA2 evidence):
  #3 — lag() for week-over-week rank trajectory delta
  #5 — ntile(10) for decile bucketing by total streams

Powers: Dashboard page "Sonic DNA" — violin plots + radar charts by rank tier.
"""
GOLD_TABLE = "sonic_dna_by_rank_tier"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, lag, min as _min, ntile,
    stddev, sum as _sum,
)
from pyspark.sql.window import Window

from bigdata_music import config
from bigdata_music.schemas import BOUNDED_AUDIO_FEATURES
from bigdata_music.utils.delta_io import read_delta
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)

AUDIO_FEATURES = BOUNDED_AUDIO_FEATURES + ["tempo", "loudness"]


def build_sonic_dna(spark: SparkSession) -> int:
    """Compute audio feature statistics by rank tier → gold/sonic_dna_by_rank_tier (Delta).

    Returns the row count written.
    """
    log.info("Gold sonic_dna: loading %s", config.SILVER_CHARTS_ENRICHED)
    df = read_delta(spark, config.SILVER_CHARTS_ENRICHED)

    # Filter top200 only (viral50 has null streams and different ranking logic)
    df = df.filter(col("chart") == "top200")

    # Per-track chart aggregates
    track_stats = (
        df.groupBy("track_id", "title", "artist")
        .agg(
            _min("rank").alias("peak_rank"),
            count("*").alias("total_chart_days"),
            _sum("streams").alias("total_streams"),
        )
    )

    # Window function #5: ntile(10) buckets tracks by total streams into deciles
    # Decile 1 = highest streams (hits), decile 10 = lowest (long tail)
    w_global = Window.orderBy(col("total_streams").desc())
    track_tiers = track_stats.withColumn(
        "stream_decile",
        ntile(10).over(w_global),
    )

    # Window function #3: lag() for rank trajectory — week-over-week rank change
    # Computed per (track_id, region) to track momentum across regions
    df_top200 = df.filter(col("chart") == "top200")
    w_week = Window.partitionBy("track_id", "region").orderBy("chart_date")
    rank_trajectory = (
        df_top200
        .withColumn("prev_rank", lag("rank", 7).over(w_week))
        .withColumn("rank_delta_7d", col("rank") - col("prev_rank"))
        .groupBy("track_id")
        .agg(avg("rank_delta_7d").alias("avg_rank_delta_7d"))
    )

    # Join with audio features via track tiers
    tracks_clean = read_delta(spark, config.SILVER_TRACKS_CLEANED)

    enriched = (
        track_tiers
        .join(tracks_clean.select("track_id", *AUDIO_FEATURES, "mood_quadrant"), on="track_id", how="left")
        .join(rank_trajectory, on="track_id", how="left")
    )

    # Aggregate audio feature statistics per decile tier
    feature_aggs = []
    for feat in AUDIO_FEATURES:
        feature_aggs += [
            avg(col(feat)).alias(f"mean_{feat}"),
            stddev(col(feat)).alias(f"std_{feat}"),
        ]

    result = (
        enriched
        .groupBy("stream_decile")
        .agg(
            count("*").alias("track_count"),
            avg("peak_rank").alias("avg_peak_rank"),
            avg("total_chart_days").alias("avg_chart_days"),
            avg("avg_rank_delta_7d").alias("avg_rank_delta_7d"),
            *feature_aggs,
        )
        .orderBy("stream_decile")
    )

    result = result.coalesce(4).cache()
    row_count = result.count()
    log.info("Gold sonic_dna: writing %d rows (10 decile tiers) to %s",
             row_count, config.GOLD_SONIC_DNA)

    (
        result.write
        .format("delta")
        .mode("overwrite")
        .save(config.GOLD_SONIC_DNA)
    )

    result.unpersist()
    log.info("Gold sonic_dna: write complete")
    return row_count
