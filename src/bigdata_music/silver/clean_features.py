"""Silver cleaning — tracks_cleaned.

Produces two derived tables:
  1. track-level  : one row per track_id (audio features are track-invariant;
                    artist names collapsed into an array)
  2. track-artist : kept as-is from Bronze (one row per track × artist)
     → stored at Silver for collaboration analysis used in Gold top_artists

Cleaning operations:
  - Clamp audio features to [0, 1] (occasionally out-of-range in source data)
  - Drop rows with null track_id (join key)
  - Add mood_quadrant label (Russell's circumplex, 1980)
  - Add release_decade for era analysis
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array_distinct, collect_set, col, count, floor, lit,
    greatest, least, when, year,
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

from bigdata_music import config
from bigdata_music.schemas import BOUNDED_AUDIO_FEATURES
from bigdata_music.utils.delta_io import read_delta
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def clamp_audio_features(df: DataFrame) -> DataFrame:
    """Clamp all bounded audio features to [0, 1].

    # RAPPORT: the HuggingFace dataset has a small number of rows where
    # instrumentalness or speechiness slightly exceeds 1.0 (float precision
    # artefact from ClickHouse export). Clamping is safer than dropping.
    """
    for feat in BOUNDED_AUDIO_FEATURES:
        df = df.withColumn(feat, least(greatest(col(feat), lit(0.0)), lit(1.0)))
    return df


def assign_mood_quadrant(df: DataFrame) -> DataFrame:
    """Classify each track into a mood quadrant using Russell's circumplex model (1980).

    Two axes: valence (negative ↔ positive affect) × energy (low ↔ high arousal).
    This is an academically grounded classification, not a hand-rolled heuristic.

    Reference: Russell, J.A. (1980). A circumplex model of affect.
    Journal of Personality and Social Psychology, 39(6), 1161–1178.
    """
    return df.withColumn(
        "mood_quadrant",
        when(
            (col("valence") > 0.5) & (col("energy") > 0.5), "Happy/Energetic"
        ).when(
            (col("valence") > 0.5) & (col("energy") <= 0.5), "Calm/Peaceful"
        ).when(
            (col("valence") <= 0.5) & (col("energy") > 0.5), "Angry/Tense"
        ).otherwise("Sad/Depressing"),
    )


def build_track_level_view(df: DataFrame) -> DataFrame:
    """Deduplicate to one row per track_id.

    Audio features are track-invariant (same regardless of credited artist),
    so we keep the first row per track_id. Artist names are collapsed into
    an array to preserve collaboration information.

    Uses row_number() + collect_set rather than dropDuplicates() so we have
    a deterministic result and keep the artists array.
    """
    w = Window.partitionBy("track_id").orderBy("artist_name")

    track_level = (
        df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn", "artist_name")
    )

    # Collect all artist names per track into an array
    artists = (
        df.groupBy("track_id")
        .agg(array_distinct(collect_set("artist_name")).alias("artists"))
    )

    return track_level.join(artists, on="track_id", how="left")


def add_release_decade(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "release_decade",
        (floor(year(col("album_release_date")) / 10) * 10).cast("int"),
    )


def build_silver_features(spark: SparkSession) -> tuple[int, int]:
    """Clean Bronze features → two Silver Delta tables.

    Returns (track_level_count, track_artist_count).
    """
    log.info("Silver: loading bronze features from %s", config.BRONZE_TRACK_FEATURES)
    df = read_delta(spark, config.BRONZE_TRACK_FEATURES)

    # Drop rows with null track_id — unusable as join key (single-pass null count)
    null_count = df.agg(count(when(col("track_id").isNull(), 1)).alias("n")).first()["n"]
    df = df.filter(col("track_id").isNotNull())
    log.info("Silver features: dropped %d rows with null track_id", null_count)

    df = clamp_audio_features(df)
    df = assign_mood_quadrant(df)
    df = add_release_decade(df)

    # --- Track-artist view (one row per track × artist) ---
    # Write first, then read back from disk. This keeps the 56M-row Bronze-backed
    # plan out of memory when building the track-level view, preventing heap OOM.
    log.info("Silver features (track-artist): writing to %s",
             config.SILVER_TRACKS_CLEANED + "_track_artist")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(config.SILVER_TRACKS_CLEANED + "_track_artist")
    )
    df_ta = read_delta(spark, config.SILVER_TRACKS_CLEANED + "_track_artist")
    ta_count = df_ta.count()
    log.info("Silver features (track-artist): wrote %d rows", ta_count)

    # --- Track-level view (deduplicated, artists collapsed) ---
    # Built from the already-written parquet data — no Bronze recomputation needed.
    df_track = build_track_level_view(df_ta)
    t_count = df_track.count()
    log.info("Silver features (track-level): writing %d rows to %s",
             t_count, config.SILVER_TRACKS_CLEANED)
    (
        df_track.write
        .format("delta")
        .mode("overwrite")
        .save(config.SILVER_TRACKS_CLEANED)
    )

    log.info("Silver features: write complete")
    return t_count, ta_count
