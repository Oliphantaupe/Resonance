"""Explicit StructType definitions for all three raw sources.

Declaring schemas here rather than using inferSchema=true prevents Spark
from silently mistyping edge-case values (e.g. rank="N/A", streams="" get
cast to null instead of crashing the pipeline mid-run).
"""
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType,
)

# ---------------------------------------------------------------------------
# Source 1 — Spotify Charts (charts.csv)
# rank and streams arrive as strings on disk; we cast at Silver.
# _corrupt_record is appended at read time in PERMISSIVE mode.
# ---------------------------------------------------------------------------
CHARTS_RAW_SCHEMA = StructType([
    StructField("title",   StringType(), True),
    StructField("rank",    StringType(), True),   # cast to int at Silver
    StructField("date",    StringType(), True),   # cast to date at Silver
    StructField("artist",  StringType(), True),
    StructField("url",     StringType(), True),   # track_id extracted at Silver
    StructField("region",  StringType(), True),
    StructField("chart",   StringType(), True),
    StructField("trend",   StringType(), True),
    StructField("streams", StringType(), True),   # cast to long at Silver; null for viral50
])

# Appended automatically when reading with PERMISSIVE mode
CORRUPT_RECORD_FIELD = StructField("_corrupt_record", StringType(), True)

# ---------------------------------------------------------------------------
# Source 2 — Spotify Track Analysis (track_features.parquet)
# Parquet already has types; we redeclare to validate the file matches
# what we expect and to document our usage contract.
# ---------------------------------------------------------------------------
TRACK_FEATURES_RAW_SCHEMA = StructType([
    StructField("track_id",                  StringType(),  False),
    StructField("artist_name",               StringType(),  True),
    StructField("track_name",                StringType(),  True),
    StructField("album_name",                StringType(),  True),
    StructField("album_release_date",        DateType(),    True),
    StructField("duration_ms",               IntegerType(), True),
    StructField("explicit",                  IntegerType(), True),
    StructField("track_popularity",          IntegerType(), True),
    StructField("album_popularity",          IntegerType(), True),
    StructField("artist_popularity",         IntegerType(), True),
    StructField("artist_followers",          LongType(),    True),
    StructField("tempo",                     DoubleType(),  True),
    StructField("key",                       IntegerType(), True),
    StructField("mode",                      IntegerType(), True),
    StructField("danceability",              DoubleType(),  True),
    StructField("energy",                    DoubleType(),  True),
    StructField("loudness",                  DoubleType(),  True),
    StructField("speechiness",               DoubleType(),  True),
    StructField("acousticness",              DoubleType(),  True),
    StructField("instrumentalness",          DoubleType(),  True),
    StructField("liveness",                  DoubleType(),  True),
    StructField("valence",                   DoubleType(),  True),
    StructField("energy_danceability_score", DoubleType(),  True),
])

# Audio feature columns that must stay in [0, 1] — clamped at Silver
BOUNDED_AUDIO_FEATURES = [
    "danceability", "energy", "speechiness",
    "acousticness", "instrumentalness", "liveness", "valence",
]

# ---------------------------------------------------------------------------
# Source 3 — Country Enrichment (countries.csv)
# Small hand-crafted file; we still enforce schema to catch the deliberate
# quirks (BOM, leading whitespace) and validate types.
# ---------------------------------------------------------------------------
COUNTRIES_RAW_SCHEMA = StructType([
    StructField("region_code",       StringType(),  False),
    StructField("country_name",      StringType(),  True),
    StructField("continent",         StringType(),  True),
    StructField("population_2020",   LongType(),    True),
    StructField("gdp_per_capita_usd", DoubleType(), True),
])
