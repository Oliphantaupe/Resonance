"""Central configuration — all paths and tuning knobs live here."""
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths — DATA_ROOT is /data inside Docker, override locally via env var
# ---------------------------------------------------------------------------
DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data"))

RAW     = DATA_ROOT / "raw"
BRONZE  = DATA_ROOT / "bronze"
SILVER  = DATA_ROOT / "silver"
GOLD    = DATA_ROOT / "gold"

# Raw source files
CHARTS_RAW          = RAW / "charts.csv"
TRACK_FEATURES_RAW  = RAW / "track_features.parquet"
COUNTRIES_RAW       = RAW / "countries.csv"

# Bronze Delta paths
BRONZE_CHARTS           = str(BRONZE / "charts")
BRONZE_TRACK_FEATURES   = str(BRONZE / "track_features")
BRONZE_COUNTRIES        = str(BRONZE / "countries")

# Silver Delta paths
SILVER_CHARTS_CLEANED   = str(SILVER / "charts_cleaned")
SILVER_TRACKS_CLEANED   = str(SILVER / "tracks_cleaned")
SILVER_COUNTRIES        = str(SILVER / "countries")
SILVER_CHARTS_ENRICHED  = str(SILVER / "charts_enriched")

# Gold Delta paths
GOLD_REGIONAL_MOOD      = str(GOLD / "regional_mood_seasonal")
GOLD_STREAK_ANALYSIS    = str(GOLD / "streak_analysis")
GOLD_SONIC_DNA          = str(GOLD / "sonic_dna_by_rank_tier")
GOLD_TRACK_LONGEVITY    = str(GOLD / "track_longevity")
GOLD_TOP_ARTISTS        = str(GOLD / "global_top_artists")

# ---------------------------------------------------------------------------
# Spark tuning — sized for ~16 GB RAM local; scale shuffle partitions in
# cluster mode via SPARK_SHUFFLE_PARTITIONS env var
# ---------------------------------------------------------------------------
SHUFFLE_PARTITIONS  = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))
DRIVER_MEMORY       = os.getenv("SPARK_DRIVER_MEMORY", "8g")
EXECUTOR_MEMORY     = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
SPARK_MASTER        = os.getenv("SPARK_MASTER_URL", "local[*]")
SPARK_LOCAL_DIR     = os.getenv("SPARK_LOCAL_DIR", str(DATA_ROOT / "spark-tmp"))

# ---------------------------------------------------------------------------
# Feature flags — toggle individual pipeline stages for faster iteration
# ---------------------------------------------------------------------------
RUN_BRONZE  = os.getenv("RUN_BRONZE", "true").lower() == "true"
RUN_SILVER  = os.getenv("RUN_SILVER", "true").lower() == "true"
RUN_GOLD    = os.getenv("RUN_GOLD",   "true").lower() == "true"
