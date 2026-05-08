"""Bronze ingestion — Spotify Track Features Parquet → Delta.

Written without partitioning: Bronze is a raw landing zone; sorting 56M
wide rows for partitionBy would OOM on a local 8g driver. Decade bucketing
is added at Silver after type-safe cleaning.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from bigdata_music import config
from bigdata_music.schemas import TRACK_FEATURES_RAW_SCHEMA
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def ingest_features(spark: SparkSession) -> int:
    """Read track_features.parquet → bronze/track_features (Delta, partitioned by release_decade).

    Returns the row count written.
    """
    log.info("Bronze: reading %s", config.TRACK_FEATURES_RAW)

    df_raw = (
        spark.read
        .schema(TRACK_FEATURES_RAW_SCHEMA)
        .parquet(str(config.TRACK_FEATURES_RAW))
    )

    df_bronze = df_raw.withColumn("ingestion_ts", current_timestamp())

    log.info("Bronze features: writing to %s", config.BRONZE_TRACK_FEATURES)

    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .save(config.BRONZE_TRACK_FEATURES)
    )

    from bigdata_music.utils.delta_io import read_delta
    row_count = read_delta(spark, config.BRONZE_TRACK_FEATURES).count()
    log.info("Bronze features: write complete — %d rows", row_count)
    return row_count
