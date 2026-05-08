"""Bronze ingestion — Country Enrichment CSV → Delta.

No partitioning: the table has ~63 rows and will always be broadcast-joined.
Writing as Delta is a deliberate choice (consistency + time-travel), not
because this table needs Delta's features at this size.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from bigdata_music import config
from bigdata_music.schemas import COUNTRIES_RAW_SCHEMA
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def ingest_countries(spark: SparkSession) -> int:
    """Read countries.csv → bronze/countries (Delta, no partition).

    The file has a deliberate UTF-8 BOM; Spark's CSV reader handles this
    automatically when encoding=UTF-8 because it strips the BOM on read.

    Returns the row count written.
    """
    log.info("Bronze: reading %s", config.COUNTRIES_RAW)

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        # BOM is stripped automatically; mixed quotes handled by default quoteChar='"'
        .schema(COUNTRIES_RAW_SCHEMA)
        .csv(str(config.COUNTRIES_RAW))
    )

    df_bronze = df_raw.withColumn("ingestion_ts", current_timestamp())

    row_count = df_bronze.count()
    log.info("Bronze countries: writing %d rows to %s", row_count, config.BRONZE_COUNTRIES)

    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .save(config.BRONZE_COUNTRIES)
    )

    log.info("Bronze countries: write complete")
    return row_count
