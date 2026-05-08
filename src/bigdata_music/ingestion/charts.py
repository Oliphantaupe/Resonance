"""Bronze ingestion — Spotify Charts CSV → Delta.

Partition by year only (not region). Reasoning: Bronze stores raw data
as-is; a (region × year) partition would create ~350 small files. Year
alone gives 5 partitions of ~700 MB each, the Spark sweet spot here.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, year, to_date

from bigdata_music import config
from bigdata_music.schemas import CHARTS_RAW_SCHEMA, CORRUPT_RECORD_FIELD
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def ingest_charts(spark: SparkSession) -> int:
    """Read charts.csv → bronze/charts (Delta, partitioned by year).

    Returns the row count written.
    """
    log.info("Bronze: reading %s", config.CHARTS_RAW)

    schema_with_corrupt = CHARTS_RAW_SCHEMA.add(CORRUPT_RECORD_FIELD)

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("encoding", "UTF-8")
        # multiLine handles titles with embedded newlines from scraper artifacts
        .option("multiLine", "false")
        .schema(schema_with_corrupt)
        .csv(str(config.CHARTS_RAW))
    )

    corrupt_count = df_raw.filter(df_raw["_corrupt_record"].isNotNull()).count()
    if corrupt_count > 0:
        log.warning("Bronze charts: %d corrupt records captured in _corrupt_record", corrupt_count)

    df_bronze = (
        df_raw
        .withColumn("ingestion_ts", current_timestamp())
        # year column is only for partitioning — we re-derive it cleanly at Silver
        .withColumn("year", year(to_date("date", "yyyy-MM-dd")))
    )

    row_count = df_bronze.count()
    log.info("Bronze charts: writing %d rows to %s", row_count, config.BRONZE_CHARTS)

    (
        df_bronze.write
        .format("delta")
        .partitionBy("year")
        .mode("overwrite")
        .save(config.BRONZE_CHARTS)
    )

    log.info("Bronze charts: write complete")
    return row_count
