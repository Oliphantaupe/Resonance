"""Silver cleaning — countries.

Handles the three deliberate quirks injected into countries.csv:
  1. UTF-8 BOM — stripped by Spark's CSV reader automatically
  2. Mixed quote styles ("United States" quoted, others not) — handled by quoteChar='"'
  3. Leading whitespace in continent field (' Europe') — trimmed with trim()

# RAPPORT: these quirks were deliberately injected to demonstrate that even
# a 63-row reference table needs cleaning. The same principles (BOM, mixed
# quoting, whitespace) appear in real-world enterprise data pipelines.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim

from bigdata_music import config
from bigdata_music.utils.delta_io import read_delta
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Strip leading/trailing whitespace from all string columns."""
    from pyspark.sql.types import StringType
    str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in str_cols:
        df = df.withColumn(c, trim(col(c)))
    return df


def build_silver_countries(spark: SparkSession) -> int:
    """Clean Bronze countries → silver/countries (Delta, no partition).

    Returns the row count written.
    """
    log.info("Silver: loading bronze countries from %s", config.BRONZE_COUNTRIES)
    df = read_delta(spark, config.BRONZE_COUNTRIES)

    before = df.count()
    df = trim_string_columns(df)
    df = df.filter(col("region_code").isNotNull())
    after = df.count()

    log.info("Silver countries: %d rows (dropped %d nulls)", after, before - after)

    # Drop ingestion_ts — would clash with charts.ingestion_ts in the enriched join
    df = df.drop("ingestion_ts")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(config.SILVER_COUNTRIES)
    )

    log.info("Silver countries: write complete")
    return after
