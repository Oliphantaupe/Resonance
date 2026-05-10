"""Silver cleaning — charts_cleaned.

Each function is a named, testable transformation. The pipeline applies
them in sequence; metrics are logged between steps so the rapport can
quantify exactly how many rows each operation affected.
"""
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce, col, count, current_timestamp,
    regexp_extract, row_number, to_date, when, year, month,
)
from pyspark.sql.window import Window

from bigdata_music import config
from bigdata_music.utils.delta_io import read_delta
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Individual cleaning transformations
# ---------------------------------------------------------------------------

def extract_track_id(df: DataFrame) -> DataFrame:
    """Spotify track_id is the last path segment of the url column.

    Returns null when url is null or the path does not match the expected
    pattern — the downstream filter_corrupt_rows will drop these rows.
    """
    return (
        df
        .withColumn(
            "track_id",
            regexp_extract(col("url"), r"track/([A-Za-z0-9]+)$", 1),
        )
        .withColumn(
            "track_id",
            when(col("track_id") == "", None).otherwise(col("track_id")),
        )
    )


def cast_date_with_fallback(df: DataFrame) -> DataFrame:
    """Most rows are ISO yyyy-MM-dd. A small fraction uses other formats.

    coalesce() tries each format in order; rows matching none become null
    and are caught by filter_corrupt_rows.

    # RAPPORT: this is the kind of multi-format datetime reality that
    # inferSchema=true would silently mishandle on edge cases.
    """
    return df.withColumn(
        "chart_date",
        coalesce(
            to_date(col("date"), "yyyy-MM-dd"),
            to_date(col("date"), "MM/dd/yyyy"),
            to_date(col("date"), "dd-MM-yyyy"),
        ),
    )


def cast_numeric_columns(df: DataFrame) -> DataFrame:
    """Cast rank (string) → int and streams (string) → long.

    streams is intentionally null for all viral50 rows; we preserve that
    rather than treating it as an error.
    """
    return (
        df
        .withColumn("rank",    col("rank").cast("int"))
        .withColumn("streams", col("streams").cast("long"))
    )


def filter_corrupt_rows(df: DataFrame) -> DataFrame:
    """Drop rows with null track_id, null chart_date, or null rank.

    Logs count per category for the rapport (demonstrates awareness of
    data quality without assuming data is clean).
    """
    # Single-pass aggregation replaces 4 separate full-table scans
    stats = df.agg(
        count("*").alias("total"),
        count(when(col("track_id").isNull(),   1)).alias("null_track_id"),
        count(when(col("chart_date").isNull(), 1)).alias("null_date"),
        count(when(col("rank").isNull(),       1)).alias("null_rank"),
    ).first()
    before        = stats["total"]
    null_track_id = stats["null_track_id"]
    null_date     = stats["null_date"]
    null_rank     = stats["null_rank"]

    log.info(
        "filter_corrupt_rows: null track_id=%d  null chart_date=%d  null rank=%d  (total before=%d)",
        null_track_id, null_date, null_rank, before,
    )

    clean = df.filter(
        col("track_id").isNotNull()
        & col("chart_date").isNotNull()
        & col("rank").isNotNull()
    )

    after = before - null_track_id  # approximate (rows with multiple nulls counted once)
    log.info("filter_corrupt_rows: dropped %d rows (%.2f%%)", before - after, (before - after) / before * 100)
    return clean


def deduplicate_chart_entries(df: DataFrame) -> DataFrame:
    """Same (date, region, rank) can appear from re-scrapes.

    Keep the row with the latest ingestion_ts per (chart_date, region, rank)
    using row_number() over a window — the classic deduplication pattern.

    # RAPPORT: row_number() over a window is the correct tool here because
    # we have a deterministic tie-breaker (ingestion_ts). A simple distinct()
    # would not let us choose *which* duplicate to keep.
    """
    w = (
        Window
        .partitionBy("chart_date", "region", "rank")
        .orderBy(col("ingestion_ts").desc())
    )
    return (
        df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )


def add_time_columns(df: DataFrame) -> DataFrame:
    """Derive year and month from chart_date for partition alignment."""
    return (
        df
        .withColumn("year",  year(col("chart_date")))
        .withColumn("month", month(col("chart_date")))
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def build_silver_charts(spark: SparkSession) -> int:
    """Apply all cleaning steps Bronze → silver/charts_cleaned (Delta).

    Returns the row count written.
    """
    log.info("Silver: loading bronze charts from %s", config.BRONZE_CHARTS)
    df = read_delta(spark, config.BRONZE_CHARTS)

    df = extract_track_id(df)
    df = cast_date_with_fallback(df)
    df = cast_numeric_columns(df)
    df = filter_corrupt_rows(df)
    df = deduplicate_chart_entries(df)
    df = add_time_columns(df)

    # Drop raw columns superseded by cleaned ones
    df = df.drop("date", "_corrupt_record")

    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    row_count = df.count()
    log.info("Silver charts: writing %d rows partitioned by (region, year) to %s",
             row_count, config.SILVER_CHARTS_CLEANED)

    (
        df.write
        .format("delta")
        # query-aligned: most Gold + dashboard queries filter by region first
        .partitionBy("region", "year")
        .mode("overwrite")
        .save(config.SILVER_CHARTS_CLEANED)
    )

    df.unpersist()
    log.info("Silver charts: write complete")
    return row_count
