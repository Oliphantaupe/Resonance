"""Unit tests for Silver chart cleaning functions.

Each function is tested in isolation with hand-crafted DataFrames
so we can assert exact row counts and column values without needing
the full 3.5 GB CSV on disk.
"""
import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col

from bigdata_music.silver.clean_charts import (
    cast_date_with_fallback,
    cast_numeric_columns,
    deduplicate_chart_entries,
    extract_track_id,
    filter_corrupt_rows,
)


def make_charts_df(spark, rows):
    return spark.createDataFrame(rows)


# --- extract_track_id ---

def test_extract_track_id_standard_url(spark):
    df = make_charts_df(spark, [
        Row(url="https://open.spotify.com/track/6mICuAdrwEjh6Y6lroV2Kg"),
    ])
    result = extract_track_id(df).collect()
    assert result[0]["track_id"] == "6mICuAdrwEjh6Y6lroV2Kg"


def test_extract_track_id_null_url(spark):
    from pyspark.sql.types import StringType, StructField, StructType
    schema = StructType([StructField("url", StringType(), True)])
    df = spark.createDataFrame([Row(url=None)], schema=schema)
    result = extract_track_id(df).collect()
    assert result[0]["track_id"] is None


def test_extract_track_id_malformed_url(spark):
    df = make_charts_df(spark, [Row(url="https://open.spotify.com/artist/abc")])
    result = extract_track_id(df).collect()
    assert result[0]["track_id"] is None


# --- cast_date_with_fallback ---

def test_cast_date_iso_format(spark):
    df = make_charts_df(spark, [Row(date="2020-06-15")])
    result = cast_date_with_fallback(df).collect()
    assert result[0]["chart_date"] is not None
    assert str(result[0]["chart_date"]) == "2020-06-15"


def test_cast_date_us_format(spark):
    df = make_charts_df(spark, [Row(date="06/15/2020")])
    result = cast_date_with_fallback(df).collect()
    assert result[0]["chart_date"] is not None


def test_cast_date_unparseable_becomes_null(spark):
    df = make_charts_df(spark, [Row(date="not-a-date")])
    result = cast_date_with_fallback(df).collect()
    assert result[0]["chart_date"] is None


# --- filter_corrupt_rows ---

def test_filter_corrupt_rows_drops_null_track_id(spark):
    from pyspark.sql.functions import lit
    df = make_charts_df(spark, [
        Row(track_id="abc123", chart_date="2020-01-01", rank=1),
        Row(track_id=None,     chart_date="2020-01-01", rank=2),
    ])
    df = df.withColumn("chart_date", col("chart_date").cast("date"))
    result = filter_corrupt_rows(df)
    assert result.count() == 1
    assert result.collect()[0]["track_id"] == "abc123"


# --- deduplicate_chart_entries ---

def test_deduplicate_keeps_latest_ingestion(spark):
    from datetime import datetime
    rows = [
        Row(chart_date="2020-01-01", region="us", rank=1,
            track_id="abc", ingestion_ts=datetime(2020, 1, 2)),
        Row(chart_date="2020-01-01", region="us", rank=1,
            track_id="abc", ingestion_ts=datetime(2020, 1, 3)),  # later → keep
    ]
    df = make_charts_df(spark, rows)
    df = df.withColumn("chart_date", col("chart_date").cast("date"))
    result = deduplicate_chart_entries(df)
    assert result.count() == 1
    assert result.collect()[0]["ingestion_ts"] == datetime(2020, 1, 3)


def test_deduplicate_distinct_rows_unchanged(spark):
    from datetime import datetime
    rows = [
        Row(chart_date="2020-01-01", region="us", rank=1,
            track_id="aaa", ingestion_ts=datetime(2020, 1, 2)),
        Row(chart_date="2020-01-01", region="us", rank=2,
            track_id="bbb", ingestion_ts=datetime(2020, 1, 2)),
    ]
    df = make_charts_df(spark, rows)
    df = df.withColumn("chart_date", col("chart_date").cast("date"))
    assert deduplicate_chart_entries(df).count() == 2
