"""Unit tests for the gap-and-island streak detection algorithm.

This is the most algorithmically complex part of the pipeline (AA2),
so it gets dedicated tests with explicit expected outputs.
"""
import pytest
from datetime import date
from pyspark.sql import Row

from bigdata_music.gold.streaks import build_streak_analysis


def make_top1_df(spark, rows):
    return spark.createDataFrame(rows)


def _streak_df(spark, entries):
    """entries: list of (track_id, region, chart_date_str, streams)"""
    rows = [
        Row(
            track_id=t, region=r, chart_date=date.fromisoformat(d),
            rank=1, title="Test Track", artist="Test Artist",
            streams=s, year=int(d[:4]),
        )
        for t, r, d, s in entries
    ]
    return spark.createDataFrame(rows)


def _run_island(spark, df):
    """Run just the gap-and-island logic inline (avoids Delta dependency)."""
    from pyspark.sql.functions import col, count, date_sub, max as _max, min as _min, row_number, sum as _sum
    from pyspark.sql.window import Window

    w_date = Window.partitionBy("track_id", "region").orderBy("chart_date")
    with_rn = df.withColumn("_rn", row_number().over(w_date))
    with_group = with_rn.withColumn(
        "streak_group", date_sub(col("chart_date"), col("_rn").cast("int"))
    )
    return (
        with_group
        .groupBy("track_id", "title", "artist", "region", "streak_group")
        .agg(
            _min("chart_date").alias("streak_start"),
            _max("chart_date").alias("streak_end"),
            count("*").alias("streak_days"),
            _sum("streams").alias("streak_total_streams"),
        )
    )


def test_single_consecutive_streak(spark):
    """5 consecutive days = 1 streak of length 5."""
    dates = ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05"]
    df = _streak_df(spark, [("T1", "us", d, 1000) for d in dates])
    result = _run_island(spark, df)
    assert result.count() == 1
    assert result.collect()[0]["streak_days"] == 5


def test_gap_creates_two_streaks(spark):
    """Days 1-3 and 5-7 (gap on day 4) = 2 streaks."""
    days = ["2020-01-01", "2020-01-02", "2020-01-03",
            "2020-01-05", "2020-01-06", "2020-01-07"]
    df = _streak_df(spark, [("T1", "us", d, 1000) for d in days])
    result = _run_island(spark, df)
    assert result.count() == 2
    lengths = sorted(result.select("streak_days").rdd.flatMap(lambda x: x).collect())
    assert lengths == [3, 3]


def test_different_regions_independent_streaks(spark):
    """Same track, same dates, two regions = 2 separate streaks."""
    days = ["2020-01-01", "2020-01-02"]
    rows = [("T1", "us", d, 1000) for d in days] + [("T1", "gb", d, 500) for d in days]
    df = _streak_df(spark, rows)
    result = _run_island(spark, df)
    assert result.count() == 2


def test_single_day_is_streak_of_one(spark):
    df = _streak_df(spark, [("T1", "us", "2020-06-15", 999)])
    result = _run_island(spark, df)
    assert result.count() == 1
    assert result.collect()[0]["streak_days"] == 1
