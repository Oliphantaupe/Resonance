"""Shared pytest fixtures.

SparkSession is created once per test session (session-scoped) to avoid
the ~30s startup overhead on every test file.
"""
import os
import sys
import pytest
from pyspark.sql import SparkSession

# On Windows the bare `python` command resolves to the Microsoft Store stub
# which cannot serve as a Spark Python worker. Point Spark at the real
# interpreter that is running this test process.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """Minimal local SparkSession for unit testing — no Delta, no packages."""
    session = (
        SparkSession.builder
        .appName("bigdata-music-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
