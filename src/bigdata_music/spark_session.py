"""SparkSession factory — single source of truth for all Spark configuration.

Two variants:
  - get_spark()          : AQE enabled (default for pipeline runs)
  - get_spark_no_aqe()   : AQE disabled (used for AA4 before/after measurement)
"""
from pyspark.sql import SparkSession

from bigdata_music import config

_DELTA_EXTENSIONS  = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG     = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
_DELTA_PACKAGE     = "io.delta:delta-spark_2.12:3.2.0"


def _base_builder(app_name: str) -> SparkSession.Builder:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(config.SPARK_MASTER)
        .config("spark.jars.packages",              _DELTA_PACKAGE)
        .config("spark.sql.extensions",             _DELTA_EXTENSIONS)
        .config("spark.sql.catalog.spark_catalog",  _DELTA_CATALOG)
        .config("spark.sql.shuffle.partitions",     str(config.SHUFFLE_PARTITIONS))
        .config("spark.driver.memory",              config.DRIVER_MEMORY)
        .config("spark.executor.memory",            config.EXECUTOR_MEMORY)
        .config("spark.local.dir",                  config.SPARK_LOCAL_DIR)
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        # Vectorized reader disabled: track_features.parquet uses PlainIntegerDictionary
        # encoding that is unsupported by the vectorized reader in PySpark 3.5
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        # Reduce log noise in notebook output
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.maxResultSize",  "4g")
    )


def get_spark(app_name: str = "bigdata-music") -> SparkSession:
    """Standard session — AQE + skew-join protection enabled."""
    return (
        _base_builder(app_name)
        .config("spark.sql.adaptive.enabled",               "true")
        .config("spark.sql.adaptive.skewJoin.enabled",      "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def get_spark_no_aqe(app_name: str = "bigdata-music-no-aqe") -> SparkSession:
    """AQE-disabled session for AA4 performance comparison (Experiment 1)."""
    return (
        _base_builder(app_name)
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
