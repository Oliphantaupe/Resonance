"""Performance measurement utilities for AA4.

Usage:
    with measure(spark, "charts x features join"):
        df_enriched = charts.join(features, ...)
        df_enriched.write...

Each block writes a JSON line to reports/perf_metrics.jsonl.
Compare runs with AQE on/off and with/without repartition to get the
before/after numbers the rapport needs.
"""
import json
import time
from contextlib import contextmanager
from pathlib import Path

from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)

METRICS_FILE = Path(__file__).resolve().parents[4] / "reports" / "perf_metrics.jsonl"


def _capture_spark_metrics(spark) -> dict:
    """Read accumulated shuffle / stage metrics from the Spark status API."""
    sc = spark.sparkContext
    status = sc.statusTracker()
    return {
        "active_jobs":   len(status.getActiveJobIds()),
        "active_stages": len(status.getActiveStageIds()),
    }


@contextmanager
def measure(spark, label: str):
    """Context manager: capture wall-clock time around a Spark action block.

    Appends a JSON record to reports/perf_metrics.jsonl so multiple runs
    can be compared in the notebook (99_perf_analysis.ipynb).

    Example record:
      {"label": "...", "aqe": true, "wall_sec": 42.3, ...}
    """
    aqe_enabled = (
        spark.conf.get("spark.sql.adaptive.enabled", "false").lower() == "true"
    )

    log.info("[measure] START  label=%r  AQE=%s", label, aqe_enabled)
    t0 = time.perf_counter()

    yield  # run the user's code block

    elapsed = time.perf_counter() - t0
    log.info("[measure] END    label=%r  wall=%.1fs", label, elapsed)

    record = {
        "label":      label,
        "aqe":        aqe_enabled,
        "wall_sec":   round(elapsed, 2),
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions", "?"),
    }

    METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with METRICS_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
