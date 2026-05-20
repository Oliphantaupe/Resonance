"""Per-batch streaming metrics logger — mirrors utils/metrics.py for the streaming pipeline.

Each micro-batch appends one JSON line to reports/streaming_metrics.jsonl.
The notebook 03_streaming_demo.ipynb reads this file to plot throughput and
latency over time, satisfying AA4.3 (streaming-specific metrics).
"""
import json
import os
from datetime import datetime
from pathlib import Path

from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)

_default_reports = Path(__file__).resolve().parents[3] / "reports"
STREAMING_METRICS_FILE = Path(
    os.getenv("REPORTS_DIR", str(_default_reports))
) / "streaming_metrics.jsonl"


def log_batch_metrics(
    batch_id: int,
    rows: int,
    processing_ms: float,
    bronze_total_rows: int = 0,
) -> None:
    """Append one metrics record for a completed micro-batch.

    Args:
        batch_id:          Spark batch identifier (monotonically increasing).
        rows:              Number of rows processed in this batch.
        processing_ms:     Wall-clock time for the foreachBatch handler (ms).
        bronze_total_rows: Cumulative row count in bronze/charts_streaming.
    """
    throughput = (rows / processing_ms * 1000) if processing_ms > 0 else 0.0
    record = {
        "batch_id":             batch_id,
        "rows":                 rows,
        "processing_ms":        round(processing_ms, 1),
        "throughput_rows_per_sec": round(throughput, 1),
        "bronze_total_rows":    bronze_total_rows,
        "ts":                   datetime.utcnow().isoformat(timespec="seconds"),
    }

    STREAMING_METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STREAMING_METRICS_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    log.info(
        "[streaming] batch=%d  rows=%d  latency=%.0fms  throughput=%.0f rows/s",
        batch_id, rows, processing_ms, throughput,
    )

    _mlflow_log(batch_id, rows, processing_ms, throughput)


def _mlflow_log(batch_id: int, rows: int, processing_ms: float, throughput: float) -> None:
    try:
        import mlflow
        if mlflow.active_run():
            mlflow.log_metrics({
                "streaming_batch_rows":           float(rows),
                "streaming_processing_ms":        round(processing_ms, 1),
                "streaming_throughput_rows_per_s": round(throughput, 1),
            }, step=batch_id)
    except Exception:
        pass
