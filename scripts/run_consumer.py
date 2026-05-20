"""Entry point: Spark Structured Streaming consumer (Kafka → Delta).

Usage:
    python scripts/run_consumer.py

The consumer blocks until interrupted. On first start it downloads
spark-sql-kafka JARs (~50 MB) via Ivy — subsequent starts use the cache.

Environment variables (all have defaults in config.py):
    KAFKA_BOOTSTRAP_SERVERS   Kafka broker address
    DATA_ROOT                 Root path for Delta tables and checkpoints
    MLFLOW_TRACKING_URI       MLflow server for metric logging
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bigdata_music.streaming.consumer import start_consumer
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)

if __name__ == "__main__":
    log.info("run_consumer: starting Spark Structured Streaming consumer")
    start_consumer()
