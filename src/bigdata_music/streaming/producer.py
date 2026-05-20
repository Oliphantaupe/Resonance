"""Kafka producer — replays charts.csv as a simulated daily chart feed.

Each day's chart rows are serialised as JSON and sent to the Kafka topic
`charts-feed` as a micro-batch. The consumer picks them up via Spark
Structured Streaming.

Design choices:
- Grouped by date (ascending) so the consumer sees records in chronological
  order, matching the real-world scenario of a daily chart update feed.
- Configurable inter-batch delay (default 0.5 s) so a demo run completes
  in minutes rather than days.
- kafka-python is used (lightweight, no JVM dependency) because the producer
  does not need Spark — it just serialises rows and publishes them.
"""
import json
import time
from pathlib import Path
from typing import Optional

from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def _wait_for_kafka(bootstrap: str, max_retries: int = 20, wait_sec: float = 5.0) -> None:
    """Block until the Kafka broker is reachable or max_retries is exceeded."""
    from kafka import KafkaProducer as _KP
    from kafka.errors import NoBrokersAvailable

    for attempt in range(1, max_retries + 1):
        try:
            p = _KP(bootstrap_servers=bootstrap, api_version_auto_timeout_ms=3000,
                    request_timeout_ms=5000)
            p.close()
            log.info("Producer: Kafka is ready")
            return
        except (NoBrokersAvailable, Exception):
            log.info("Producer: Kafka not ready yet (%d/%d), retrying in %.0fs …",
                     attempt, max_retries, wait_sec)
            time.sleep(wait_sec)
    raise RuntimeError(f"Kafka at {bootstrap} not reachable after {max_retries} retries")


def run_producer(
    bootstrap: str,
    topic: str,
    csv_path: Path,
    batch_delay: float = 0.5,
    limit_days: Optional[int] = None,
) -> None:
    """Stream charts.csv date-by-date into a Kafka topic.

    Args:
        bootstrap:   Kafka broker address (e.g. "kafka:29092").
        topic:       Kafka topic name (e.g. "charts-feed").
        csv_path:    Path to the raw charts.csv file.
        batch_delay: Seconds to sleep between date batches.
        limit_days:  Stop after this many dates (None = stream all).
    """
    import pandas as pd
    from kafka import KafkaProducer

    log.info("Producer: reading %s", csv_path)
    df = pd.read_csv(
        csv_path,
        dtype=str,           # keep all columns as strings — Bronze schema handles casts
        keep_default_na=False,
    )

    if "date" not in df.columns:
        raise ValueError("charts.csv must contain a 'date' column")

    dates = sorted(df["date"].unique())
    if limit_days is not None:
        dates = dates[:limit_days]

    log.info("Producer: waiting for Kafka at %s …", bootstrap)
    _wait_for_kafka(bootstrap)

    log.info("Producer: connecting to Kafka at %s, topic=%r", bootstrap, topic)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        batch_size=16384,
        linger_ms=10,
    )

    t_start = time.perf_counter()
    cumulative = 0

    for i, date in enumerate(dates):
        batch = df[df["date"] == date].to_dict(orient="records")
        for row in batch:
            producer.send(topic, value=row)
        producer.flush()

        cumulative += len(batch)
        elapsed = time.perf_counter() - t_start
        log.info(
            "Producer: sent date=%s  batch_rows=%d  cumulative=%d  elapsed=%.1fs",
            date, len(batch), cumulative, elapsed,
        )

        if i < len(dates) - 1:
            time.sleep(batch_delay)

    producer.close()
    log.info(
        "Producer: finished — %d dates, %d total rows in %.1fs",
        len(dates), cumulative, time.perf_counter() - t_start,
    )
