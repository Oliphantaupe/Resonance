"""Entry point: Kafka producer that replays charts.csv as a daily chart feed.

Usage:
    python scripts/run_producer.py [options]

Options:
    --bootstrap  Kafka broker (default: value of KAFKA_BOOTSTRAP_SERVERS env,
                 fallback "kafka:29092")
    --topic      Kafka topic (default: "charts-feed")
    --delay      Seconds between date batches (default: 0.5)
    --days       Number of dates to send (default: all ~1800 dates in dataset)
    --csv        Path to charts.csv (default: DATA_ROOT/raw/charts.csv)
"""
import argparse
import sys
from pathlib import Path

# Ensure src/ is importable when run directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bigdata_music import config
from bigdata_music.streaming.producer import run_producer
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka producer for Spotify charts stream")
    parser.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP,
                        help="Kafka bootstrap server")
    parser.add_argument("--topic",     default=config.KAFKA_TOPIC,
                        help="Kafka topic name")
    parser.add_argument("--delay",     type=float, default=0.5,
                        help="Seconds between date batches")
    parser.add_argument("--days",      type=int,   default=None,
                        help="Limit to first N dates (for testing)")
    parser.add_argument("--csv",       default=str(config.CHARTS_RAW),
                        help="Path to charts.csv")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        log.error("charts.csv not found at %s — run download_data.py first", csv_path)
        sys.exit(1)

    log.info(
        "Starting producer: bootstrap=%s  topic=%s  delay=%.1fs  days=%s",
        args.bootstrap, args.topic, args.delay, args.days or "all",
    )
    run_producer(
        bootstrap=args.bootstrap,
        topic=args.topic,
        csv_path=csv_path,
        batch_delay=args.delay,
        limit_days=args.days,
    )


if __name__ == "__main__":
    main()
