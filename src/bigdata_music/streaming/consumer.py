"""Spark Structured Streaming consumer — Kafka → Bronze → Silver → Gold (Delta).

Reads from the `charts-feed` Kafka topic, parses JSON messages into the raw
charts schema, then delegates each micro-batch to batch_handler.process_batch
which handles Bronze append, Silver MERGE, and Gold recompute.

Run via:
    python scripts/run_consumer.py
or inside Docker:
    docker compose --profile streaming up kafka-consumer
"""
import os

from pyspark.sql.functions import col, from_json

from bigdata_music import config
from bigdata_music.schemas import CHARTS_RAW_SCHEMA
from bigdata_music.spark_session import get_spark_streaming
from bigdata_music.streaming import batch_handler
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def start_consumer() -> None:
    """Start the streaming query and block until termination."""
    log.info("Consumer: starting SparkSession with Kafka package")
    spark = get_spark_streaming()

    # Share SparkSession with the batch handler (needed for Delta reads/writes)
    batch_handler.set_spark(spark)

    log.info("Consumer: connecting to Kafka %s, topic=%r", config.KAFKA_BOOTSTRAP, config.KAFKA_TOPIC)
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        # Limit messages per trigger to keep micro-batch size manageable in demo
        .option("maxOffsetsPerTrigger", "5000")
        .load()
    )

    # Parse JSON value column → struct using the raw charts schema
    parsed = (
        raw_stream
        .select(
            from_json(col("value").cast("string"), CHARTS_RAW_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
        # Drop rows where JSON parsing failed entirely (value does not match schema)
        .filter(col("title").isNotNull() | col("artist").isNotNull())
    )

    log.info(
        "Consumer: starting streaming query — checkpoint=%s  trigger=10s",
        config.CHECKPOINT_CHARTS_STREAMING,
    )
    query = (
        parsed.writeStream
        .trigger(processingTime="10 seconds")
        .foreachBatch(batch_handler.process_batch)
        .option("checkpointLocation", config.CHECKPOINT_CHARTS_STREAMING)
        .start()
    )

    log.info("Consumer: streaming query active — awaiting termination")
    query.awaitTermination()
