"""foreachBatch handler for the Spark Structured Streaming consumer.

Called once per micro-batch with (batch_df, batch_id). Responsible for:
  1. Appending raw records to bronze/charts_streaming (Delta).
  2. Applying silver cleaning transforms (reuses transform_charts from silver layer).
  3. MERGE-ing cleaned records into silver/charts_streaming (dedup by primary key).
  4. Recomputing the streaming gold aggregate (top artists by stream count).
  5. Logging per-batch metrics (latency, throughput, cumulative rows).

Why foreachBatch instead of a native Delta sink?
  Delta's native streaming sink writes only to one table per query. foreachBatch
  lets us fan out to Bronze → Silver → Gold in a single micro-batch with full
  ACID semantics on each write, and gives us precise wall-clock measurement for
  the streaming metrics (AA4.3).
"""
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, current_timestamp, sum as _sum, trunc

from bigdata_music import config
from bigdata_music.silver.clean_charts import transform_charts
from bigdata_music.streaming.streaming_metrics import log_batch_metrics
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)

# Module-level SparkSession reference set by consumer.py before streaming starts
_spark: SparkSession = None


def set_spark(spark: SparkSession) -> None:
    global _spark
    _spark = spark


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Handle one Spark Structured Streaming micro-batch end-to-end."""
    t0 = time.perf_counter()

    n_rows = batch_df.count()
    if n_rows == 0:
        log.info("[batch %d] empty — skipping", batch_id)
        return

    log.info("[batch %d] received %d rows", batch_id, n_rows)

    # Stamp ingestion time — required by deduplicate_chart_entries (tie-breaker)
    raw = batch_df.drop("kafka_timestamp").withColumn("ingestion_ts", current_timestamp())

    # 1 ── Bronze append ────────────────────────────────────────────────────
    (
        raw.write
        .format("delta")
        .mode("append")
        .save(config.BRONZE_CHARTS_STREAMING)
    )
    log.info("[batch %d] bronze append done", batch_id)

    # 2 ── Silver cleaning ──────────────────────────────────────────────────
    # transform_charts is the same pure-function chain used by the batch
    # pipeline — no duplication of cleaning logic.
    try:
        cleaned = transform_charts(raw)
    except Exception as exc:
        log.warning("[batch %d] silver transform failed: %s", batch_id, exc)
        cleaned = None

    # 3 ── Silver MERGE (dedup on track_id + chart_date + region) ──────────
    if cleaned is not None and cleaned.count() > 0:
        _merge_silver(cleaned)
        log.info("[batch %d] silver merge done", batch_id)

        # 4 ── Gold: top artists from streaming silver ──────────────────────
        _update_gold_top_artists()
        _update_gold_recent_submissions()
        log.info("[batch %d] gold update done", batch_id)

    # 5 ── Metrics ──────────────────────────────────────────────────────────
    processing_ms = (time.perf_counter() - t0) * 1000
    bronze_total = _count_bronze_total()
    log_batch_metrics(batch_id, n_rows, processing_ms, bronze_total)


# ── Internal helpers ───────────────────────────────────────────────────────

def _merge_silver(cleaned: DataFrame) -> None:
    """MERGE cleaned batch into silver/charts_streaming using Delta MERGE.

    Deduplicates on (track_id, chart_date, region) — the natural primary key
    of a chart entry. Records already present are skipped (whenNotMatchedInsert)
    so re-processing the same Kafka messages is idempotent.
    """
    import os
    from pathlib import Path

    from delta.tables import DeltaTable

    path = config.SILVER_CHARTS_STREAMING
    if DeltaTable.isDeltaTable(_spark, path):
        delta_tbl = DeltaTable.forPath(_spark, path)
        (
            delta_tbl.alias("existing")
            .merge(
                cleaned.alias("new"),
                "existing.track_id = new.track_id "
                "AND existing.chart_date = new.chart_date "
                "AND existing.region = new.region",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            cleaned.write
            .format("delta")
            .mode("overwrite")
            .save(path)
        )


def _update_gold_top_artists() -> None:
    """Recompute the streaming top-artists aggregate from silver/charts_streaming.

    Simpler than the batch version (no audio features, no rolling window) to
    keep micro-batch latency low. The dashboard Live section shows this table.
    """
    import os
    from pathlib import Path

    path = config.SILVER_CHARTS_STREAMING
    if not Path(path).exists():
        return

    silver = _spark.read.format("delta").load(path)

    top_artists = (
        silver
        .filter(col("streams").isNotNull())
        .withColumn("month_start", trunc(col("chart_date"), "month"))
        .groupBy("artist", "month_start")
        .agg(
            _sum("streams").alias("monthly_streams"),
            count("*").alias("chart_entries"),
        )
        .orderBy(col("monthly_streams").desc())
    )

    (
        top_artists.write
        .format("delta")
        .mode("overwrite")
        .save(config.GOLD_TOP_ARTISTS_STREAMING)
    )


def _update_gold_recent_submissions() -> None:
    """Write the 100 most recently ingested silver rows to gold/recent_submissions.

    The Dash Submit page reads this table to show a live feed of what was recently
    processed, refreshed every 15 s. Keeping it in Gold means no extra mount needed
    in the dash container.
    """
    from pathlib import Path

    path = config.SILVER_CHARTS_STREAMING
    if not Path(path).exists():
        return

    recent = (
        _spark.read.format("delta").load(path)
        .orderBy(col("ingestion_ts").desc())
        .limit(100)
        .select("ingestion_ts", "artist", "title", "region", "chart",
                "rank", "streams", "chart_date")
    )
    (
        recent.write
        .format("delta")
        .mode("overwrite")
        .save(config.GOLD_RECENT_SUBMISSIONS)
    )


def _count_bronze_total() -> int:
    """Return current row count in bronze/charts_streaming (0 if not yet written)."""
    import os
    from pathlib import Path
    try:
        if Path(config.BRONZE_CHARTS_STREAMING).exists():
            return _spark.read.format("delta").load(config.BRONZE_CHARTS_STREAMING).count()
    except Exception:
        pass
    return 0
