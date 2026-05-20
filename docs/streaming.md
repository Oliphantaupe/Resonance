# Streaming Architecture — Resonance

This document explains how the real-time data pipeline works end-to-end: from
a Kafka message produced by the dashboard form or the replay script, through
Spark Structured Streaming, into three layers of Delta Lake tables, and finally
onto the live dashboard.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Why These Technologies](#2-why-these-technologies)
3. [Data Flow](#3-data-flow)
4. [Message Format](#4-message-format)
5. [Spark Consumer — internals](#5-spark-consumer--internals)
6. [Silver Cleaning Pipeline](#6-silver-cleaning-pipeline)
7. [Gold Aggregates](#7-gold-aggregates)
8. [Checkpointing and Fault Tolerance](#8-checkpointing-and-fault-tolerance)
9. [Streaming Metrics](#9-streaming-metrics)
10. [Dashboard Integration](#10-dashboard-integration)
11. [Submit Page — single entry and bulk JSON](#11-submit-page--single-entry-and-bulk-json)
12. [Running the Stack](#12-running-the-stack)
13. [Observed Performance](#13-observed-performance)

---

## 1. Overview

```
charts.csv replay                 Dashboard form
(kafka-producer container)        (Submit page — /submit)
        │                                  │
        │  JSON messages, one per row      │  Single row or bulk array
        └──────────────┬───────────────────┘
                       ▼
             Kafka topic: charts-feed
                       │
                       ▼
        Spark Structured Streaming consumer
        (kafka-consumer container)
               │
               ├─ every 10 s: foreachBatch
               │
               ├─ 1. Bronze append     → data/bronze/charts_streaming   (Delta)
               ├─ 2. Silver MERGE      → data/silver/charts_streaming   (Delta)
               ├─ 3. Gold top-artists  → data/gold/top_artists_streaming (Delta)
               ├─ 4. Gold recent rows  → data/gold/recent_submissions    (Delta)
               └─ 5. Metrics log       → reports/streaming_metrics.jsonl
                       │
                       ▼
              Plotly Dash dashboard
              ├─ /artists   — LIVE section, 30 s auto-refresh
              └─ /submit    — Recently Processed table, 15 s auto-refresh
```

A row submitted through the form reaches the "Recently Processed" table in
**~40 seconds**: 10 s for the next Spark trigger + ~30 s of processing across
the three layers.

---

## 2. Why These Technologies

### Kafka

Kafka decouples message *production* from *consumption*. The producer (whether
the replay script or the dashboard form) can publish messages at any rate
without the consumer needing to be ready at the same instant. Kafka retains
messages on disk, so the consumer can replay from the beginning (`startingOffsets=earliest`)
after a restart without data loss. For a chart feed — daily batches arriving at
irregular intervals — this replay semantic is a natural fit.

Alternative considered: direct HTTP push to a Spark socket source. Rejected
because it offers no persistence, no replay, and no backpressure.

### Spark Structured Streaming

Spark Structured Streaming treats a stream as an unbounded table and applies
the same DataFrame API used in the batch pipeline. This lets us reuse the
existing Silver cleaning functions (`transform_charts`) without any duplication
of logic between batch and streaming paths.

The `foreachBatch` sink is used instead of a native Delta streaming sink because
we need to fan out to **three separate tables** (Bronze, Silver, Gold) inside a
single micro-batch with ACID semantics on each write. A native streaming sink
can only target one table per query.

### Delta Lake

Delta Lake provides:
- **ACID transactions** — the MERGE operation that deduplicates Silver is fully
  atomic; a crashed micro-batch leaves no partial writes.
- **Exactly-once semantics** — the MERGE condition `(track_id, chart_date, region)`
  means re-processing the same Kafka messages (e.g. after a consumer restart)
  produces the same result; duplicates are silently skipped.
- **Time travel** — the dashboard's `load_gold` function can query any historical
  version of a Gold table using the `version=` parameter of `DeltaTable`.
- **Schema enforcement** — the explicit `CHARTS_RAW_SCHEMA` prevents Spark from
  silently mistyping fields like `rank="N/A"` or `streams=""`.

---

## 3. Data Flow

### Stage 0 — Production

**Replay producer** (`scripts/run_producer.py`):
- Reads `data/raw/charts.csv` with pandas, grouped by `date` column ascending.
- Sends each day's rows as individual JSON messages to `charts-feed`.
- Default delay between date batches: 0.3 s (configurable via `--delay`).
- 30-day demo run: ~352,000 rows, ~43 s elapsed.

**Dashboard form** (`/submit`):
- Single entry: one JSON message published on form submit.
- Bulk JSON: array of objects, validated field-by-field, then each published as
  a separate Kafka message in a tight loop.
- Both paths generate a deterministic fake Spotify URL:
  `https://open.spotify.com/track/<md5(artist:title)[:22]>`
  so that the Silver `regexp_extract` step can derive a valid `track_id`.

### Stage 1 — Kafka

Topic: `charts-feed`, single partition, replication factor 1.

The consumer reads with `maxOffsetsPerTrigger=5000` — this caps each
micro-batch at 5,000 messages regardless of how many are in the queue. Without
this cap, a large backlog (e.g. after the replay producer sends 350,000 rows
before the consumer starts) would flood a single micro-batch and cause OOM or
multi-minute latency spikes.

### Stage 2 — Bronze (raw append)

The raw batch DataFrame (with all fields as strings, matching `CHARTS_RAW_SCHEMA`)
is appended to `data/bronze/charts_streaming` as a Delta table. No cleaning
is applied. This layer is the permanent record of every message received, in
arrival order.

```
bronze/charts_streaming columns
  title      STRING
  rank       STRING   ← not yet cast
  date       STRING   ← not yet cast
  artist     STRING
  url        STRING
  region     STRING
  chart      STRING
  trend      STRING
  streams    STRING   ← null for viral50
  ingestion_ts TIMESTAMP   ← stamped by the foreachBatch handler
```

### Stage 3 — Silver (cleaning + MERGE)

The same `transform_charts(df)` function used by the batch pipeline is called
on the raw batch. It applies six transformations in sequence (see
[Section 6](#6-silver-cleaning-pipeline)).

The result is MERGEd into `data/silver/charts_streaming` using Delta MERGE:

```
MERGE INTO silver USING cleaned_batch
  ON silver.track_id  = batch.track_id
 AND silver.chart_date = batch.chart_date
 AND silver.region    = batch.region
WHEN NOT MATCHED THEN INSERT *
```

`WHEN NOT MATCHED` only — records already present (same primary key) are
silently skipped. This makes the Silver layer idempotent: replaying the same
Kafka messages after a consumer restart produces no duplicates.

### Stage 4 — Gold (aggregates)

Two Gold tables are recomputed from scratch each micro-batch:

**`top_artists_streaming`** — monthly stream counts per artist, used by the
Top Artists live-feed section:

```
SELECT artist,
       date_trunc('month', chart_date) AS month_start,
       SUM(streams)                    AS monthly_streams,
       COUNT(*)                        AS chart_entries
FROM silver/charts_streaming
WHERE streams IS NOT NULL
GROUP BY artist, month_start
ORDER BY monthly_streams DESC
```

**`recent_submissions`** — the 100 most recently ingested rows from Silver,
ordered by `ingestion_ts` descending. Powers the "Recently Processed" table
on the Submit page, selecting only display columns:
`ingestion_ts, artist, title, region, chart, rank, streams, chart_date`.

### Stage 5 — Metrics

After each micro-batch completes, one JSON line is appended to
`reports/streaming_metrics.jsonl`:

```json
{
  "batch_id": 42,
  "rows": 5000,
  "processing_ms": 1847.3,
  "throughput_rows_per_sec": 2706.9,
  "bronze_total_rows": 215000,
  "ts": "2026-05-20T12:50:22"
}
```

If an MLflow run is active, the same values are logged via
`mlflow.log_metrics(..., step=batch_id)`.

---

## 4. Message Format

Every Kafka message is a UTF-8 JSON object serialised from a Python `dict`.
All values are strings to match the Bronze schema (casting happens at Silver):

```json
{
  "title":   "Anti-Hero",
  "rank":    "1",
  "date":    "2026-05-20",
  "artist":  "Taylor Swift",
  "url":     "https://open.spotify.com/track/5sdQOyqq2IDhvmx2lHOpwd",
  "region":  "us",
  "chart":   "top200",
  "trend":   "SAME_POSITION",
  "streams": "4821033"
}
```

`streams` is an empty string `""` for viral50 entries (no stream count available).
The Silver cast converts it to `null (LongType)`, which is the correct
representation for "not measured".

---

## 5. Spark Consumer — internals

**File:** `src/bigdata_music/streaming/consumer.py`

```
SparkSession  ←  get_spark_streaming()
                 (adds spark-sql-kafka-0-10_2.12:3.5.0 to jars.packages
                  alongside io.delta:delta-spark_2.12:3.2.0)

readStream
  .format("kafka")
  .option("subscribe", "charts-feed")
  .option("startingOffsets", "earliest")
  .option("maxOffsetsPerTrigger", "5000")

  ─ Kafka row has columns: key, value, topic, partition, offset, timestamp …
  ─ value is raw bytes

.select(
    from_json(col("value").cast("string"), CHARTS_RAW_SCHEMA).alias("data"),
    col("timestamp").alias("kafka_timestamp"),
)
.select("data.*", "kafka_timestamp")
.filter(col("title").isNotNull() | col("artist").isNotNull())
  ─ drops rows where from_json failed entirely (malformed JSON)

.writeStream
  .trigger(processingTime="10 seconds")
  .foreachBatch(process_batch)
  .option("checkpointLocation", "data/checkpoints/charts_streaming")
  .start()
  .awaitTermination()
```

The `SparkSession` is shared with `batch_handler` via `batch_handler.set_spark(spark)`
before the query starts. This is necessary because Delta MERGE (used in
`_merge_silver`) requires a running SparkSession for the `DeltaTable.forPath`
call.

**Why `get_spark_streaming()` instead of `get_spark()`?**

The Kafka SQL JAR (`spark-sql-kafka-0-10_2.12`) is ~50 MB and is downloaded
from Maven Central on first start via Ivy. Adding it to `get_spark()` would
impose this download cost on every batch pipeline run (notebooks 00–02), which
is unnecessary. The two factory functions share a `_base_builder` helper and
only differ in the `jars.packages` config.

---

## 6. Silver Cleaning Pipeline

**File:** `src/bigdata_music/silver/clean_charts.py`, function `transform_charts(df)`

All six steps are pure DataFrame transformations — no I/O — and are applied in
sequence by both the batch and streaming paths.

| Step | Function | What it does |
|------|----------|--------------|
| 1 | `extract_track_id` | `regexp_extract(url, r"track/([A-Za-z0-9]+)$", 1)` → `track_id`; null if no match |
| 2 | `cast_date_with_fallback` | Tries `yyyy-MM-dd`, `MM/dd/yyyy`, `dd-MM-yyyy` via `coalesce(to_date(...))` → `chart_date` |
| 3 | `cast_numeric_columns` | `rank` STRING → INT, `streams` STRING → LONG (null for Viral 50) |
| 4 | `filter_corrupt_rows` | Drops rows with null `track_id`, `chart_date`, or `rank`; logs counts |
| 5 | `deduplicate_chart_entries` | `row_number()` over `(chart_date, region, rank)` window ordered by `ingestion_ts` DESC; keeps latest |
| 6 | `add_time_columns` | Derives `year` and `month` from `chart_date` for partition alignment |

After all steps, the `date` and `_corrupt_record` columns are dropped.

**Deduplication rationale:** Step 5 uses `row_number()` rather than `distinct()`
because we have a deterministic tie-breaker (`ingestion_ts`). This ensures the
most recently received message wins when the same chart slot appears more than
once — which happens when the replay producer sends overlapping date ranges or
the dashboard form submits a row with the same (date, region, rank) as an
existing entry.

---

## 7. Gold Aggregates

### `top_artists_streaming`

Location: `data/gold/top_artists_streaming`

Built by `_update_gold_top_artists()` in `batch_handler.py`. Reads the entire
`silver/charts_streaming` table, filters to rows where `streams IS NOT NULL`
(excluding viral50), groups by `(artist, month_start)`, aggregates stream sum
and chart entry count, orders by streams descending.

Overwritten in full each micro-batch — acceptable because Silver is small in
the streaming context (≤ 705 K rows in demo) and the GROUP BY is fast.

The Top Artists page (`/artists`) checks `available_tables()` for
`top_artists_streaming` every 30 s and renders a "● STREAMING LIVE" section
above the batch charts if it exists.

### `recent_submissions`

Location: `data/gold/recent_submissions`

Built by `_update_gold_recent_submissions()`. Reads Silver streaming, sorts by
`ingestion_ts` descending, takes the top 100 rows, writes them to Gold. The
Submit page reads this table every 15 s to populate the "Recently Processed"
feed. Keeping it in Gold means no additional volume mount is needed for the
`dash` container — it already has `/data/gold:ro`.

---

## 8. Checkpointing and Fault Tolerance

Location: `data/checkpoints/charts_streaming`

Spark Structured Streaming writes a checkpoint after every successfully
committed micro-batch. The checkpoint contains:
- **Offsets** committed to Kafka (which messages have been consumed).
- **WAL** (write-ahead log) of the pending batch state.
- **Metadata** about the streaming query.

On consumer restart:
1. Spark reads the checkpoint to find the last committed Kafka offset.
2. It resumes reading from that offset, not from `startingOffsets=earliest`.
3. The Silver MERGE's `WHEN NOT MATCHED` condition means any rows that were
   partially written before the crash are idempotently re-processed without
   producing duplicates.

This gives **exactly-once delivery** at the Delta layer, even if the consumer
container crashes mid-batch.

The `ivy-cache` Docker named volume (`/root/.ivy2`) persists the downloaded
Kafka/Delta JARs between consumer container restarts so the ~50 MB Ivy
resolution only happens on the very first start.

---

## 9. Streaming Metrics

**File:** `src/bigdata_music/streaming/streaming_metrics.py`

`log_batch_metrics(batch_id, rows, processing_ms, bronze_total_rows)` is
called at the end of every `process_batch` invocation. It:

1. Appends a JSON record to `reports/streaming_metrics.jsonl`.
2. Writes a structured log line at INFO level.
3. If an MLflow run is active, logs `streaming_batch_rows`,
   `streaming_processing_ms`, and `streaming_throughput_rows_per_s` as
   step metrics (step = batch_id).

The metrics file is read by `notebooks/03_streaming_demo.ipynb` to plot
throughput and latency over time, satisfying AA4.3.

**Measured values from the demo run (143 batches, 705,122 total rows):**

| Metric | Typical range | Notes |
|--------|--------------|-------|
| Rows per batch | 5,000 | Capped by `maxOffsetsPerTrigger` |
| Processing latency | 1,400 – 4,600 ms | Includes Bronze append + Silver MERGE + two Gold writes |
| Throughput | 1,400 – 2,800 rows/s | Lower on tail batches as topic drains |
| Trigger interval | 10 s | Configurable in `consumer.py` |
| Bronze cumulative | 705,122 | After 30-date replay run |

The end-to-end latency from Kafka publish to dashboard update is
**≤ 40 seconds**: at most 10 s until the next trigger fires, plus
~2–4 s of processing, plus the 30 s `dcc.Interval` on the dashboard.

---

## 10. Dashboard Integration

### Top Artists page (`/artists`)

The `update_live_feed` callback fires every 30 s via `dcc.Interval`. It calls
`available_tables()` (scans `data/gold/` for Delta log directories), and if
`top_artists_streaming` is present:
1. Clears the `load_gold` LRU cache to force a fresh read.
2. Loads the table via `deltalake.DeltaTable` (Python, no Spark).
3. Renders a horizontal bar chart under a "● STREAMING LIVE" badge.

If the streaming stack is not running the section is simply absent — the page
degrades gracefully to batch data only.

### Submit page (`/submit`)

The `update_recent` callback fires every 15 s. It checks for
`recent_submissions` in `available_tables()` and, if present, clears the cache
and renders a `DataTable` showing the 20 most recently processed rows, sorted
by `ingestion_ts` descending.

Both callbacks use `load_gold.cache_clear()` before re-reading. Without this,
the `@lru_cache` on `load_gold` would serve the same DataFrame indefinitely.

---

## 11. Submit Page — single entry and bulk JSON

**File:** `dashboard/pages/submit.py`

### Single entry

The form collects: artist, title, region (dropdown of 30 regions), chart type
(top200 / viral50), rank (1–200), streams (optional), date. On submit:

1. Required fields are validated client-side (Dash State pattern).
2. A fake Spotify URL is derived: `md5(f"{artist}:{title}").hexdigest()[:22]`
   gives a 22-char hex string that passes the Silver `regexp_extract` pattern
   and produces a unique `track_id` per (artist, title) pair.
3. The row dict is serialised to JSON and sent to `charts-feed` via
   `kafka-python` with a 5 s request timeout.

### Bulk JSON

Accepts a JSON array. Each object is validated for required keys
(`artist, title, region, chart, rank, date`). All valid rows are published in
a loop through a single `KafkaProducer` connection.

Example payload:

```json
[
  {
    "artist": "Billie Eilish", "title": "What Was I Made For?",
    "region": "us", "chart": "top200", "rank": 1,
    "streams": 4821033, "date": "2026-05-20"
  },
  {
    "artist": "SZA", "title": "Snooze",
    "region": "us", "chart": "viral50", "rank": 2,
    "date": "2026-05-20"
  }
]
```

`streams` is optional (omit or set `null` for viral50 entries). All other
fields are required. `chart` defaults to `"top200"` if omitted.

---

## 12. Running the Stack

### Start everything

```bash
make streaming-up
# equivalent to:
# docker compose --profile streaming up -d
```

This starts 7 containers:
- `zookeeper` — Kafka coordination
- `kafka` — message broker
- `kafka-producer` — replays 30 days of charts.csv at 0.3 s / day
- `kafka-consumer` — Spark Structured Streaming consumer
- `mlflow` — experiment tracking (metrics logged per batch)
- `jupyter` — batch pipeline notebooks
- `dash` — Plotly Dash dashboard at http://localhost:8050

### Watch logs

```bash
make streaming-logs
# tails kafka-producer and kafka-consumer logs

docker compose logs dash     # dashboard startup / callback errors
docker compose logs mlflow   # MLflow tracking server
```

### Stop

```bash
make streaming-down
```

### First run note

On first start, the `kafka-consumer` container downloads the Kafka SQL and
Delta JARs via Ivy (~50 MB total). This takes 30–60 s depending on internet
speed. Subsequent starts use the `ivy-cache` named volume and start in seconds.

The `kafka-producer` also installs `kafka-python` and `pandas` via pip on
every start (the image is `python:3.11-slim`). This takes ~20 s. The producer
includes a `_wait_for_kafka` retry loop that polls every 5 s until Kafka is
ready, so the ordering of container startup does not matter.

### Manual producer (custom date range)

```bash
docker compose exec kafka-consumer bash
python /scripts/run_producer.py --delay 0.1 --days 7
# streams 7 days of data as fast as possible
```

Or from the host if `kafka-python` is installed:

```bash
python scripts/run_producer.py --bootstrap localhost:9092 --delay 0.5 --days 30
```

---

## 13. Observed Performance

All measurements from a local Docker environment (1 CPU allocated to
`kafka-consumer`, 4 GB driver memory).

```
Total batches processed : 143
Total rows in Bronze    : 705,122
Average batch latency   : ~2,300 ms
Peak throughput         : ~2,800 rows/s   (batch 17, warm JVM)
Min throughput          : ~29 rows/s      (tail batch — 112 rows only)
Median processing time  : ~2,200 ms
```

The tail batches (batch 141+) show artificially low throughput because the
topic was nearly drained (112 rows vs. 5,000 for a full batch), so the fixed
overhead of Delta MERGE and Gold rewrites dominates the measured time.

For a production deployment the main bottleneck would be the Gold full-recompute
on every micro-batch. For datasets larger than a few million Silver rows,
`_update_gold_top_artists` should be replaced with an incremental aggregation
strategy (e.g. accumulate partial sums per batch and MERGE into a running
aggregate rather than reading all of Silver each time).
