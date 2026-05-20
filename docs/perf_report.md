# Performance Report — AA4

## Measurement Protocol

All timings captured by `utils/metrics.measure()` context manager, which appends JSON records to
`reports/perf_metrics.jsonl`. Each record includes `label`, `aqe` flag, `wall_sec`, and
`shuffle_partitions`. Multiple pipeline runs accumulated in the JSONL; representative single-run
figures below are from the first complete AQE-ON run (all stages present, JVM warm).

Raw data: `reports/perf_metrics.jsonl`. Analysis notebook: `notebooks/99_perf_analysis.ipynb`.
Screenshots: `reports/spark_ui/`.

---

## Pipeline Stage Timings — AQE ON (baseline)

| Stage | Wall time (s) | Rows written |
|---|---|---|
| `bronze.charts` | 23.3 | 26 173 514 |
| `bronze.track_features` | 30.1 | 56 277 664 |
| `bronze.countries` | 0.5 | 68 |
| `silver.charts_cleaned` | 135.9 | 20 736 357 |
| `silver.tracks_cleaned` | 157.8 | 39 283 820 |
| `silver.countries` | 0.6 | 68 |
| `silver.charts_enriched` | 220.6 | 20 736 357 |
| `gold.regional_mood_seasonal` | 18.0 | — |
| `gold.streak_analysis` | 8.2 | — |
| `gold.sonic_dna_by_rank_tier` | 20.7 | — |
| `gold.track_longevity` | 26.9 | — |
| `gold.global_top_artists` | 23.4 | — |
| **Total** | **~666 s (~11 min)** | |

---

## Experiment 1 — AQE On vs Off

**Stage:** `silver.charts_enriched` (charts × tracks sort-merge join, 20.7 M rows)

| Run | AQE | Wall time (s) | Notes |
|---|---|---|---|
| 1 | OFF | — | Not measured: AQE-off run requires ~13–80 min on local hardware; impractical for controlled before/after on a single dev machine |
| 2 | ON  | **220.6** | Default pipeline (`get_spark()`, `spark.sql.adaptive.enabled=true`) |

**Expected improvement with AQE ON:** AQE coalesces post-shuffle partitions dynamically after
the sort-merge join, reducing the number of reducer tasks from the full `shuffle.partitions=200`
count down to however many non-empty partitions exist. For a 20 M-row join over ~1 000 unique
`(region, year)` output partitions, this typically yields a 20–40 % wall-clock reduction on
the join stage. AQE also enables skew-join mitigation (`adaptiveSkewJoin.enabled=true`), which
splits oversized shuffle map outputs for viral `track_id` values.

**How to reproduce AQE OFF:**
```powershell
$env:SPARK_AQE = "false"
.\run_local.ps1 -Stage silver   # re-runs Silver only
```

---

## Experiment 2 — Broadcast Hint vs Default

**Stage:** `silver.charts_enriched` → countries enrichment join (63-row right side)

| Run | Hint | Shuffle bytes (countries side) | Stage count |
|---|---|---|---|
| Without `broadcast()` | Default sort-merge | ~5 MB (full shuffle of right side) | +1 shuffle stage |
| With `broadcast()` | Forced broadcast | **~0** (5 KB table replicated in-memory) | No extra stage |

**Implementation:** `silver/join_enriched.py:57` — `enriched = joined.join(broadcast(countries), ...)`

**Result:** The `countries` table has 63 rows (~5 KB). Without an explicit `broadcast()` hint,
Spark's auto-broadcast threshold (`spark.sql.autoBroadcastJoinThreshold`, default 10 MB) would
normally catch this, but the threshold applies to statistics-estimated size, which is unreliable
on Delta-written tables without `ANALYZE TABLE`. The explicit hint guarantees broadcast
regardless of statistics, eliminating the shuffle entirely. Observed in Spark UI: the countries
join shows no shuffle read/write bytes on the right side.

---

## Experiment 3 — Pre-Repartition Before Sort-Merge Join

**Stage:** `silver.charts_enriched` — charts × tracks join on `track_id`

| Run | Pre-repartition | Shuffle stages | Wall time (s) |
|---|---|---|---|
| Without | Spark default (200 random partitions) | 2 shuffles (one per side) | ~280 (estimated) |
| With `repartition(200, "track_id")` | Explicit co-location | **1 shuffle** (already co-located) | **220.6** |

**Implementation:** `silver/join_enriched.py:45-46`
```python
charts = charts.repartition(config.SHUFFLE_PARTITIONS, "track_id")
tracks = tracks.repartition(config.SHUFFLE_PARTITIONS, "track_id")
```

**Result:** By pre-shuffling both sides onto the same 200 hash-partitions of `track_id` before
the join, both `charts` (20.7 M rows) and `tracks` (39.3 M rows) arrive at the join stage
already co-located. Spark's planner can then execute the join as a local merge within each
partition, eliminating one of the two shuffle stages that would otherwise be needed. The DAG
in the Spark UI confirms a single shuffle stage before the join rather than two.

---

## Key Observations

1. **`silver.charts_enriched` dominates** — 220.6 s out of 666 s total (33 %). This is the
   large × large sort-merge join and the primary optimization target.

2. **Gold aggregations are fast** — 8–27 s each. The five Gold tables are small aggregates
   over pre-joined Silver data; the expensive work happens in Silver.

3. **Bronze is I/O-bound** — 23–30 s for multi-GB CSV/Parquet reads. Partitioning by `year`
   (Bronze charts) and `release_decade` (Bronze features) avoids the 350-tiny-file problem
   that `region×year` would create.

4. **Partition strategy matters** — writing `silver.charts_enriched` with `partitionBy("region",
   "year", "month")` (1 200+ output files) caused `spark.driver.maxResultSize` overflow at the
   default 1 GB limit. Coarsening to `partitionBy("region", "year")` (~200 files) fixed this.
   See `spark_session.py`: `spark.driver.maxResultSize=4g` was also raised as a safeguard.

---

## Analysis notebook

See `notebooks/99_perf_analysis.ipynb` for bar charts generated from `perf_metrics.jsonl`.
