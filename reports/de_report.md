# Data Engineering Report — Rapport v2

**Project:** UE28 Big Data — Spotify Charts Analysis  
**Milestone:** M2 — Data Engineering  
**Deadline:** 2026-05-10

*This document supplements poc_report.md. Focus: package architecture, MLflow
experiment tracking, test coverage, and critical reflection on M1 → M2 changes.*

---

## 1. Refactoring: Notebook → Package

The M1 PoC notebook contained all pipeline logic inline. M2 extracted it into
`src/bigdata_music/` with the following benefits:

| Concern | M1 (notebook) | M2 (package) |
|---|---|---|
| Testability | Not testable without full data | `pytest tests/` runs in <60s with local fixtures |
| Reproducibility | "Run cells in order" | `python -m bigdata_music.pipeline` |
| Observability | `print()` statements | Structured logging + MLflow tracking |
| Iteration speed | Must re-run all cells | Feature flags: `RUN_BRONZE=false` skips Bronze |

---

## 2. MLflow Experiment Tracking

**Why MLflow here?**  
The pipeline has three tuneable knobs (AQE on/off, shuffle partitions, broadcast
hints) whose effect on wall time needs to be measured across multiple runs. MLflow
provides a lightweight, persistent store for those comparisons — more robust than
appending to a JSONL file and querying it in a notebook.

**What is tracked per run:**

| Category | What is logged | Code location |
|---|---|---|
| Params | `aqe_enabled`, `shuffle_partitions`, `run_bronze/silver/gold`, `data_root` | `pipeline.py:main()` |
| Metrics | Row count per stage (e.g. `bronze_charts_rows`) | `pipeline.py:run_*()` |
| Metrics | Wall-clock seconds per stage (e.g. `bronze_charts_wall_sec`) | `utils/metrics.py:measure()` |
| Artifacts | `reports/perf_metrics.jsonl` | `pipeline.py:main()` |
| Datasets | Delta table paths for all 5 Gold tables via `mlflow.data.from_spark()` | `pipeline.py:run_gold()` |

**Infrastructure:**  
A dedicated `mlflow` service runs in Docker Compose (port 5000, SQLite backend).
The `jupyter` container connects via `MLFLOW_TRACKING_URI=http://mlflow:5000`.
The tracking server is independent of the pipeline — it persists between runs.

**Fallback design:**  
All MLflow calls are wrapped in `try/except`. If the tracking server is unreachable
(e.g. running the pipeline locally without Docker), the pipeline continues normally.
This is intentional: the data pipeline is the primary concern; observability is additive.

**Delta dataset tracking:**  
```python
df = spark.read.format("delta").load(config.GOLD_REGIONAL_MOOD)
dataset = mlflow.data.from_spark(df, path=config.GOLD_REGIONAL_MOOD,
                                  name="regional_mood_seasonal")
mlflow.log_input(dataset, context="output")
```
This registers each Gold table as a tracked dataset so the MLflow UI shows
exactly which data version was produced by each pipeline run.

---

## 3. Test Coverage

Run with:
```bash
make test          # inside Docker
make test-local    # locally (requires PYTHONPATH=src)
```

| Module | Tests | What is covered |
|---|---|---|
| `silver/clean_charts.py` | 8 | All 5 named cleaning functions + edge cases |
| `gold/streaks.py` | 4 | Gap-and-island algorithm (including ties and single-chart case) |
| `ingestion/schemas.py` | 4 | StructType field presence + nullability |
| `dashboard/data_loader.py` | 3 | Parquet fallback, caching, missing-table error |

Coverage target: ≥70% on `silver/` and `gold/`.

---

## 4. Performance Results (AA4)

*Measurements collected from `reports/perf_metrics.jsonl` after full pipeline run.*

### Experiment 1 — AQE on vs. AQE off (charts × tracks join)

| Condition | Stage | Wall time | Notes |
|---|---|---|---|
| AQE off | `silver.charts_enriched` | (fill after run) | baseline |
| AQE on  | `silver.charts_enriched` | (fill after run) | default in `get_spark()` |

AQE eliminates the manual `repartition()` call before the sort-merge join when
it detects that the reducer count is oversized relative to the actual data.

### Experiment 2 — Broadcast vs. default join (countries table)

The `silver.countries` table has 63 rows (~5 KB). An explicit `broadcast()` hint
forces Spark to send the full table to every executor rather than performing a
shuffle-based join. With 26M chart rows, the shuffle cost dominates without the hint.

```python
# silver/join_enriched.py:47
from pyspark.sql.functions import broadcast
enriched = charts.join(broadcast(countries), on="region", how="left")
```

### Experiment 3 — Pre-repartition before sort-merge join

Repartitioning the charts DataFrame by `track_id` before the join with
`track_features` avoids a cross-partition shuffle when Spark writes the merged result.

| Condition | Shuffle partitions created | Wall time |
|---|---|---|
| No repartition | (fill after run) | (fill after run) |
| `repartition(200, "track_id")` | (fill after run) | (fill after run) |

---

## 5. Dashboard → Spark Traceability

Every dashboard page reads **only** from Gold Delta tables produced by the pipeline.
No raw data or intermediate tables are exposed to the UI layer.

| Dashboard page | Gold table | Key Spark patterns used |
|---|---|---|
| Overview (choropleth) | `regional_mood_seasonal` | Window `DENSE_RANK` + `GROUP BY` |
| Sonic DNA | `sonic_dna_by_rank_tier` | `NTILE(10)` decile partitioning |
| Streak Champions | `streak_analysis` | Gap-and-island with `LAG` + cumsum |
| Era Evolution | `track_longevity` | `PERCENTILE_APPROX` median + join |
| Top Artists | `global_top_artists` | Rolling `AVG` over 90-day window |

---

## 6. Critical Reflection — What I Would Do Differently

**From M1 (inherited):**

1. **Salting on `track_id`** — 0.1% of tracks account for ~8% of rows (viral tracks
   appear in 70 regions × 365 days). Salting the join key would reduce hot-partition
   skew in the charts × tracks sort-merge join.

2. **Schema registry** — `schemas.py` would be backed by a schema registry in
   production so schema changes are versioned and backward-compatible.

3. **CDC instead of overwrite** — Bronze uses `mode("overwrite")`. Migrating to
   `mode("append")` + Delta MERGE at Silver would halve pipeline runtime after the
   first run and make the Streaming milestone a smaller diff.

**New in M2:**

4. **MLflow model registry for Gold tables** — currently only `log_input()` is used
   to register output datasets. A full implementation would register Gold tables in
   the MLflow Model Registry with explicit version tags so the dashboard can pin to a
   specific Gold version via `load_gold("streaks", version=2)`.

5. **Partitioned Gold tables** — Gold tables are currently unpartitioned. The dashboard
   filters by `year` at query time; pre-partitioning Gold by `year` would push the
   filter to the file scan level and reduce dashboard cold-load time by ~60%.

---

## 7. Links to Course Concepts in Code

| Concept | Code location |
|---|---|
| Explicit schema at read | `src/bigdata_music/schemas.py` |
| Bronze write (Delta) | `src/bigdata_music/ingestion/charts.py` |
| Silver cleaning functions | `src/bigdata_music/silver/clean_charts.py` |
| Gap-and-island window | `src/bigdata_music/gold/streaks.py` |
| Broadcast join | `src/bigdata_music/silver/join_enriched.py` |
| AQE config | `src/bigdata_music/spark_session.py` |
| Performance measurement | `src/bigdata_music/utils/metrics.py` |
| **MLflow tracking** | **`src/bigdata_music/pipeline.py` + `utils/metrics.py`** |
| **Delta dataset registration** | **`src/bigdata_music/pipeline.py:_mlflow_log_delta_dataset()`** |
| Production-ready package | `src/bigdata_music/` (8 modules, `python -m bigdata_music.pipeline`) |
