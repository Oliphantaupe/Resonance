# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project context

UE28 Big Data academic project. Spotify Charts + Track Features + Country Enrichment through a Bronze → Silver → Gold medallion pipeline on Delta Lake, visualised by a multi-page Plotly Dash dashboard. MLflow tracks every pipeline run. Deadline M2: **2026-05-10**.

---

## Commands

### Run the pipeline locally (Windows)

```powershell
$env:DATA_ROOT = "e:\Resonance\data"
$env:PYTHONPATH = "e:\Resonance\src"
python -m bigdata_music.pipeline
```

Or via the helper script:
```powershell
.\run_local.ps1              # full pipeline + dashboard
.\run_local.ps1 -Stage pipeline   # pipeline only
.\run_local.ps1 -Stage silver     # Silver only (RUN_BRONZE=false RUN_GOLD=false)
```

Feature flags skip layers: `$env:RUN_BRONZE="false"` etc.

### Run tests locally

```powershell
$env:PYTHONPATH = "e:\Resonance\src"
pytest tests/ -v --cov=bigdata_music --cov-report=term-missing
```

Run a single test file:
```powershell
pytest tests/test_clean_charts.py -v
```

### Docker (preferred for pipeline runs)

```bash
make up          # start all services
make pipeline    # run pipeline inside jupyter container
make test        # run pytest inside jupyter container
make mlflow-ui   # start MLflow at http://localhost:5000
```

Services: `jupyter` (8888 / Spark UI 4040, runs pipeline in `local[*]` mode), `dash` (8050), `mlflow` (5000). Kafka/Zookeeper are dormant (streaming profile only). The former `bitnami/spark` cluster was removed — it was pulled from Docker Hub.

### Dashboard

```powershell
$env:DATA_ROOT = "e:\Resonance\data"
$env:PYTHONPATH = "e:\Resonance\src"
python dashboard/app.py
```

---

## Architecture

### Medallion pipeline

```
data/raw/  →  Bronze (Delta)  →  Silver (Delta)  →  Gold (Delta)  →  Dash
```

**Entry point:** `src/bigdata_music/pipeline.py` — `main()` calls `run_bronze()`, `run_silver()`, `run_gold()` in sequence. Each stage is wrapped in `utils/metrics.measure()` for wall-clock + MLflow logging.

**Config:** All paths, tuning knobs, and feature flags live in `src/bigdata_music/config.py`. Everything is driven by env vars (`DATA_ROOT`, `SPARK_SHUFFLE_PARTITIONS`, `RUN_BRONZE/SILVER/GOLD`, etc.). Never hardcode a path.

**Data Catalog:** `conf/catalog.yml` — Kedro-style catalog listing all 13 datasets (raw/bronze/silver/gold) with paths, formats, schemas, row counts, partition strategy, and the module that produces each one. Mirrors `config.py` constants but adds human-readable metadata.

**Schemas:** All three raw sources have explicit `StructType` declarations in `src/bigdata_music/schemas.py`. `inferSchema=false` is mandatory — do not change this.

### Bronze

Three ingestion modules under `src/bigdata_music/ingestion/`. Each reads raw → adds `ingestion_ts` → writes Delta with explicit partitioning rationale:
- `charts.py`: CSV → `partitionBy("year")` (5 partitions ~700 MB each; region×year would be 350 tiny files)
- `features.py`: Parquet → `partitionBy("release_decade")`
- `countries.py`: CSV → no partition (63 rows, always broadcast)

All reads use `mode("PERMISSIVE")` with `_corrupt_record` capture.

### Silver

Named, testable transformation functions — not inline lambdas. Key modules:
- `clean_charts.py`: `extract_track_id` → `cast_date_with_fallback` → `cast_numeric_columns` → `filter_corrupt_rows` → `deduplicate_chart_entries` → `add_time_columns`. Written `partitionBy("region", "year")`.
- `clean_features.py`: Clamps audio features to [0,1], derives `mood_quadrant` via Russell's circumplex model (valence × energy), produces both a track-level view and a track-artist view.
- `join_enriched.py`: The central join. Pre-repartitions both sides on `track_id` before the sort-merge join. Uses `broadcast(countries)` for the country enrichment step. Written `partitionBy("region", "year", "month")`.

### Gold

Five aggregation modules, each powering one dashboard page (1:1 mapping):

| Module | Gold table | Dashboard page | Key Spark pattern |
|---|---|---|---|
| `mood_seasonal.py` | `regional_mood_seasonal` | Home / Choropleth | SUM OVER window for mood_share |
| `streaks.py` | `streak_analysis` | Streak Champions | Gap-and-island: `row_number()` + `date_sub()` |
| `sonic_dna.py` | `sonic_dna_by_rank_tier` | Sonic DNA | `ntile(10)` decile bucketing |
| `longevity.py` | `track_longevity` | Era Evolution | `PERCENTILE_APPROX`, rolling avg |
| `top_artists.py` | `global_top_artists` | Top Artists | 90-day rolling sum with `rangeBetween`, `dense_rank()` |

Gold tables are small aggregates — not partitioned.

### Delta I/O on Windows

**Critical:** `spark.read.format("delta").load(path)` crashes on Windows without `hadoop.dll` due to `NativeIO.Windows.access0`. The workaround is `src/bigdata_music/utils/delta_io.py:read_delta()` — it enumerates `.parquet` files with `os.walk` and passes explicit paths to `spark.read.parquet()`. **Always use `read_delta()` to read Delta tables in the pipeline, never `spark.read.format("delta").load()`.**

Writes still use `df.write.format("delta").save()` — this works because no existing `_delta_log` is listed on first write.

### MLflow

The pipeline wraps every run in `mlflow.start_run()` in `pipeline.py:main()`. It logs:
- **Params**: AQE flag, shuffle partitions, stage flags, data root
- **Metrics**: row counts per stage, wall-clock seconds per stage (via `utils/metrics.measure()`)
- **Datasets**: all 5 Gold Delta tables via `_mlflow_log_delta_dataset()`
- **Artifact**: `reports/perf_metrics.jsonl`

All MLflow calls are wrapped in `try/except` — the pipeline continues if the tracking server is unreachable. Set `MLFLOW_TRACKING_URI` to point at a running server; defaults to `http://localhost:5000`.

### Dashboard

`dashboard/data_loader.py` is the only data access point. It uses `deltalake.DeltaTable(path).to_pandas()` with `@lru_cache` — no Spark in the dashboard process, no SQLite, no raw data. Gold tables are small aggregates that fit in memory.

Each page in `dashboard/pages/` reads exactly one Gold table and declares `GOLD_TABLE = "..."` at the top.

---

## Windows-specific gotchas

- **`PYSPARK_PYTHON`**: Tests set `os.environ["PYSPARK_PYTHON"] = sys.executable` in `conftest.py` to avoid the Microsoft Store Python stub. If a Spark worker fails to start, check this.
- **`HADOOP_HOME`**: For local pipeline runs, `run_local.ps1` auto-detects `C:\hadoop\bin\winutils.exe`. Without it, Delta writes may fail on second run (existing `_delta_log` triggers `NativeIO`). The pipeline's `_clean()` helper deletes output directories before each write as a workaround.
- **Log files**: `silver_run*.log`, `gold_run*.log` in the project root are past pipeline run logs. `gold_run.log` contains a historical `country_name` AnalysisException that is already fixed in the current code.

---

## Test structure

`tests/conftest.py` provides a session-scoped `spark` fixture (no Delta packages, `local[2]`, AQE off) to keep tests fast. Tests use inline `spark.createDataFrame()` — no real data files needed. Coverage target: ≥70% on `silver/` and `gold/`.
