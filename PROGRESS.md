# Project Progress

**Project:** UE28 Big Data — Spotify Charts Analysis  
**Last updated:** 2026-05-05  
**Current milestone:** M1 — PoC + Dashboard  
**Hard deadline: 2026-05-10 (7 days) — full production-ready code (M2) due**

---

## Milestone overview

| # | Name | Deliverable | Deadline | Status |
|---|---|---|---|---|
| M1 | PoC + Dashboard | `notebooks/02_poc_pipeline.ipynb` + Dash (2 pages) | ASAP (feeds M2) | **In progress** |
| M2 | Data Engineering | `src/bigdata_music/` package + 5 Gold tables + 5 Dash pages + perf report | **2026-05-10** | Pending |
| M3 | Streaming | Kafka replay + Structured Streaming + auto-refresh Dash | TBD | Pending |

---

## M1 — PoC + Dashboard

### Phase 0 — Data acquisition

| Task | Status | Notes |
|---|---|---|
| Create `data/` directory structure | Done | `raw/`, `bronze/`, `silver/`, `gold/` |
| Download `charts.csv` (Source 1 — Kaggle) | Done | 3,322 MB — Kaggle Bearer token via HTTP |
| Download `track_features.parquet` (Source 2 — HuggingFace) | Done | 4,097 MB — huggingface_hub |
| Build `countries.csv` (Source 3 — hand-crafted) | Done | 63 rows, quirks injected (BOM, mixed quotes, leading space) |
| `scripts/download_data.py` re-download script | Done | Handles all 3 sources, `--source verify` |
| `.gitignore` | Done | Excludes `data/` subdirs |

### Phase 1 — Infrastructure

| Task | Status | Notes |
|---|---|---|
| `docker-compose.yml` | **Done** | spark-master, spark-worker, jupyter, dash, kafka (dormant), zookeeper (dormant) |
| `Dockerfile.spark` | **Done** | jupyter/pyspark-notebook:spark-3.5.0 + delta-spark 3.2.0 JARs pre-installed |
| `Dockerfile.dash` | **Done** | python:3.11-slim + plotly dash + deltalake |
| `pyproject.toml` | **Done** | project metadata + all deps |
| `Makefile` | **Done** | `up`, `pipeline`, `test`, `clean-delta`, `spark-ui` targets |
| `src/bigdata_music/spark_session.py` | **Done** | `get_spark()` + `get_spark_no_aqe()` (AA4 Experiment 1) |
| `src/bigdata_music/schemas.py` | **Done** | CHARTS_RAW_SCHEMA, TRACK_FEATURES_RAW_SCHEMA, COUNTRIES_RAW_SCHEMA |
| `src/bigdata_music/config.py` | **Done** | All paths + tuning knobs + feature flags via env vars |

### Phase 2 — Bronze layer

| Task | Status | Notes |
|---|---|---|
| `bronze.charts` | **Done** | `ingestion/charts.py` — partition by `year`, PERMISSIVE + corrupt record |
| `bronze.track_features` | **Done** | `ingestion/features.py` — partition by `release_decade` |
| `bronze.countries` | **Done** | `ingestion/countries.py` — no partition, BOM handled by Spark |

### Phase 3 — Silver layer

| Task | Status | Notes |
|---|---|---|
| `silver.charts_cleaned` | **Done** | `silver/clean_charts.py` — 5 named cleaning functions |
| `silver.tracks_cleaned` | **Done** | `silver/clean_features.py` — track-level + track-artist views, mood_quadrant |
| `silver.countries` | **Done** | `silver/clean_countries.py` — trim whitespace (GB quirk) |
| `silver.charts_enriched` | **Done** | `silver/join_enriched.py` — repartition + sort-merge + broadcast |

### Phase 4 — Gold layer

| Table | Status | Dashboard page | Priority |
|---|---|---|---|
| `gold.regional_mood_seasonal` | **Done** | Regional Mood Map | High |
| `gold.streak_analysis` | **Done** | Streak Champions | High |
| `gold.sonic_dna_by_rank_tier` | **Done** | Sonic DNA | High |
| `gold.track_longevity` | **Done** | Era Evolution | Medium |
| `gold.global_top_artists` | **Done** | Top Artists | Low |

### Phase 5 — Dashboard

| Page | Status | Gold table | Priority |
|---|---|---|---|
| `pages/home.py` — Global KPIs + animated choropleth | **Done** | `regional_mood_seasonal` | High |
| `pages/sonic_dna.py` — Radar + bar by rank tier | **Done** | `sonic_dna_by_rank_tier` | High |
| `pages/streaks.py` — Sortable table + bar chart | **Done** | `streak_analysis` | Medium |
| `pages/era_evolution.py` — Scatter + decade bar | **Done** | `track_longevity` | Medium |
| `pages/top_artists.py` — Rolling streams + radar | **Done** | `global_top_artists` | Low |

### Phase 6 — Tests + Reports

| Task | Status | Notes |
|---|---|---|
| `tests/conftest.py` | **Done** | Session-scoped SparkSession fixture |
| `tests/test_schemas.py` | **Done** | Schema field assertions |
| `tests/test_clean_charts.py` | **Done** | 8 tests for all cleaning functions |
| `tests/test_streaks.py` | **Done** | 4 tests for gap-and-island algorithm |
| `tests/test_data_loader.py` | **Done** | 3 tests with DeltaTable mocked |
| `reports/poc_report.md` | **Done** | Decisions-first rapport template |
| `reports/perf_report.md` | **Done** | AA4 experiment template (fill after pipeline run) |
| `reports/de_report.md` | **Done** | M2 rapport template |

### Phase 7 — PoC notebook

| Task | Status | Notes |
|---|---|---|
| `notebooks/02_poc_pipeline.ipynb` | **Pending** | Need to run pipeline first to generate Gold data |

---

## M1 success criteria checklist

- [ ] At least 11 distinct `.write()` calls visible in notebook
- [ ] All reads use explicit `StructType` (no `inferSchema=true`)
- [ ] Cleaning functions are named and called explicitly
- [ ] Dashboard reads from `/data/gold/*` Delta paths only
- [ ] At least 1 window function pattern visible
- [ ] Rapport v1 explains Delta vs Parquet, medallion rationale, partition choices

---

## M2 — Data Engineering (due 2026-05-10)

Key additions over M1: modular `src/` package, all 5 Gold tables, all 6 window patterns, 3 perf experiments, `pytest` with ≥70% coverage on `silver/` + `gold/`, all 5 Dash pages.

### M2 success criteria checklist

- [ ] `python -m bigdata_music.pipeline` runs the full pipeline end-to-end
- [ ] `pytest tests/` passes with ≥70% coverage on `silver/` and `gold/`
- [ ] `reports/perf_report.md` has 3 before/after measurements with Spark UI screenshots
- [ ] All 5 Gold tables written
- [ ] All 5 Dash pages rendering from Gold Delta
- [ ] Rapport v2 addresses every "I" rating from PoC v1 grade explicitly

### Checklist cross-reference (project_checklist.md)

| Checklist requirement | Architecture answer | Status |
|---|---|---|
| Always `.write()` outputs | 11+ Delta writes; dashboard won't render without them | Pending build |
| Explicit schema at read | `schemas.py` StructType for all 3 sources | Pending build |
| Think about partitioning | Per-layer rationale in arch §6-7 | Pending build |
| Spark-native + readable + commented | DataFrame API only; `# RAPPORT:` hooks | Pending build |
| Advanced transformations | 6 window patterns + 3 complex joins (arch §9) | Pending build |
| Schema at read + cleaning | Named functions in `silver/clean_charts.py` | Pending build |
| Spark → Delta → Dashboard | `data_loader.py` reads Gold Delta only | Pending build |
| ≥2 analytical visualizations | 5 pages (2 minimum enforced for M1) | Pending build |
| Performance measurement + Spark UI | 3 experiments in `utils/metrics.py` (arch §10) | Pending build |
| Decisions-first report | Pre-staged `# RAPPORT:` hooks (arch §15) | Pending build |
| Production-ready refactored code | `src/bigdata_music/` package (M2 scope) | **Due 2026-05-10** |

---

## Decisions log

| Date | Decision | Rationale |
|---|---|---|
| 2026-05-03 | Kaggle download via Bearer HTTP (not CLI) | `KGAT_` token incompatible with kaggle CLI Basic Auth; Bearer + GET + redirect works against signed GCS URL |
| 2026-05-03 | HuggingFace download via `huggingface_hub` | `hf_hub_download` available, no credentials needed for public dataset |
| 2026-05-03 | countries.csv with 63 rows | All Spotify chart regions as of 2017–2021 identified; deliberate quirks per architecture spec |
