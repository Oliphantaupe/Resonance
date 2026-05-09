# Resonance — Spotify Charts Big Data Pipeline

Medallion pipeline (Bronze → Silver → Gold) over 26 million Spotify chart entries, enriched with audio features and country metadata, served through a 5-page Plotly Dash dashboard. Built with PySpark 3.5, Delta Lake, and MLflow.

---

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Docker Desktop | ≥ 4.x | [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/) |
| Kaggle account | — | [kaggle.com](https://www.kaggle.com) — needed to download the charts dataset |

Everything else (Python, Java, Spark, dependencies) runs inside Docker. No local install required.

---

## Step 1 — Get the data

The raw data is not included in the repo (~7.4 GB total). Download it once before running anything.

### 1a — Kaggle charts dataset (~3.5 GB)

1. Go to [kaggle.com](https://www.kaggle.com) → Account → **API** → **Create New Token**
2. This downloads a `kaggle.json` file. Place it at:
   - **Windows:** `C:\Users\<you>\.kaggle\kaggle.json`
   - **macOS / Linux:** `~/.kaggle/kaggle.json`
3. Install the Kaggle CLI and download:
   ```bash
   pip install kaggle huggingface_hub
   python scripts/download_data.py --source kaggle
   ```

### 1b — Track features dataset (~4.1 GB)

```bash
python scripts/download_data.py --source hf
```

### Verify downloads

```bash
python scripts/download_data.py --source verify
```

Expected output: `OK  charts.csv … OK  track_features.parquet … OK  countries.csv`

> `countries.csv` is already in the repo — no download needed.

---

## Step 2 — Start the stack

```bash
make up
```

This starts five services in Docker: Spark master + worker, Jupyter, Dash dashboard, and MLflow.
First run pulls images and builds containers (~3–5 min). Subsequent starts take seconds.

---

## Step 3 — Run the pipeline

```bash
make pipeline
```

Runs Bronze → Silver → Gold inside the Jupyter container. Reads from `data/raw/`, writes Delta tables to `data/bronze/`, `data/silver/`, `data/gold/`. Takes **~10–15 minutes** on a modern laptop.

Progress is printed to the terminal. When it finishes you'll see `Pipeline complete`.

---

## Step 4 — Explore

| URL | What's there |
|---|---|
| **http://localhost:8050** | Plotly Dash dashboard (5 pages) |
| **http://localhost:5000** | MLflow — pipeline run history, metrics, params |
| **http://localhost:8080** | Spark Master UI |
| **http://localhost:4040** | Spark Application UI (only while pipeline is running) |
| **http://localhost:8888** | Jupyter Lab |

Run `make urls` to print this table at any time.

---

## Performance charts

After the pipeline completes, generate the AA4 performance charts (no Jupyter needed):

```bash
python scripts/perf_charts.py
```

Outputs three PNG files to `reports/spark_ui/` and prints a summary table to stdout.

---

## Tests

```bash
make test
```

Runs pytest inside the Docker container. No data files needed — tests use inline `createDataFrame()`.

---

## Stopping everything

```bash
make down
```

---

## Re-running the pipeline

The pipeline deletes and rewrites Delta outputs on each run. To run only one layer:

```bash
# Silver + Gold only (skip Bronze — useful after first run)
docker compose exec jupyter bash -c "RUN_BRONZE=false python -m bigdata_music.pipeline"

# Gold only
docker compose exec jupyter bash -c "RUN_BRONZE=false RUN_SILVER=false python -m bigdata_music.pipeline"
```

---

## Project layout

```
├── src/bigdata_music/       # Pipeline source code
│   ├── pipeline.py          # Entry point — orchestrates Bronze→Silver→Gold
│   ├── config.py            # All paths and tuning knobs (env-var driven)
│   ├── schemas.py           # Explicit StructType schemas for all 3 sources
│   ├── ingestion/           # Bronze: CSV/Parquet → Delta
│   ├── silver/              # Cleaning, joining, enrichment
│   ├── gold/                # 5 aggregation modules (one per dashboard page)
│   └── utils/               # SparkSession, Delta I/O, metrics, logging
├── dashboard/               # Plotly Dash app (reads Gold Delta tables only)
├── conf/catalog.yml         # Kedro-style Data Catalog — all 13 datasets
├── reports/
│   ├── perf_report.md       # AA4 performance analysis (3 experiments)
│   └── spark_ui/            # Generated performance charts
├── scripts/
│   ├── download_data.py     # Data acquisition (Kaggle + HuggingFace)
│   └── perf_charts.py       # Generate performance charts from perf_metrics.jsonl
├── notebooks/
│   └── 99_perf_analysis.ipynb  # Same charts as perf_charts.py, in notebook form
├── tests/                   # pytest — Silver and Gold unit tests
├── docker-compose.yml
├── Makefile                 # All commands (make up / pipeline / test / charts)
└── data/README.md           # Data provenance — MD5 hashes + source URLs
```

---

## Local development (Windows, without Docker)

Requires Python 3.10, Java 11+, and `C:\hadoop\winutils.exe` (see [steveloughran/winutils](https://github.com/steveloughran/winutils)).

```powershell
$env:DATA_ROOT  = "e:\Resonance\data"
$env:PYTHONPATH = "e:\Resonance\src"
$env:HADOOP_HOME = "C:\hadoop"
pip install -e ".[dev]"
python -m bigdata_music.pipeline
```

Dashboard only (no Spark, no Java):

```powershell
$env:DATA_ROOT  = "e:\Resonance\data"
$env:PYTHONPATH = "e:\Resonance\src"
python dashboard/app.py
```
