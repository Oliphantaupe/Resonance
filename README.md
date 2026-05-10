# Resonance — Spotify Charts Big Data Pipeline

Medallion pipeline (Bronze → Silver → Gold) over 26 million Spotify chart entries, enriched with audio features and country metadata, served through a 5-page Plotly Dash dashboard. Built with PySpark 3.5, Delta Lake, and MLflow.

---

## Prerequisites

| Tool | Install |
|---|---|
| **Docker Desktop** | [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/) — enable WSL 2 backend on Windows |
| **`make`** | macOS/Linux: pre-installed. Windows: `choco install make` (requires [Chocolatey](https://chocolatey.org/install)) or use the equivalent commands listed below |
| **Python 3.10+** | Only needed to run the data download script — not needed inside Docker |
| **Kaggle account** | [kaggle.com](https://www.kaggle.com) — needed to download the charts dataset |

---

## Step 1 — Get the data

The raw datasets are not included in the repo (~7.4 GB total). Download them once before running anything. `countries.csv` is not on Kaggle or HuggingFace — download it from [Google Drive](https://drive.google.com/file/d/1zJRo3bwBxV7f87D0kyqs7GBOb-jf5o7k/view?usp=sharing) and place it at `data/raw/countries.csv`.

### 1a — Install download dependencies

```bash
pip install kaggle huggingface_hub
```

> On Windows use `python -m pip install kaggle huggingface_hub`

### 1b — Set up Kaggle credentials

1. Go to [kaggle.com](https://www.kaggle.com) → Account → **API** → **Create New Token**
2. This downloads `kaggle.json`. Place it at:
   - **macOS / Linux:** `~/.kaggle/kaggle.json`
   - **Windows:** `C:\Users\<your-username>\.kaggle\kaggle.json`

### 1c — Download

```bash
python scripts/download_data.py --source kaggle   # charts.csv (~3.5 GB)
python scripts/download_data.py --source hf       # track_features.parquet (~4.1 GB)
python scripts/download_data.py --source verify   # confirm both files OK
```

Expected output from `verify`: three `OK` lines for `charts.csv`, `track_features.parquet`, and `countries.csv`.

---

## Step 2 — Start the stack

```bash
make up
```

**Windows without `make`:**
```bash
docker compose up -d jupyter dash mlflow
```

Pulls images and builds the Jupyter container on first run (~3–5 min). Wait until all services are healthy:

```bash
docker compose ps   # all should show "running"
```

---

## Step 3 — Run the pipeline

```bash
make pipeline
```

**Windows without `make`:**
```bash
docker compose exec jupyter python -m bigdata_music.pipeline
```

Runs Bronze → Silver → Gold inside the Jupyter container. Takes **~55 minutes** on first run. When done you'll see `Pipeline complete` in the terminal.

---

## Step 4 — Explore

| URL | What's there |
|---|---|
| **http://localhost:8050** | Plotly Dash dashboard (5 pages) |
| **http://localhost:5000** | MLflow — run history, metrics, params |
| **http://localhost:4040** | Spark Application UI *(only while pipeline runs)* |
| **http://localhost:8888** | Jupyter Lab |

---

## Performance charts

After the pipeline completes, generate the AA4 performance charts (no Jupyter needed):

```bash
python scripts/perf_charts.py
```

Saves three PNG files to `reports/spark_ui/` and prints a summary table.

---

## Tests

```bash
make test
```

**Windows without `make`:**
```bash
docker compose exec jupyter pytest /src/tests/ -v --cov=/src/bigdata_music --cov-report=term-missing
```

No data files needed — tests use inline `createDataFrame()`.

---

## Stopping everything

```bash
make down
# or
docker compose down
```

---

## Re-running individual layers

```bash
# Silver + Gold only (skip Bronze — saves ~5 min after first run)
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
│   ├── de_report.md         # M2 Data Engineering report (MLflow, tests, MLOps tools)
│   ├── perf_report.md       # AA4 performance analysis (3 experiments)
│   └── spark_ui/            # Generated performance charts
├── scripts/
│   ├── download_data.py     # Data acquisition (Kaggle + HuggingFace)
│   └── perf_charts.py       # Generate performance charts (run after pipeline)
├── notebooks/
│   └── 99_perf_analysis.ipynb  # Same as perf_charts.py, in notebook form (Docker/Jupyter)
├── tests/                   # pytest — Silver and Gold unit tests
├── data/
│   └── raw/                 # Not in repo — see Step 1 above
│   └── README.md            # Data provenance — MD5 hashes + source URLs
├── docker-compose.yml
└── Makefile                 # Shortcuts (see commands above)
```

---

## Troubleshooting

**`make pipeline` fails with "no such container"**  
The `jupyter` service isn't ready yet. Run `docker compose ps` and wait until all containers show `running`, then retry.

**Dashboard shows no data**  
The pipeline hasn't run yet, or it failed. Run `make pipeline` and check for errors.

**Port already in use**  
Another service is using 8050, 5000, 8080, or 8888. Stop it or change the port in `docker-compose.yml`.

**Download fails: `kaggle.json` not found**  
Follow Step 1b — the credentials file must be in the exact path listed.
