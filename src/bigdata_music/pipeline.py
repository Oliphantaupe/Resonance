"""Pipeline orchestrator — Bronze → Silver → Gold.

Run the full pipeline:
    python -m bigdata_music.pipeline

Run individual stages via feature flags:
    RUN_BRONZE=true RUN_SILVER=false RUN_GOLD=false python -m bigdata_music.pipeline

The pipeline is structured as a sequence of named stages. Each stage is
logged and timed. Metrics are written to reports/perf_metrics.jsonl and
forwarded to the MLflow tracking server (MLFLOW_TRACKING_URI env var,
default http://localhost:5000).  Browse runs at http://localhost:5000 after
starting: docker compose up mlflow
"""
import os
import shutil
import sys
from pathlib import Path

from bigdata_music import config
from bigdata_music.spark_session import get_spark
from bigdata_music.utils.logging import get_logger
from bigdata_music.utils.metrics import METRICS_FILE, measure

log = get_logger(__name__)

# ── MLflow helpers ────────────────────────────────────────────────────

def _mlflow_log_metric(key: str, value) -> None:
    """Log a metric to the active run; silently skip if MLflow is unavailable."""
    try:
        import mlflow
        if mlflow.active_run() and value is not None:
            mlflow.log_metric(key.replace(".", "_"), float(value))
    except Exception:
        pass


def _mlflow_log_delta_dataset(spark, path: str, name: str, context: str = "output") -> None:
    """Register a written Delta table as an MLflow dataset input record."""
    try:
        import mlflow
        if not mlflow.active_run():
            return
        df = spark.read.format("delta").load(path)
        dataset = mlflow.data.from_spark(df, path=path, name=name)
        mlflow.log_input(dataset, context=context)
    except Exception as exc:
        log.debug("MLflow dataset logging skipped for %s: %s", name, exc)


# ── Stage cleanup ─────────────────────────────────────────────────────

def _clean(*paths: str) -> None:
    """Remove Delta output directories before writing.

    Hadoop's NativeIO.Windows.access0 is called when _delta_log already exists,
    which fails without a matching hadoop.dll. Deleting first ensures listFrom
    sees a missing directory (FileNotFoundException) instead of an empty one.
    """
    for p in paths:
        if Path(p).exists():
            shutil.rmtree(p, ignore_errors=True)
            log.info("Cleaned output path: %s", p)


# ── Pipeline stages ───────────────────────────────────────────────────

def run_bronze(spark) -> None:
    from bigdata_music.ingestion.charts import ingest_charts
    from bigdata_music.ingestion.features import ingest_features
    from bigdata_music.ingestion.countries import ingest_countries

    _clean(config.BRONZE_CHARTS, config.BRONZE_TRACK_FEATURES, config.BRONZE_COUNTRIES)
    log.info("=== BRONZE ===")

    with measure(spark, "bronze.charts"):
        n = ingest_charts(spark)
        log.info("bronze.charts: %d rows", n)
    _mlflow_log_metric("bronze_charts_rows", n)

    with measure(spark, "bronze.track_features"):
        n = ingest_features(spark)
        log.info("bronze.track_features: %d rows", n)
    _mlflow_log_metric("bronze_track_features_rows", n)

    with measure(spark, "bronze.countries"):
        n = ingest_countries(spark)
        log.info("bronze.countries: %d rows", n)
    _mlflow_log_metric("bronze_countries_rows", n)


def run_silver(spark) -> None:
    from bigdata_music.silver.clean_charts import build_silver_charts
    from bigdata_music.silver.clean_features import build_silver_features
    from bigdata_music.silver.clean_countries import build_silver_countries
    from bigdata_music.silver.join_enriched import build_charts_enriched

    _clean(
        config.SILVER_CHARTS_CLEANED,
        config.SILVER_TRACKS_CLEANED,
        config.SILVER_TRACKS_CLEANED + "_track_artist",
        config.SILVER_COUNTRIES,
        config.SILVER_CHARTS_ENRICHED,
    )
    log.info("=== SILVER ===")

    with measure(spark, "silver.charts_cleaned"):
        n = build_silver_charts(spark)
        log.info("silver.charts_cleaned: %d rows", n)
    _mlflow_log_metric("silver_charts_cleaned_rows", n)

    with measure(spark, "silver.tracks_cleaned"):
        t, ta = build_silver_features(spark)
        log.info("silver.tracks_cleaned: %d track-level, %d track-artist", t, ta)
    _mlflow_log_metric("silver_tracks_cleaned_rows", t)
    _mlflow_log_metric("silver_track_artist_rows", ta)

    with measure(spark, "silver.countries"):
        n = build_silver_countries(spark)
        log.info("silver.countries: %d rows", n)
    _mlflow_log_metric("silver_countries_rows", n)

    with measure(spark, "silver.charts_enriched"):
        n = build_charts_enriched(spark)
        log.info("silver.charts_enriched: %d rows", n)
    _mlflow_log_metric("silver_charts_enriched_rows", n)


def run_gold(spark) -> None:
    from bigdata_music.gold.mood_seasonal import build_regional_mood_seasonal
    from bigdata_music.gold.streaks import build_streak_analysis
    from bigdata_music.gold.sonic_dna import build_sonic_dna
    from bigdata_music.gold.longevity import build_track_longevity
    from bigdata_music.gold.top_artists import build_top_artists

    _clean(
        config.GOLD_REGIONAL_MOOD,
        config.GOLD_STREAK_ANALYSIS,
        config.GOLD_SONIC_DNA,
        config.GOLD_TRACK_LONGEVITY,
        config.GOLD_TOP_ARTISTS,
    )
    log.info("=== GOLD ===")

    with measure(spark, "gold.regional_mood_seasonal"):
        n = build_regional_mood_seasonal(spark)
        log.info("gold.regional_mood_seasonal: %d rows", n)
    _mlflow_log_metric("gold_regional_mood_rows", n)
    _mlflow_log_delta_dataset(spark, config.GOLD_REGIONAL_MOOD, "regional_mood_seasonal")

    with measure(spark, "gold.streak_analysis"):
        n = build_streak_analysis(spark)
        log.info("gold.streak_analysis: %d rows", n)
    _mlflow_log_metric("gold_streak_analysis_rows", n)
    _mlflow_log_delta_dataset(spark, config.GOLD_STREAK_ANALYSIS, "streak_analysis")

    with measure(spark, "gold.sonic_dna_by_rank_tier"):
        n = build_sonic_dna(spark)
        log.info("gold.sonic_dna_by_rank_tier: %d rows", n)
    _mlflow_log_metric("gold_sonic_dna_rows", n)
    _mlflow_log_delta_dataset(spark, config.GOLD_SONIC_DNA, "sonic_dna_by_rank_tier")

    with measure(spark, "gold.track_longevity"):
        n = build_track_longevity(spark)
        log.info("gold.track_longevity: %d rows", n)
    _mlflow_log_metric("gold_track_longevity_rows", n)
    _mlflow_log_delta_dataset(spark, config.GOLD_TRACK_LONGEVITY, "track_longevity")

    with measure(spark, "gold.global_top_artists"):
        n = build_top_artists(spark)
        log.info("gold.global_top_artists: %d rows", n)
    _mlflow_log_metric("gold_top_artists_rows", n)
    _mlflow_log_delta_dataset(spark, config.GOLD_TOP_ARTISTS, "global_top_artists")


# ── Entry point ───────────────────────────────────────────────────────

def main() -> None:
    log.info("Starting bigdata-music pipeline")
    log.info("DATA_ROOT=%s  AQE=enabled", config.DATA_ROOT)

    spark = get_spark()
    aqe = spark.conf.get("spark.sql.adaptive.enabled", "false").lower() == "true"

    # Configure MLflow tracking (falls back gracefully if server is unreachable)
    try:
        import mlflow
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("bigdata-music")
        run_ctx = mlflow.start_run(run_name="full_pipeline")
        mlflow.log_params({
            "aqe_enabled":         str(aqe),
            "shuffle_partitions":  config.SHUFFLE_PARTITIONS,
            "run_bronze":          config.RUN_BRONZE,
            "run_silver":          config.RUN_SILVER,
            "run_gold":            config.RUN_GOLD,
            "data_root":           str(config.DATA_ROOT),
        })
        log.info("MLflow run started: %s  UI=%s", run_ctx.info.run_id, tracking_uri)
    except Exception as exc:
        log.warning("MLflow unavailable — pipeline will still run: %s", exc)
        run_ctx = None

    try:
        if config.RUN_BRONZE:
            run_bronze(spark)
        else:
            log.info("Skipping Bronze (RUN_BRONZE=false)")

        if config.RUN_SILVER:
            run_silver(spark)
        else:
            log.info("Skipping Silver (RUN_SILVER=false)")

        if config.RUN_GOLD:
            run_gold(spark)
        else:
            log.info("Skipping Gold (RUN_GOLD=false)")

        log.info("Pipeline complete.")

        # Attach perf metrics file as an artifact
        if run_ctx and METRICS_FILE.exists():
            try:
                import mlflow
                mlflow.log_artifact(str(METRICS_FILE), artifact_path="reports")
            except Exception:
                pass

    except Exception:
        log.exception("Pipeline failed — see traceback above")
        if run_ctx:
            try:
                import mlflow
                mlflow.end_run(status="FAILED")
            except Exception:
                pass
        sys.exit(1)
    finally:
        if run_ctx:
            try:
                import mlflow
                mlflow.end_run()
            except Exception:
                pass
        spark.stop()


if __name__ == "__main__":
    main()
