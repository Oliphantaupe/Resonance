"""Pipeline orchestrator — Bronze → Silver → Gold.

Run the full pipeline:
    python -m bigdata_music.pipeline

Run individual stages via feature flags:
    RUN_BRONZE=true RUN_SILVER=false RUN_GOLD=false python -m bigdata_music.pipeline

The pipeline is structured as a sequence of named stages. Each stage is
logged and timed. Metrics are written to reports/perf_metrics.jsonl.
"""
import shutil
import sys
from pathlib import Path

from bigdata_music import config
from bigdata_music.spark_session import get_spark
from bigdata_music.utils.logging import get_logger
from bigdata_music.utils.metrics import measure

log = get_logger(__name__)


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


def run_bronze(spark) -> None:
    from bigdata_music.ingestion.charts import ingest_charts
    from bigdata_music.ingestion.features import ingest_features
    from bigdata_music.ingestion.countries import ingest_countries

    _clean(config.BRONZE_CHARTS, config.BRONZE_TRACK_FEATURES, config.BRONZE_COUNTRIES)
    log.info("=== BRONZE ===")
    with measure(spark, "bronze.charts"):
        n = ingest_charts(spark)
        log.info("bronze.charts: %d rows", n)

    with measure(spark, "bronze.track_features"):
        n = ingest_features(spark)
        log.info("bronze.track_features: %d rows", n)

    with measure(spark, "bronze.countries"):
        n = ingest_countries(spark)
        log.info("bronze.countries: %d rows", n)


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

    with measure(spark, "silver.tracks_cleaned"):
        t, ta = build_silver_features(spark)
        log.info("silver.tracks_cleaned: %d track-level, %d track-artist", t, ta)

    with measure(spark, "silver.countries"):
        n = build_silver_countries(spark)
        log.info("silver.countries: %d rows", n)

    with measure(spark, "silver.charts_enriched"):
        n = build_charts_enriched(spark)
        log.info("silver.charts_enriched: %d rows", n)


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

    with measure(spark, "gold.streak_analysis"):
        n = build_streak_analysis(spark)
        log.info("gold.streak_analysis: %d rows", n)

    with measure(spark, "gold.sonic_dna_by_rank_tier"):
        n = build_sonic_dna(spark)
        log.info("gold.sonic_dna_by_rank_tier: %d rows", n)

    with measure(spark, "gold.track_longevity"):
        n = build_track_longevity(spark)
        log.info("gold.track_longevity: %d rows", n)

    with measure(spark, "gold.global_top_artists"):
        n = build_top_artists(spark)
        log.info("gold.global_top_artists: %d rows", n)


def main() -> None:
    log.info("Starting bigdata-music pipeline")
    log.info("DATA_ROOT=%s  AQE=enabled", config.DATA_ROOT)

    spark = get_spark()

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

    except Exception:
        log.exception("Pipeline failed — see traceback above")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
