"""Silver — charts_enriched: the central join that fuses all three sources.

Join strategy:
  - charts × tracks  : sort-merge join (large × large, on track_id)
  - enriched × countries : broadcast join (large × tiny, ~63 rows)

# RAPPORT: Experiments show that explicit repartition(200, "track_id")
# upstream on both sides before the sort-merge saves one shuffle stage
# because the planner can co-locate matching partitions. See AA4 Experiment 3.

# RAPPORT: broadcast(countries_df) guarantees the small-table broadcast
# regardless of Spark's stats-based auto-broadcast threshold. AA4 Experiment 2
# measures the shuffle-byte reduction this provides.
"""
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col

from bigdata_music import config
from bigdata_music.utils.delta_io import read_delta
from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def build_charts_enriched(spark: SparkSession) -> int:
    """Join charts × tracks × countries → silver/charts_enriched (Delta).

    Returns the row count written.
    """
    log.info("Silver enriched: loading charts, tracks, countries")

    charts = read_delta(spark, config.SILVER_CHARTS_CLEANED)
    tracks = read_delta(spark, config.SILVER_TRACKS_CLEANED)
    countries = read_delta(spark, config.SILVER_COUNTRIES)

    # Drop ingestion_ts from tracks — charts already carries it; keeping both
    # would produce duplicate column names that Delta writer rejects.
    tracks = tracks.drop("ingestion_ts")

    # Repartition both large sides on the join key before sort-merge.
    # This pre-shuffles both sides to the same 200 partitions so the
    # planner avoids a second shuffle during the join itself.
    # RAPPORT (AA4 Experiment 3): without this, both sides shuffle
    # independently — two shuffle stages instead of one.
    charts = charts.repartition(config.SHUFFLE_PARTITIONS, "track_id")
    tracks = tracks.repartition(config.SHUFFLE_PARTITIONS, "track_id")

    # Large × large: sort-merge join on track_id
    joined = charts.join(tracks, on="track_id", how="left")

    # Large × tiny: broadcast join — eliminates shuffle for the right side entirely
    # RAPPORT (AA4 Experiment 2): shuffle bytes for the countries side drop to ~0
    #
    # charts.region holds full country names (e.g. "Argentina"); countries uses
    # country_name as the human-readable key — we join on that column.
    enriched = joined.join(
        broadcast(countries),
        on=joined["region"] == countries["country_name"],
        how="left",
    ).drop(countries["country_name"])

    enriched = enriched.persist(StorageLevel.MEMORY_AND_DISK)
    row_count = enriched.count()
    log.info("Silver enriched: writing %d rows partitioned by (region, year) to %s",
             row_count, config.SILVER_CHARTS_ENRICHED)

    (
        enriched.write
        .format("delta")
        .partitionBy("region", "year")
        .mode("overwrite")
        .save(config.SILVER_CHARTS_ENRICHED)
    )

    enriched.unpersist()
    log.info("Silver enriched: write complete")
    return row_count
