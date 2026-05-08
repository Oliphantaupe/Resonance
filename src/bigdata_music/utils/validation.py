"""Pre/post layer validation checks.

Each check raises ValueError if the assertion fails, stopping the pipeline
before writing corrupt data into the next layer.
"""
from pyspark.sql import DataFrame

from bigdata_music.utils.logging import get_logger

log = get_logger(__name__)


def assert_no_nulls(df: DataFrame, column: str, layer: str) -> None:
    """Raise if any null values exist in a non-nullable column."""
    null_count = df.filter(df[column].isNull()).count()
    if null_count > 0:
        raise ValueError(
            f"[{layer}] Column '{column}' has {null_count} null values — aborting write."
        )
    log.info("[%s] assert_no_nulls(%s): OK", layer, column)


def assert_min_rows(df: DataFrame, min_rows: int, layer: str) -> None:
    """Raise if the DataFrame has fewer rows than expected."""
    actual = df.count()
    if actual < min_rows:
        raise ValueError(
            f"[{layer}] Expected >= {min_rows} rows, got {actual} — aborting write."
        )
    log.info("[%s] assert_min_rows(%d): OK (actual=%d)", layer, min_rows, actual)


def assert_column_range(df: DataFrame, column: str, lo: float, hi: float, layer: str) -> None:
    """Raise if any value in column falls outside [lo, hi]."""
    out_of_range = df.filter((df[column] < lo) | (df[column] > hi)).count()
    if out_of_range > 0:
        raise ValueError(
            f"[{layer}] Column '{column}' has {out_of_range} values outside [{lo}, {hi}]."
        )
    log.info("[%s] assert_column_range(%s, [%.1f, %.1f]): OK", layer, column, lo, hi)


def log_layer_summary(df: DataFrame, layer: str, table: str) -> None:
    """Log a quick row count + column count summary before writing."""
    log.info("[%s] %s — %d rows × %d columns", layer, table, df.count(), len(df.columns))
