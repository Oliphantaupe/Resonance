"""Cached Gold Delta reader for the Dash app.

Uses deltalake (Python, no Spark) to read Gold tables into pandas at
startup. Gold tables are small aggregates (<100 MB each), so in-memory
caching via lru_cache is safe.

# RAPPORT: using deltalake rather than pandas.read_parquet() preserves
# access to Delta features in the rapport narrative: time-travel queries,
# schema evolution, and a clean story about why we chose Delta over Parquet.
# Verified by: grep -r sqlite dashboard/  → no results.
"""
import os
from functools import lru_cache
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable

_env = os.getenv("DATA_ROOT")
if _env:
    DATA_ROOT = Path(_env)
else:
    # Auto-detect: data/ sits at the project root (parent of dashboard/)
    DATA_ROOT = Path(__file__).parent.parent / "data"
GOLD = DATA_ROOT / "gold"


@lru_cache(maxsize=16)
def load_gold(table: str, version: int | None = None) -> pd.DataFrame:
    """Load a Gold Delta table into a pandas DataFrame.

    Args:
        table:   table name (subdirectory under data/gold/)
        version: optional Delta version for time-travel queries

    Cached: subsequent calls with the same arguments return the cached frame.
    Call load_gold.cache_clear() to force a refresh.
    """
    path = str(GOLD / table)
    dt = DeltaTable(path, version=version)
    return dt.to_pandas()


def available_tables() -> list[str]:
    """Return the list of Gold tables that currently exist on disk."""
    if not GOLD.exists():
        return []
    return [d.name for d in GOLD.iterdir() if d.is_dir() and (d / "_delta_log").exists()]
