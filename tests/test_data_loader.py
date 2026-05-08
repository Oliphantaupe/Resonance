"""Tests for the dashboard data_loader module.

These tests verify the loader's interface without requiring Gold Delta
tables to be on disk — they mock the DeltaTable call.
"""
import importlib
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def test_load_gold_calls_delta_table(tmp_path, monkeypatch):
    """load_gold should call DeltaTable with the correct path."""
    import dashboard.data_loader as dl

    # Override DATA_ROOT to the temp dir
    monkeypatch.setattr(dl, "GOLD", tmp_path / "gold")

    mock_dt = MagicMock()
    mock_dt.to_pandas.return_value = pd.DataFrame({"a": [1, 2]})

    with patch("dashboard.data_loader.DeltaTable", return_value=mock_dt) as mock_cls:
        dl.load_gold.cache_clear()
        result = dl.load_gold("streak_analysis")

    mock_cls.assert_called_once()
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2


def test_load_gold_is_cached(tmp_path, monkeypatch):
    """Second call with same args should not call DeltaTable again."""
    import dashboard.data_loader as dl
    monkeypatch.setattr(dl, "GOLD", tmp_path / "gold")

    mock_dt = MagicMock()
    mock_dt.to_pandas.return_value = pd.DataFrame({"x": [99]})

    with patch("dashboard.data_loader.DeltaTable", return_value=mock_dt) as mock_cls:
        dl.load_gold.cache_clear()
        dl.load_gold("some_table")
        dl.load_gold("some_table")   # second call — should be cached

    assert mock_cls.call_count == 1, "DeltaTable should only be called once (cached)"


def test_available_tables_empty_when_no_gold(tmp_path, monkeypatch):
    import dashboard.data_loader as dl
    monkeypatch.setattr(dl, "GOLD", tmp_path / "nonexistent")
    assert dl.available_tables() == []
