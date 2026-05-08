"""Test that schema definitions are valid and have expected fields."""
from pyspark.sql.types import StringType, LongType

from bigdata_music.schemas import (
    CHARTS_RAW_SCHEMA,
    TRACK_FEATURES_RAW_SCHEMA,
    COUNTRIES_RAW_SCHEMA,
    BOUNDED_AUDIO_FEATURES,
)


def test_charts_schema_has_required_columns():
    names = {f.name for f in CHARTS_RAW_SCHEMA.fields}
    assert {"title", "rank", "date", "artist", "url", "region", "chart", "streams"} <= names


def test_charts_rank_is_string():
    field = next(f for f in CHARTS_RAW_SCHEMA.fields if f.name == "rank")
    assert isinstance(field.dataType, StringType), "rank must be StringType at Bronze (cast at Silver)"


def test_track_features_track_id_not_nullable():
    field = next(f for f in TRACK_FEATURES_RAW_SCHEMA.fields if f.name == "track_id")
    assert not field.nullable, "track_id is our join key — must be non-nullable"


def test_countries_schema_has_five_columns():
    assert len(COUNTRIES_RAW_SCHEMA.fields) == 5


def test_bounded_audio_features_includes_valence_and_energy():
    assert "valence" in BOUNDED_AUDIO_FEATURES
    assert "energy" in BOUNDED_AUDIO_FEATURES
