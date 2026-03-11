"""
TDD tests for the transform_weather_df helper in glue/jobs/silver_weather.py.

These tests run without AWS or Spark — pure pandas only.
"""

import pandas as pd
import pytest

from glue.jobs.silver_weather import transform_weather_df

EXPECTED_COLUMNS = [
    "date",
    "province",
    "latitude",
    "longitude",
    "temp_max_c",
    "temp_min_c",
    "precipitation_mm",
    "wind_speed_max_kmh",
    "evapotranspiration_mm",
    "year",
    "month",
]


def _make_df(**overrides) -> pd.DataFrame:
    """Return a minimal valid raw-Bronze-style DataFrame."""
    base = {
        "date": ["2024-01-15", "2024-02-20"],
        "province": ["buenos_aires", "cordoba"],
        "latitude": ["-34.6037", "-31.4135"],
        "longitude": ["-58.3816", "-64.1811"],
        "temp_max_c": ["32.5", "28.1"],
        "temp_min_c": ["18.2", "14.7"],
        "precipitation_mm": ["5.0", "0.0"],
        "wind_speed_max_kmh": ["45.3", "30.0"],
        "evapotranspiration_mm": ["6.1", "4.8"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_output_columns():
    df = transform_weather_df(_make_df())
    assert list(df.columns) == EXPECTED_COLUMNS


def test_types_cast():
    df = transform_weather_df(_make_df())
    for col in (
        "latitude",
        "longitude",
        "temp_max_c",
        "temp_min_c",
        "precipitation_mm",
        "wind_speed_max_kmh",
        "evapotranspiration_mm",
    ):
        assert pd.api.types.is_float_dtype(df[col]), f"{col} should be float"
    assert pd.api.types.is_integer_dtype(df["year"])
    assert pd.api.types.is_integer_dtype(df["month"])


def test_deduplication():
    raw = _make_df(
        date=["2024-01-15", "2024-01-15", "2024-02-20"],
        province=["buenos_aires", "buenos_aires", "cordoba"],
        temp_max_c=["32.5", "99.9", "28.1"],
        latitude=["-34.6037", "-34.6037", "-31.4135"],
        longitude=["-58.3816", "-58.3816", "-64.1811"],
        temp_min_c=["18.2", "18.2", "14.7"],
        precipitation_mm=["5.0", "5.0", "0.0"],
        wind_speed_max_kmh=["45.3", "45.3", "30.0"],
        evapotranspiration_mm=["6.1", "6.1", "4.8"],
    )
    df = transform_weather_df(raw)
    assert len(df) == 2
    # First occurrence kept — temp_max_c should be 32.5, not 99.9
    ba_row = df[df["province"] == "buenos_aires"].iloc[0]
    assert ba_row["temp_max_c"] == pytest.approx(32.5)


def test_nulls_dropped():
    raw = _make_df(
        date=["2024-01-15", None, "2024-02-20"],
        province=["buenos_aires", "cordoba", None],
        latitude=["-34.6037", "-31.4135", "-38.0"],
        longitude=["-58.3816", "-64.1811", "-65.0"],
        temp_max_c=["32.5", "28.1", "20.0"],
        temp_min_c=["18.2", "14.7", "10.0"],
        precipitation_mm=["5.0", "0.0", "1.0"],
        wind_speed_max_kmh=["45.3", "30.0", "20.0"],
        evapotranspiration_mm=["6.1", "4.8", "3.0"],
    )
    df = transform_weather_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["province"] == "buenos_aires"


def test_year_month_derived():
    df = transform_weather_df(_make_df())
    row0 = df[df["province"] == "buenos_aires"].iloc[0]
    assert row0["year"] == 2024
    assert row0["month"] == 1

    row1 = df[df["province"] == "cordoba"].iloc[0]
    assert row1["year"] == 2024
    assert row1["month"] == 2


def test_empty_input():
    empty = pd.DataFrame(
        columns=[
            "date",
            "province",
            "latitude",
            "longitude",
            "temp_max_c",
            "temp_min_c",
            "precipitation_mm",
            "wind_speed_max_kmh",
            "evapotranspiration_mm",
        ]
    )
    df = transform_weather_df(empty)
    assert df.empty
    assert list(df.columns) == EXPECTED_COLUMNS
