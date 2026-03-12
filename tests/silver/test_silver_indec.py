"""
TDD tests for the transform_indec_df helper in glue/jobs/silver_indec.py.

These tests run without AWS or Spark — pure pandas only.

Input contract: post-unpivot long DataFrame with columns:
  indice_tiempo, province, country, raw_value
"""

import pandas as pd

from glue.jobs.silver_indec import transform_indec_df

EXPECTED_COLUMNS = ["year", "province", "country", "fob_usd"]


def _make_df(**overrides) -> pd.DataFrame:
    """Return a minimal valid long-format INDEC DataFrame."""
    base = {
        "indice_tiempo": ["2020-01-01", "2021-06-01"],
        "province": ["buenos_aires", "cordoba"],
        "country": ["brasil", "china"],
        "raw_value": ["1500.50", "2000.00"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_output_columns():
    df = transform_indec_df(_make_df())
    assert list(df.columns) == EXPECTED_COLUMNS


def test_fob_usd_null_dropped():
    raw = _make_df(raw_value=["not_a_number", "2000.00"])
    df = transform_indec_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["country"] == "china"


def test_fob_usd_zero_dropped():
    raw = _make_df(raw_value=["0", "2000.00"])
    df = transform_indec_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["country"] == "china"


def test_fob_usd_negative_dropped():
    raw = _make_df(raw_value=["-100.0", "2000.00"])
    df = transform_indec_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["country"] == "china"


def test_province_null_dropped():
    raw = _make_df(province=[None, "cordoba"])
    df = transform_indec_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["province"] == "cordoba"


def test_province_empty_dropped():
    raw = _make_df(province=["   ", "cordoba"])
    df = transform_indec_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["province"] == "cordoba"


def test_country_null_dropped():
    raw = _make_df(country=[None, "china"])
    df = transform_indec_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["country"] == "china"


def test_year_too_low_dropped():
    raw = _make_df(indice_tiempo=["1989-12-01", "2020-01-01"])
    df = transform_indec_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["year"] == 2020


def test_year_too_high_dropped():
    raw = _make_df(indice_tiempo=["2031-01-01", "2020-01-01"])
    df = transform_indec_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["year"] == 2020


def test_year_derived_correctly():
    df = transform_indec_df(_make_df())
    assert df.iloc[0]["year"] == 2020
    assert df.iloc[1]["year"] == 2021


def test_fob_usd_type():
    df = transform_indec_df(_make_df())
    assert pd.api.types.is_float_dtype(df["fob_usd"])


def test_empty_input():
    empty = pd.DataFrame(columns=["indice_tiempo", "province", "country", "raw_value"])
    df = transform_indec_df(empty)
    assert df.empty
    assert list(df.columns) == EXPECTED_COLUMNS
