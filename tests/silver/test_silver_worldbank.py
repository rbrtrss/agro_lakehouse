"""
TDD tests for the transform_worldbank_df helper in glue/jobs/silver_worldbank.py.

These tests run without AWS or Spark — pure pandas only.

Input contract: raw long CSV DataFrame with columns:
  country, country_code, indicator, indicator_code, year, value
"""

import pandas as pd
import pytest

from glue.jobs.silver_worldbank import transform_worldbank_df

EXPECTED_COLUMNS = [
    "country",
    "country_code",
    "indicator",
    "indicator_code",
    "year",
    "value",
]


def _make_df(**overrides) -> pd.DataFrame:
    """Return a minimal valid World Bank long-format DataFrame."""
    base = {
        "country": ["Argentina", "Brazil"],
        "country_code": ["ARG", "BRA"],
        "indicator": ["Soybean Price", "Soybean Price"],
        "indicator_code": ["PSOYB", "PSOYB"],
        "year": ["2020", "2021"],
        "value": ["350.50", "420.00"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_output_columns():
    df = transform_worldbank_df(_make_df())
    assert list(df.columns) == EXPECTED_COLUMNS


def test_value_null_dropped():
    raw = _make_df(value=["not_a_number", "420.00"])
    df = transform_worldbank_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["country_code"] == "BRA"


def test_value_zero_dropped():
    raw = _make_df(value=["0", "420.00"])
    df = transform_worldbank_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["country_code"] == "BRA"


def test_value_negative_dropped():
    raw = _make_df(value=["-10.0", "420.00"])
    df = transform_worldbank_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["country_code"] == "BRA"


def test_country_code_null_dropped():
    raw = _make_df(country_code=[None, "BRA"])
    df = transform_worldbank_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["country_code"] == "BRA"


def test_country_code_empty_dropped():
    raw = _make_df(country_code=["   ", "BRA"])
    df = transform_worldbank_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["country_code"] == "BRA"


def test_indicator_code_null_dropped():
    raw = _make_df(indicator_code=[None, "PSOYB"])
    df = transform_worldbank_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["country_code"] == "BRA"


def test_year_too_low_dropped():
    raw = _make_df(year=["1959", "2020"])
    df = transform_worldbank_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["year"] == 2020


def test_year_too_high_dropped():
    raw = _make_df(year=["2031", "2020"])
    df = transform_worldbank_df(raw)
    assert len(df) == 1
    assert df.iloc[0]["year"] == 2020


def test_dedup_keeps_first():
    raw = pd.DataFrame(
        {
            "country": ["Argentina", "Argentina", "Brazil"],
            "country_code": ["ARG", "ARG", "BRA"],
            "indicator": ["Soybean Price", "Soybean Price", "Soybean Price"],
            "indicator_code": ["PSOYB", "PSOYB", "PSOYB"],
            "year": ["2020", "2020", "2021"],
            "value": ["350.50", "999.99", "420.00"],
        }
    )
    df = transform_worldbank_df(raw)
    assert len(df) == 2
    arg_row = df[df["country_code"] == "ARG"].iloc[0]
    assert arg_row["value"] == pytest.approx(350.50)


def test_value_type():
    df = transform_worldbank_df(_make_df())
    assert pd.api.types.is_float_dtype(df["value"])


def test_year_type():
    df = transform_worldbank_df(_make_df())
    assert pd.api.types.is_integer_dtype(df["year"])


def test_empty_input():
    empty = pd.DataFrame(
        columns=[
            "country",
            "country_code",
            "indicator",
            "indicator_code",
            "year",
            "value",
        ]
    )
    df = transform_worldbank_df(empty)
    assert df.empty
    assert list(df.columns) == EXPECTED_COLUMNS
