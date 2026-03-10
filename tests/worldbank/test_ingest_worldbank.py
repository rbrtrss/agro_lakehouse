"""Tests for ingestion/worldbank/ingest_worldbank.py."""

from __future__ import annotations

from datetime import date

import httpx
import pytest
import respx
from freezegun import freeze_time

from ingestion.worldbank.ingest_worldbank import FILENAME, fetch_indicator, make_s3_key

WB_BASE_PATTERN = "https://api.worldbank.org/v2/country/ARG/indicator/"
EXPECTED_COLUMNS = {
    "country",
    "country_code",
    "indicator",
    "indicator_code",
    "year",
    "value",
}


# --- make_s3_key ---


def test_make_s3_key_explicit_date():
    key = make_s3_key(date(2024, 3, 5))
    assert key.endswith(FILENAME)
    assert "source=worldbank" in key
    assert "year=2024" in key
    assert "month=03" in key


def test_make_s3_key_zero_pads_month():
    key = make_s3_key(date(2024, 1, 1))
    assert "month=01" in key


@freeze_time("2026-03-10")
def test_make_s3_key_defaults_to_today():
    key = make_s3_key()
    assert "year=2026" in key
    assert "month=03" in key


# --- fetch_indicator ---


@respx.mock
async def test_fetch_indicator_columns(wb_payload):
    code = "AG.PRD.CREL.MT"
    respx.get(url__startswith=WB_BASE_PATTERN).mock(
        return_value=httpx.Response(200, json=wb_payload(code=code))
    )
    async with httpx.AsyncClient() as client:
        df = await fetch_indicator(client, "cereal_production_mt", code)
    assert set(df.columns) == EXPECTED_COLUMNS


@respx.mock
async def test_fetch_indicator_country_code(wb_payload):
    code = "AG.PRD.CREL.MT"
    respx.get(url__startswith=WB_BASE_PATTERN).mock(
        return_value=httpx.Response(200, json=wb_payload(code=code))
    )
    async with httpx.AsyncClient() as client:
        df = await fetch_indicator(client, "cereal_production_mt", code)
    assert df["country_code"].iloc[0] == "ARG"


@respx.mock
async def test_fetch_indicator_name_and_code(wb_payload):
    code = "AG.PRD.CREL.MT"
    name = "cereal_production_mt"
    respx.get(url__startswith=WB_BASE_PATTERN).mock(
        return_value=httpx.Response(200, json=wb_payload(code=code))
    )
    async with httpx.AsyncClient() as client:
        df = await fetch_indicator(client, name, code)
    assert (df["indicator"] == name).all()
    assert (df["indicator_code"] == code).all()


@respx.mock
async def test_fetch_indicator_empty_on_empty_payload():
    respx.get(url__startswith=WB_BASE_PATTERN).mock(
        return_value=httpx.Response(200, json=[{}, []])
    )
    async with httpx.AsyncClient() as client:
        df = await fetch_indicator(client, "some_name", "AG.PRD.CREL.MT")
    assert df.empty


@respx.mock
async def test_fetch_indicator_empty_on_malformed_payload():
    respx.get(url__startswith=WB_BASE_PATTERN).mock(
        return_value=httpx.Response(200, json={"error": "bad"})
    )
    async with httpx.AsyncClient() as client:
        df = await fetch_indicator(client, "some_name", "AG.PRD.CREL.MT")
    assert df.empty


@respx.mock
async def test_fetch_indicator_raises_on_http_error():
    respx.get(url__startswith=WB_BASE_PATTERN).mock(return_value=httpx.Response(500))
    async with httpx.AsyncClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_indicator(client, "some_name", "AG.PRD.CREL.MT")
