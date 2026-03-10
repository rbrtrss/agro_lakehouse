"""Tests for ingestion/weather/ingest_weather.py."""

from __future__ import annotations

from datetime import date
from io import StringIO

import httpx
import pandas as pd
import pytest
import respx
from freezegun import freeze_time

from ingestion.weather.ingest_weather import (
    EXPECTED_COLUMNS,
    STATIONS,
    fetch_and_upload_province,
    fetch_province_weather,
    make_s3_key,
)

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"


# --- STATIONS constants ---


def test_stations_contains_all_provinces():
    expected = {
        "buenos_aires",
        "cordoba",
        "santa_fe",
        "entre_rios",
        "la_pampa",
        "salta",
        "tucuman",
    }
    assert set(STATIONS.keys()) == expected


def test_stations_have_lat_lon():
    for province, coords in STATIONS.items():
        assert "lat" in coords, f"Missing 'lat' for {province}"
        assert "lon" in coords, f"Missing 'lon' for {province}"


# --- make_s3_key ---


def test_make_s3_key_explicit_date():
    key = make_s3_key("cordoba", date(2024, 3, 5))
    assert "source=weather" in key
    assert "year=2024" in key
    assert "month=03" in key
    assert "weather_cordoba.csv" in key


def test_make_s3_key_zero_pads_month():
    key = make_s3_key("cordoba", date(2024, 1, 1))
    assert "month=01" in key


def test_make_s3_key_contains_province():
    key = make_s3_key("santa_fe", date(2024, 3, 5))
    assert "weather_santa_fe.csv" in key


@freeze_time("2026-03-10")
def test_make_s3_key_defaults_to_today():
    key = make_s3_key("cordoba")
    assert "year=2026" in key
    assert "month=03" in key


# --- fetch_province_weather ---


@respx.mock
async def test_fetch_province_weather_columns(weather_payload):
    respx.get(OPEN_METEO_URL).mock(
        return_value=httpx.Response(200, json=weather_payload())
    )
    async with httpx.AsyncClient() as client:
        df = await fetch_province_weather(
            client, "cordoba", -31.4, -64.2, date(2024, 1, 1), date(2024, 1, 3)
        )
    assert set(df.columns) == set(EXPECTED_COLUMNS)


@respx.mock
async def test_fetch_province_weather_row_count(weather_payload):
    respx.get(OPEN_METEO_URL).mock(
        return_value=httpx.Response(200, json=weather_payload(n=3))
    )
    async with httpx.AsyncClient() as client:
        df = await fetch_province_weather(
            client, "cordoba", -31.4, -64.2, date(2024, 1, 1), date(2024, 1, 3)
        )
    assert len(df) == 3


@respx.mock
async def test_fetch_province_weather_province_column(weather_payload):
    respx.get(OPEN_METEO_URL).mock(
        return_value=httpx.Response(200, json=weather_payload(province="cordoba"))
    )
    async with httpx.AsyncClient() as client:
        df = await fetch_province_weather(
            client, "cordoba", -31.4, -64.2, date(2024, 1, 1), date(2024, 1, 3)
        )
    assert (df["province"] == "cordoba").all()


@respx.mock
async def test_fetch_province_weather_coordinates(weather_payload):
    respx.get(OPEN_METEO_URL).mock(
        return_value=httpx.Response(200, json=weather_payload())
    )
    async with httpx.AsyncClient() as client:
        df = await fetch_province_weather(
            client, "cordoba", -31.4, -64.2, date(2024, 1, 1), date(2024, 1, 3)
        )
    assert (df["latitude"] == -31.4).all()
    assert (df["longitude"] == -64.2).all()


@respx.mock
async def test_fetch_province_weather_values(weather_payload):
    respx.get(OPEN_METEO_URL).mock(
        return_value=httpx.Response(200, json=weather_payload(n=1))
    )
    async with httpx.AsyncClient() as client:
        df = await fetch_province_weather(
            client, "cordoba", -31.4, -64.2, date(2024, 1, 1), date(2024, 1, 1)
        )
    assert df["temp_max_c"].iloc[0] == pytest.approx(30.0)
    assert df["temp_min_c"].iloc[0] == pytest.approx(15.0)
    assert df["precipitation_mm"].iloc[0] == pytest.approx(0.0)
    assert df["wind_speed_max_kmh"].iloc[0] == pytest.approx(20.0)
    assert df["evapotranspiration_mm"].iloc[0] == pytest.approx(4.5)


@respx.mock
async def test_fetch_province_weather_empty_on_missing_daily():
    respx.get(OPEN_METEO_URL).mock(
        return_value=httpx.Response(200, json={"latitude": -31.4, "longitude": -64.2})
    )
    async with httpx.AsyncClient() as client:
        df = await fetch_province_weather(
            client, "cordoba", -31.4, -64.2, date(2024, 1, 1), date(2024, 1, 3)
        )
    assert df.empty


@respx.mock
async def test_fetch_province_weather_empty_on_empty_time_series():
    respx.get(OPEN_METEO_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "daily": {
                    "time": [],
                    "temperature_2m_max": [],
                    "temperature_2m_min": [],
                    "precipitation_sum": [],
                    "wind_speed_10m_max": [],
                    "et0_fao_evapotranspiration": [],
                }
            },
        )
    )
    async with httpx.AsyncClient() as client:
        df = await fetch_province_weather(
            client, "cordoba", -31.4, -64.2, date(2024, 1, 1), date(2024, 1, 3)
        )
    assert df.empty


@respx.mock
async def test_fetch_province_weather_raises_on_http_error():
    respx.get(OPEN_METEO_URL).mock(return_value=httpx.Response(500))
    async with httpx.AsyncClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_province_weather(
                client, "cordoba", -31.4, -64.2, date(2024, 1, 1), date(2024, 1, 3)
            )


# --- fetch_and_upload_province ---


@respx.mock
async def test_fetch_and_upload_skips_existing(s3_client, monkeypatch):
    monkeypatch.setenv("BRONZE_BUCKET", "agro-lakehouse-bronze")
    key = make_s3_key("cordoba", date(2024, 1, 1))
    s3_client.put_object(Bucket="agro-lakehouse-bronze", Key=key, Body=b"existing")

    result = await fetch_and_upload_province(
        s3_client,
        "cordoba",
        -31.4,
        -64.2,
        date(2024, 1, 1),
        date(2024, 1, 3),
        date(2024, 1, 1),
    )
    assert result == "skipped"
    assert len(respx.calls) == 0


@respx.mock
async def test_fetch_and_upload_uploads_csv(s3_client, weather_payload, monkeypatch):
    monkeypatch.setenv("BRONZE_BUCKET", "agro-lakehouse-bronze")
    respx.get(OPEN_METEO_URL).mock(
        return_value=httpx.Response(200, json=weather_payload(n=3))
    )

    result = await fetch_and_upload_province(
        s3_client,
        "cordoba",
        -31.4,
        -64.2,
        date(2024, 1, 1),
        date(2024, 1, 3),
        date(2024, 1, 1),
    )
    assert result == "uploaded"

    key = make_s3_key("cordoba", date(2024, 1, 1))
    obj = s3_client.get_object(Bucket="agro-lakehouse-bronze", Key=key)
    df = pd.read_csv(StringIO(obj["Body"].read().decode()))
    assert len(df) == 3
    assert "province" in df.columns
