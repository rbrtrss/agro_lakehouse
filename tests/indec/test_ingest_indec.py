"""Tests for ingestion/indec/ingest_indec.py."""

from __future__ import annotations

from datetime import date

import httpx
import pytest
import respx
from freezegun import freeze_time

from ingestion.indec.ingest_indec import fetch_csv_resource, make_s3_key

CKAN_URL = "https://datos.gob.ar/api/3/action/package_show"


# --- make_s3_key ---


def test_make_s3_key_explicit_date():
    key = make_s3_key("f.csv", date(2024, 3, 5))
    assert key == "source=indec/year=2024/month=03/f.csv"


def test_make_s3_key_zero_pads_month():
    key = make_s3_key("f.csv", date(2024, 1, 1))
    assert "month=01" in key


@freeze_time("2026-03-10")
def test_make_s3_key_defaults_to_today():
    key = make_s3_key("f.csv")
    assert "year=2026" in key
    assert "month=03" in key


# --- fetch_csv_resource ---


@respx.mock
async def test_fetch_csv_resource_returns_first_csv(ckan_payload):
    respx.get(CKAN_URL).mock(
        return_value=httpx.Response(200, json=ckan_payload(("CSV", "XLS")))
    )
    async with httpx.AsyncClient() as client:
        resource = await fetch_csv_resource(client)
    assert resource["format"] == "CSV"


@respx.mock
async def test_fetch_csv_resource_exits_on_api_failure():
    respx.get(CKAN_URL).mock(
        return_value=httpx.Response(
            200, json={"success": False, "error": {"message": "Not found"}}
        )
    )
    async with httpx.AsyncClient() as client:
        with pytest.raises(SystemExit) as exc_info:
            await fetch_csv_resource(client)
    assert exc_info.value.code == 1


@respx.mock
async def test_fetch_csv_resource_exits_when_no_csv(ckan_payload):
    respx.get(CKAN_URL).mock(
        return_value=httpx.Response(200, json=ckan_payload(("XLS", "PDF")))
    )
    async with httpx.AsyncClient() as client:
        with pytest.raises(SystemExit) as exc_info:
            await fetch_csv_resource(client)
    assert exc_info.value.code == 1


@respx.mock
async def test_fetch_csv_resource_raises_on_http_error():
    respx.get(CKAN_URL).mock(return_value=httpx.Response(500))
    async with httpx.AsyncClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_csv_resource(client)
