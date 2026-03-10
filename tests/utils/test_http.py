"""Tests for ingestion/utils/http.py."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from ingestion.utils.http import download_file


@respx.mock
async def test_download_success_writes_content(tmp_path):
    url = "http://test.example.com/file.csv"
    dest = tmp_path / "file.csv"
    respx.get(url).mock(return_value=httpx.Response(200, content=b"a,b\n1,2\n"))
    await download_file(url, dest)
    assert dest.read_bytes() == b"a,b\n1,2\n"


@respx.mock
async def test_download_returns_path(tmp_path):
    url = "http://test.example.com/file.csv"
    dest = tmp_path / "file.csv"
    respx.get(url).mock(return_value=httpx.Response(200, content=b"data"))
    result = await download_file(url, dest)
    assert result == dest


@respx.mock
async def test_download_creates_parent_dirs(tmp_path):
    url = "http://test.example.com/file.csv"
    dest = tmp_path / "nested" / "deep" / "file.csv"
    respx.get(url).mock(return_value=httpx.Response(200, content=b"data"))
    await download_file(url, dest)
    assert dest.exists()


@respx.mock
async def test_download_retries_then_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr("ingestion.utils.http.asyncio.sleep", AsyncMock())
    url = "http://test.example.com/file.csv"
    dest = tmp_path / "file.csv"
    route = respx.get(url)
    route.side_effect = [
        httpx.ConnectError("fail"),
        httpx.ConnectError("fail"),
        httpx.Response(200, content=b"ok"),
    ]
    await download_file(url, dest)
    assert dest.read_bytes() == b"ok"


@respx.mock
async def test_download_raises_after_max_retries(tmp_path, monkeypatch):
    monkeypatch.setattr("ingestion.utils.http.asyncio.sleep", AsyncMock())
    url = "http://test.example.com/file.csv"
    dest = tmp_path / "file.csv"
    respx.get(url).mock(side_effect=httpx.ConnectError("always fails"))
    with pytest.raises(RuntimeError, match="Failed to download"):
        await download_file(url, dest)


@respx.mock
async def test_download_4xx_raises_runtime_error(tmp_path, monkeypatch):
    monkeypatch.setattr("ingestion.utils.http.asyncio.sleep", AsyncMock())
    url = "http://test.example.com/file.csv"
    dest = tmp_path / "file.csv"
    respx.get(url).mock(return_value=httpx.Response(404))
    with pytest.raises(RuntimeError, match="Failed to download"):
        await download_file(url, dest)
