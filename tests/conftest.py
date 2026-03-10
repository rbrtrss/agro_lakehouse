"""Shared fixtures for all ingestion tests."""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws


@pytest.fixture()
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")


@pytest.fixture()
def s3_client(aws_credentials):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="agro-lakehouse-bronze")
        yield client


@pytest.fixture()
def ckan_payload():
    """Factory: ckan_payload(formats) builds a CKAN API response."""

    def _make(formats=("CSV",)):
        resources = [
            {"format": f, "url": f"http://example.com/{f}.csv", "name": f}
            for f in formats
        ]
        return {"success": True, "result": {"resources": resources}}

    return _make


@pytest.fixture()
def wb_payload():
    """Factory: wb_payload(code, n) builds a World Bank API response."""

    def _make(code="AG.PRD.CREL.MT", n=2):
        rows = [
            {
                "country": {"value": "Argentina"},
                "countryiso3code": "ARG",
                "date": str(2023 - i),
                "value": float(100 + i),
                "indicator": {"id": code, "value": "Some indicator"},
            }
            for i in range(n)
        ]
        return [{}, rows]

    return _make
