"""Tests for ingestion/utils/s3_utils.py."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from ingestion.utils.s3_utils import make_s3_client, object_exists, upload_file

BUCKET = "agro-lakehouse-bronze"
KEY = "source=indec/year=2024/month=03/file.csv"


def test_object_exists_true(s3_client):
    s3_client.put_object(Bucket=BUCKET, Key=KEY, Body=b"data")
    assert object_exists(s3_client, BUCKET, KEY) is True


def test_object_exists_false_on_404(s3_client):
    assert object_exists(s3_client, BUCKET, "missing/key.csv") is False


def test_object_exists_reraises_non_404(s3_client):
    error = ClientError(
        {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject"
    )
    with patch.object(s3_client, "head_object", side_effect=error):
        with pytest.raises(ClientError) as exc_info:
            object_exists(s3_client, BUCKET, KEY)
    assert exc_info.value.response["Error"]["Code"] == "403"


def test_upload_file_puts_object(s3_client, tmp_path):
    local = tmp_path / "file.csv"
    local.write_bytes(b"col1,col2\n1,2\n")
    upload_file(s3_client, local, BUCKET, KEY)
    body = s3_client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert body == b"col1,col2\n1,2\n"


def test_upload_file_attaches_metadata(s3_client, tmp_path):
    local = tmp_path / "file.csv"
    local.write_bytes(b"data")
    upload_file(s3_client, local, BUCKET, KEY, metadata={"source": "indec"})
    head = s3_client.head_object(Bucket=BUCKET, Key=KEY)
    assert head["Metadata"]["source"] == "indec"


def test_upload_file_no_metadata(s3_client, tmp_path):
    local = tmp_path / "file.csv"
    local.write_bytes(b"data")
    upload_file(s3_client, local, BUCKET, KEY, metadata=None)
    head = s3_client.head_object(Bucket=BUCKET, Key=KEY)
    assert head["Metadata"] == {}


def test_make_s3_client_returns_client(aws_credentials):
    with mock_aws():
        client = make_s3_client()
        assert hasattr(client, "upload_file")
