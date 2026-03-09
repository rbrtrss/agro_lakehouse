"""Shared S3 helpers for Bronze ingestion scripts."""

from __future__ import annotations

from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def make_s3_client():
    """Return a boto3 S3 client using the env/~/.aws credential chain."""
    return boto3.client("s3")


def object_exists(s3_client, bucket: str, key: str) -> bool:
    """Return True if *key* exists in *bucket*, False on 404.

    Re-raises any ClientError that is not a 404.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "404":
            return False
        raise


def upload_file(
    s3_client,
    local_path: Path,
    bucket: str,
    key: str,
    metadata: dict[str, str] | None = None,
) -> None:
    """Upload *local_path* to s3://*bucket*/*key* with optional object metadata."""
    s3_client.upload_file(
        Filename=str(local_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs={"Metadata": metadata or {}},
    )
