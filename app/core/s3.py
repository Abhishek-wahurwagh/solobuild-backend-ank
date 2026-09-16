"""
S3 helper utilities — wraps ``boto3`` for resume batch uploads/downloads.

All operations are synchronous (boto3 is sync) and designed to be called
from the ARQ worker or wrapped in ``asyncio.to_thread`` when needed.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO

import boto3
from botocore.config import Config as BotoConfig

from app.core.config import settings

# Reuse a module-level client — safe for multi-threaded workers.
_client = None


def get_s3_client():
    """Return a reusable boto3 S3 client."""
    global _client
    if _client is None:
        _client = boto3.client(
            "s3",
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION,
            config=BotoConfig(
                retries={"max_attempts": 3, "mode": "adaptive"},
                max_pool_connections=25,
            ),
        )
    return _client


def upload_fileobj_to_s3(
    key: str,
    file_obj: BinaryIO,
    *,
    bucket: str | None = None,
    content_type: str = "application/octet-stream",
) -> str:
    """Stream *file_obj* into S3 under *key*.  Returns the full S3 URI."""
    bucket = bucket or settings.AWS_BUCKET_NAME
    client = get_s3_client()
    client.upload_fileobj(
        file_obj,
        bucket,
        key,
        ExtraArgs={"ContentType": content_type},
    )
    return f"s3://{bucket}/{key}"


def upload_bytes_to_s3(
    key: str,
    data: bytes,
    *,
    bucket: str | None = None,
    content_type: str = "application/octet-stream",
) -> str:
    """Upload raw bytes to S3.  Returns the full S3 URI."""
    bucket = bucket or settings.AWS_BUCKET_NAME
    client = get_s3_client()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType=content_type,
    )
    return f"s3://{bucket}/{key}"


def download_s3_prefix(
    prefix: str,
    local_dir: Path,
    *,
    bucket: str | None = None,
) -> list[Path]:
    """Download every object under *prefix* into *local_dir*.

    Returns the list of local file paths that were written.
    """
    bucket = bucket or settings.AWS_BUCKET_NAME
    client = get_s3_client()

    paginator = client.get_paginator("list_objects_v2")
    downloaded: list[Path] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            # Skip "directory" markers
            if key.endswith("/"):
                continue
            relative = key[len(prefix):].lstrip("/")
            if not relative:
                continue
            dest = local_dir / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, key, str(dest))
            downloaded.append(dest)

    return downloaded


def download_s3_object(
    key: str,
    dest: Path,
    *,
    bucket: str | None = None,
) -> Path:
    """Download a single S3 object to a local path."""
    bucket = bucket or settings.AWS_BUCKET_NAME
    client = get_s3_client()
    dest.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(bucket, key, str(dest))
    return dest
