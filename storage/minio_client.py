"""Minimal S3-compatible storage operations for MinIO."""

import io
import json
from typing import Any

import boto3
from botocore.client import Config


def create_s3_client(endpoint: str, access_key: str, secret_key: str):
    """Create a path-style S3 client suitable for local MinIO."""
    return boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=access_key,
        aws_secret_access_key=secret_key, region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=5,
            read_timeout=20,
            retries={
                "mode": "standard",
                "max_attempts": 2,
            },
        ),
    )


def ensure_bucket(client: Any, bucket: str) -> None:
    """Create the configured bucket when absent."""
    try:
        client.head_bucket(Bucket=bucket)
    except client.exceptions.ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        client.create_bucket(Bucket=bucket)


def put_json(client: Any, bucket: str, key: str,
             payload: dict[str, Any]) -> None:
    """Serialize JSON deterministically and replace the object at key."""
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      sort_keys=True).encode("utf-8")
    client.upload_fileobj(io.BytesIO(body), bucket, key,
                          ExtraArgs={"ContentType": "application/json"})

