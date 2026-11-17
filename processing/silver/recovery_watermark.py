"""Persistent recovery watermark for RIPE Atlas Silver."""

from __future__ import annotations

import json
from typing import Any

from ingestion.ripe_atlas.models import (
    parse_utc,
    utc_text,
)
from storage.minio_client import put_json


WATERMARK_KEY = (
    "silver/ripe_atlas/recovery/"
    "_WATERMARK.json"
)


def validate_five_minute_boundary(
    value,
) -> None:
    if (
        value.second != 0
        or value.microsecond != 0
        or value.minute % 5 != 0
    ):
        raise ValueError(
            "Silver recovery watermark must align "
            "to a five-minute UTC boundary"
        )


def read_silver_recovery_watermark(
    client: Any,
    bucket: str,
):
    """Return verified-through UTC boundary, or None when uninitialized."""

    try:
        response = client.get_object(
            Bucket=bucket,
            Key=WATERMARK_KEY,
        )

    except client.exceptions.ClientError as exc:
        code = str(
            exc.response
            .get("Error", {})
            .get("Code", "")
        )

        if code in {
            "404",
            "NoSuchKey",
            "NotFound",
        }:
            return None

        raise

    payload = json.loads(
        response["Body"].read()
    )

    value = parse_utc(
        payload["verified_through_utc"]
    )

    validate_five_minute_boundary(
        value
    )

    return value


def write_silver_recovery_watermark(
    client: Any,
    bucket: str,
    verified_through,
) -> str:
    """Persist the latest continuously verified Silver boundary."""

    validate_five_minute_boundary(
        verified_through
    )

    put_json(
        client,
        bucket,
        WATERMARK_KEY,
        {
            "source": "ripe_atlas",
            "dataset": "ping",
            "status": "verified",
            "verified_through_utc": utc_text(
                verified_through
            ),
        },
    )

    return WATERMARK_KEY
