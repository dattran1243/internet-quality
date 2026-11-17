"""Persistent completion manifests for Silver five-minute windows."""

from __future__ import annotations

from typing import Any

from ingestion.ripe_atlas.models import (
    TimeWindow,
    utc_text,
)
from processing.silver.incremental import (
    validate_five_minute_window,
)
from storage.minio_client import put_json


def silver_window_manifest_key(
    window: TimeWindow,
) -> str:
    """Return deterministic MinIO key for one completed Silver window."""

    validate_five_minute_window(
        window
    )

    start = window.start

    return (
        "silver/ripe_atlas/window_manifests/"
        f"year={start:%Y}/"
        f"month={start:%m}/"
        f"day={start:%d}/"
        f"hour={start:%H}/"
        f"minute={start:%M}/"
        "_SUCCESS.json"
    )


def silver_window_manifest_exists(
    client: Any,
    bucket: str,
    window: TimeWindow,
) -> bool:
    """Return True only when the deterministic completion marker exists."""

    key = silver_window_manifest_key(
        window
    )

    try:
        client.head_object(
            Bucket=bucket,
            Key=key,
        )
        return True

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
            return False

        raise


def write_silver_window_manifest(
    client: Any,
    bucket: str,
    window: TimeWindow,
    country_codes: list[str],
    measurement_ids: list[int] | None = None,
) -> str:
    """
    Mark one Silver five-minute window complete.

    The object key is deterministic, so reruns replace the same marker.
    """

    validate_five_minute_window(
        window
    )

    countries = sorted(
        {
            str(code).upper()
            for code in country_codes
        }
    )

    if not countries:
        raise ValueError(
            "Silver window manifest requires at least one country"
        )

    payload = {
        "status": "success",
        "source": "ripe_atlas",
        "dataset": "ping",
        "window_start_utc": utc_text(
            window.start
        ),
        "window_stop_utc": utc_text(
            window.stop
        ),
        "countries": countries,
        "country_count": len(
            countries
        ),
    }

    if measurement_ids is not None:
        payload["measurement_ids"] = sorted(
            {
                int(value)
                for value in measurement_ids
            }
        )

    key = silver_window_manifest_key(
        window
    )

    put_json(
        client,
        bucket,
        key,
        payload,
    )

    return key
