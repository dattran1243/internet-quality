"""Persistent completion manifests for Gold hourly windows."""

from __future__ import annotations

from typing import Any

from ingestion.ripe_atlas.models import (
    TimeWindow,
    utc_text,
)
from processing.silver.incremental import (
    validate_hour_aligned_window,
)
from storage.minio_client import put_json


def validate_gold_hour_window(
    window: TimeWindow,
) -> None:
    """Validate one exact hour-aligned Gold processing window."""

    validate_hour_aligned_window(
        window
    )

    duration_seconds = (
        window.stop
        - window.start
    ).total_seconds()

    if duration_seconds != 3600:
        raise ValueError(
            "Gold manifest requires exactly one hour: "
            f"got {duration_seconds} seconds"
        )


def gold_window_manifest_key(
    window: TimeWindow,
) -> str:
    """Return deterministic MinIO key for one completed Gold hour."""

    validate_gold_hour_window(
        window
    )

    start = window.start

    return (
        "gold/ripe_atlas/window_manifests/"
        f"year={start:%Y}/"
        f"month={start:%m}/"
        f"day={start:%d}/"
        f"hour={start:%H}/"
        "_SUCCESS.json"
    )


def gold_window_manifest_exists(
    client: Any,
    bucket: str,
    window: TimeWindow,
) -> bool:
    """Return True only when the deterministic Gold marker exists."""

    key = gold_window_manifest_key(
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


def write_gold_window_manifest(
    client: Any,
    bucket: str,
    window: TimeWindow,
    country_codes: list[str],
    measurement_ids: list[int] | None = None,
) -> str:
    """
    Mark one Gold hourly window complete.

    The object key is deterministic, so reruns replace the same marker.
    """

    validate_gold_hour_window(
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
            "Gold window manifest requires at least one country"
        )

    payload = {
        "status": "success",
        "source": "ripe_atlas",
        "dataset": "hourly_network_quality",
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

    key = gold_window_manifest_key(
        window
    )

    put_json(
        client,
        bucket,
        key,
        payload,
    )

    return key
