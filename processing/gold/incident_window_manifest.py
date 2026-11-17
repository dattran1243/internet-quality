"""Persistent completion manifests for global incident-candidate hours."""

from __future__ import annotations

from typing import Any

from ingestion.ripe_atlas.models import (
    TimeWindow,
    utc_text,
)
from processing.gold.window_manifest import (
    validate_gold_hour_window,
)
from storage.minio_client import put_json


def validate_incident_hour_window(
    window: TimeWindow,
) -> None:
    validate_gold_hour_window(window)


def incident_window_manifest_key(
    window: TimeWindow,
) -> str:
    validate_incident_hour_window(window)

    start = window.start

    return (
        "gold/ripe_atlas/incident_window_manifests/"
        f"year={start:%Y}/"
        f"month={start:%m}/"
        f"day={start:%d}/"
        f"hour={start:%H}/"
        "_SUCCESS.json"
    )


def incident_window_manifest_exists(
    client: Any,
    bucket: str,
    window: TimeWindow,
) -> bool:
    key = incident_window_manifest_key(window)

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


def write_incident_window_manifest(
    *,
    client: Any,
    bucket: str,
    window: TimeWindow,
    country_codes: list[str],
    measurement_ids: list[int],
    row_count: int,
    incident_candidate_count: int,
) -> str:
    validate_incident_hour_window(window)

    countries = sorted(
        {
            str(code).strip().upper()
            for code in country_codes
        }
    )

    if not countries:
        raise ValueError(
            "Incident manifest requires at least one country"
        )

    measurements = sorted(
        {
            int(value)
            for value in measurement_ids
        }
    )

    if not measurements:
        raise ValueError(
            "Incident manifest requires measurement IDs"
        )

    payload = {
        "status": "success",
        "source": "ripe_atlas",
        "dataset": "incident_candidates",
        "window_start_utc": utc_text(window.start),
        "window_stop_utc": utc_text(window.stop),
        "countries": countries,
        "country_count": len(countries),
        "measurement_ids": measurements,
        "row_count": int(row_count),
        "incident_candidate_count": int(
            incident_candidate_count
        ),
    }

    key = incident_window_manifest_key(window)

    put_json(
        client,
        bucket,
        key,
        payload,
    )

    return key
