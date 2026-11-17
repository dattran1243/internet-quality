#!/usr/bin/env python
"""Detect missing global RIPE Atlas incident-candidate hours."""

from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path
import sys

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)

from configs.settings import settings
from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
    utc_text,
)
from processing.gold.incident_window_manifest import (
    incident_window_manifest_exists,
)
from processing.gold.window_manifest import (
    gold_window_manifest_exists,
)
from processing.silver.incremental import (
    validate_hour_aligned_window,
)
from storage.minio_client import create_s3_client


ONE_HOUR = timedelta(hours=1)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Classify global incident hours as "
            "COMPLETE, READY, or BLOCKED."
        )
    )

    parser.add_argument(
        "--start",
        required=True,
    )

    parser.add_argument(
        "--stop",
        required=True,
    )

    return parser.parse_args()


def hour_windows(
    window: TimeWindow,
) -> list[TimeWindow]:
    if window.start >= window.stop:
        raise ValueError(
            "start must be before stop"
        )

    result = []

    cursor = window.start

    while cursor < window.stop:
        hour = TimeWindow(
            cursor,
            cursor + ONE_HOUR,
        )

        if hour.stop > window.stop:
            raise ValueError(
                "Detection range must contain "
                "whole UTC hours only"
            )

        result.append(hour)

        cursor = hour.stop

    return result


def main() -> None:
    args = arguments()

    window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )

    validate_hour_aligned_window(
        window
    )

    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    complete = 0
    ready = 0
    blocked = 0

    for hour in hour_windows(window):
        if incident_window_manifest_exists(
            s3,
            settings.minio_bucket,
            hour,
        ):
            status = "COMPLETE"
            reason = (
                "incident_manifest_exists"
            )
            complete += 1

        elif gold_window_manifest_exists(
            s3,
            settings.minio_bucket,
            hour,
        ):
            status = "READY"
            reason = (
                "gold_complete_incident_missing"
            )
            ready += 1

        else:
            status = "BLOCKED"
            reason = (
                "gold_manifest_missing"
            )
            blocked += 1

        print(
            "Incident gap "
            f"window={utc_text(hour.start)}"
            f"..{utc_text(hour.stop)} "
            f"status={status} "
            f"reason={reason}"
        )

    total = (
        complete
        + ready
        + blocked
    )

    print(
        "Incident gap summary "
        f"total={total} "
        f"complete={complete} "
        f"ready={ready} "
        f"blocked={blocked}"
    )


if __name__ == "__main__":
    main()
