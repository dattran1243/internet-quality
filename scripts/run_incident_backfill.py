#!/usr/bin/env python
"""Backfill completed Gold hours into global incident candidates."""

from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path
import subprocess
import sys
import time

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

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

RUNNER = (
    PROJECT_ROOT
    / "scripts"
    / "run_anomaly_incident_global_hour.py"
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill global RIPE Atlas "
            "anomaly/incident hourly windows."
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

    parser.add_argument(
        "--measurement-id",
        action="append",
        type=int,
        dest="measurement_ids",
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
                "Backfill range must contain "
                "whole UTC hours only"
            )

        result.append(hour)

        cursor = hour.stop

    return result


def main() -> None:
    started = time.monotonic()

    args = arguments()

    window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )

    validate_hour_aligned_window(
        window
    )

    windows = hour_windows(
        window
    )

    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    # ---------------------------------------------
    # Preflight the entire requested range first.
    # ---------------------------------------------

    ready = []
    complete = []
    blocked = []

    for hour in windows:
        if incident_window_manifest_exists(
            s3,
            settings.minio_bucket,
            hour,
        ):
            complete.append(hour)

        elif gold_window_manifest_exists(
            s3,
            settings.minio_bucket,
            hour,
        ):
            ready.append(hour)

        else:
            blocked.append(hour)

    print(
        "Incident backfill preflight "
        f"total={len(windows)} "
        f"ready={len(ready)} "
        f"complete={len(complete)} "
        f"blocked={len(blocked)}"
    )

    for hour in complete:
        print(
            "SKIP COMPLETE "
            f"{utc_text(hour.start)}"
            f"..{utc_text(hour.stop)}"
        )

    if blocked:
        missing = [
            utc_text(hour.start)
            for hour in blocked
        ]

        raise RuntimeError(
            "Incident backfill blocked because "
            "Gold manifest is missing for: "
            f"{missing}"
        )

    processed = 0

    for index, hour in enumerate(
        ready,
        start=1,
    ):
        print(
            "Incident backfill window start "
            f"position={index}/{len(ready)} "
            f"window={utc_text(hour.start)}"
            f"..{utc_text(hour.stop)}"
        )

        command = [
            sys.executable,
            str(RUNNER),
            "--start",
            utc_text(hour.start),
            "--stop",
            utc_text(hour.stop),
        ]

        for measurement_id in (
            args.measurement_ids
            or []
        ):
            command.extend(
                [
                    "--measurement-id",
                    str(measurement_id),
                ]
            )

        subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            check=True,
        )

        if not incident_window_manifest_exists(
            s3,
            settings.minio_bucket,
            hour,
        ):
            raise RuntimeError(
                "Incident runner completed but "
                "manifest is still missing for "
                f"{utc_text(hour.start)}"
            )

        processed += 1

        print(
            "Incident backfill window complete "
            f"window={utc_text(hour.start)}"
            f"..{utc_text(hour.stop)}"
        )

    print(
        "Incident backfill complete "
        f"processed={processed} "
        f"skipped_complete={len(complete)} "
        f"blocked=0 "
        f"duration_seconds="
        f"{time.monotonic() - started:.2f}"
    )


if __name__ == "__main__":
    main()
