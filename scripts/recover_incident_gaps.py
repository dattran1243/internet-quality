#!/usr/bin/env python
"""Safely recover missing global RIPE Atlas incident-candidate hours."""

from __future__ import annotations

import argparse
from datetime import (
    datetime,
    timedelta,
    timezone,
)
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

BACKFILL_SCRIPT = (
    PROJECT_ROOT
    / "scripts"
    / "run_incident_backfill.py"
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Detect and recover bounded missing "
            "global incident-candidate hours."
        )
    )

    parser.add_argument(
        "--start",
    )

    parser.add_argument(
        "--stop",
    )

    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=24,
    )

    parser.add_argument(
        "--max-windows",
        type=int,
        default=3,
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    return parser.parse_args()


def floor_utc_hour(
    value: datetime,
) -> datetime:
    return value.astimezone(
        timezone.utc
    ).replace(
        minute=0,
        second=0,
        microsecond=0,
    )


def resolve_window(
    args: argparse.Namespace,
) -> TimeWindow:
    has_start = bool(args.start)
    has_stop = bool(args.stop)

    if has_start != has_stop:
        raise ValueError(
            "Manual recovery requires both "
            "--start and --stop"
        )

    if has_start:
        window = TimeWindow(
            parse_utc(args.start),
            parse_utc(args.stop),
        )

        validate_hour_aligned_window(
            window
        )

        if window.start >= window.stop:
            raise ValueError(
                "start must be before stop"
            )

        return window

    if args.lookback_hours <= 0:
        raise ValueError(
            "lookback-hours must be positive"
        )

    stop = floor_utc_hour(
        datetime.now(
            timezone.utc
        )
    )

    start = (
        stop
        - timedelta(
            hours=args.lookback_hours
        )
    )

    return TimeWindow(
        start,
        stop,
    )


def hour_windows(
    window: TimeWindow,
) -> list[TimeWindow]:
    result = []

    cursor = window.start

    while cursor < window.stop:
        hour = TimeWindow(
            cursor,
            cursor + ONE_HOUR,
        )

        if hour.stop > window.stop:
            raise ValueError(
                "Recovery window must contain "
                "whole UTC hours only"
            )

        result.append(hour)

        cursor = hour.stop

    return result


def main() -> None:
    started = time.monotonic()

    args = arguments()

    if args.max_windows <= 0:
        raise ValueError(
            "max-windows must be positive"
        )

    scan_window = resolve_window(
        args
    )

    windows = hour_windows(
        scan_window
    )

    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    complete = []
    ready = []
    blocked = []

    print(
        "Incident recovery scan "
        f"window={utc_text(scan_window.start)}"
        f"..{utc_text(scan_window.stop)} "
        f"hours={len(windows)} "
        f"max_windows={args.max_windows} "
        f"dry_run={args.dry_run}"
    )

    for hour in windows:
        if incident_window_manifest_exists(
            s3,
            settings.minio_bucket,
            hour,
        ):
            status = "COMPLETE"
            complete.append(hour)

        elif gold_window_manifest_exists(
            s3,
            settings.minio_bucket,
            hour,
        ):
            status = "READY"
            ready.append(hour)

        else:
            status = "BLOCKED"
            blocked.append(hour)

        print(
            "Incident recovery gap "
            f"window={utc_text(hour.start)}"
            f"..{utc_text(hour.stop)} "
            f"status={status}"
        )

    selected = (
        ready[
            :args.max_windows
        ]
    )

    deferred = (
        ready[
            args.max_windows:
        ]
    )

    print(
        "Incident recovery selection "
        f"complete={len(complete)} "
        f"ready={len(ready)} "
        f"blocked={len(blocked)} "
        f"selected={len(selected)} "
        f"deferred={len(deferred)}"
    )

    for hour in selected:
        print(
            "SELECTED "
            f"{utc_text(hour.start)}"
            f"..{utc_text(hour.stop)}"
        )

    for hour in deferred:
        print(
            "DEFERRED "
            f"{utc_text(hour.start)}"
            f"..{utc_text(hour.stop)}"
        )

    if args.dry_run:
        print(
            "Incident recovery dry-run complete "
            f"selected={len(selected)} "
            f"blocked={len(blocked)} "
            f"deferred={len(deferred)}"
        )
        return

    recovered = 0

    for index, hour in enumerate(
        selected,
        start=1,
    ):
        print(
            "Incident recovery start "
            f"position={index}/{len(selected)} "
            f"window={utc_text(hour.start)}"
            f"..{utc_text(hour.stop)}"
        )

        command = [
            sys.executable,
            str(BACKFILL_SCRIPT),
            "--start",
            utc_text(hour.start),
            "--stop",
            utc_text(hour.stop),
        ]

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
                "Incident recovery completed but "
                "manifest is still missing for "
                f"{utc_text(hour.start)}"
            )

        recovered += 1

        print(
            "Incident recovery complete "
            f"window={utc_text(hour.start)}"
            f"..{utc_text(hour.stop)}"
        )

    print(
        "Incident recovery complete "
        f"recovered={recovered} "
        f"blocked={len(blocked)} "
        f"deferred={len(deferred)} "
        f"duration_seconds="
        f"{time.monotonic() - started:.2f}"
    )


if __name__ == "__main__":
    main()
