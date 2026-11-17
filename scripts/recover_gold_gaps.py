#!/usr/bin/env python
"""Recover missing RIPE Atlas Gold hourly windows safely."""

from __future__ import annotations

import argparse
from datetime import (
    datetime,
    timedelta,
    timezone,
)
import logging
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
from processing.gold.window_manifest import (
    gold_window_manifest_exists,
    validate_gold_hour_window,
)
from processing.silver.window_manifest import (
    silver_window_manifest_exists,
)
from storage.minio_client import create_s3_client


ONE_HOUR = timedelta(hours=1)
FIVE_MINUTES = timedelta(minutes=5)

PROJECT_ROOT = (
    Path(__file__).resolve().parents[1]
)

BACKFILL_SCRIPT = (
    PROJECT_ROOT
    / "scripts"
    / "run_gold_backfill.py"
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Detect missing Gold hourly manifests and "
            "recover only hours whose twelve Silver "
            "five-minute dependencies are complete."
        )
    )

    parser.add_argument(
        "--start",
        help=(
            "Optional manual UTC inclusive start "
            "on an exact hour boundary"
        ),
    )

    parser.add_argument(
        "--stop",
        help=(
            "Optional manual UTC exclusive stop "
            "on an exact hour boundary"
        ),
    )

    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=24,
        help=(
            "Automatic-mode lookback window. "
            "Default: 24 hours."
        ),
    )

    parser.add_argument(
        "--max-windows",
        type=int,
        default=3,
        help=(
            "Maximum missing READY Gold hours to recover "
            "in one invocation. Default: 3."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Detect and report recovery candidates "
            "without running Spark."
        ),
    )

    return parser.parse_args()


def build_hour_windows(
    start,
    stop,
) -> list[TimeWindow]:
    if start >= stop:
        raise ValueError(
            "start must be earlier than stop"
        )

    for name, value in (
        ("start", start),
        ("stop", stop),
    ):
        if (
            value.minute != 0
            or value.second != 0
            or value.microsecond != 0
        ):
            raise ValueError(
                f"{name} must be aligned to an exact UTC hour"
            )

    seconds = (
        stop - start
    ).total_seconds()

    if seconds % 3600 != 0:
        raise ValueError(
            "Recovery range must contain whole hours"
        )

    result: list[TimeWindow] = []

    cursor = start

    while cursor < stop:
        window = TimeWindow(
            cursor,
            cursor + ONE_HOUR,
        )

        validate_gold_hour_window(
            window
        )

        result.append(
            window
        )

        cursor += ONE_HOUR

    return result


def resolve_range(
    args: argparse.Namespace,
) -> tuple:
    manual_start = (
        args.start is not None
    )

    manual_stop = (
        args.stop is not None
    )

    if manual_start != manual_stop:
        raise ValueError(
            "--start and --stop must be supplied together"
        )

    if manual_start:
        start = parse_utc(
            args.start
        )

        stop = parse_utc(
            args.stop
        )

        return (
            start,
            stop,
            True,
        )

    if args.lookback_hours <= 0:
        raise ValueError(
            "--lookback-hours must be positive"
        )

    now = datetime.now(
        timezone.utc
    )

    # Only consider fully completed UTC hours.
    stop = now.replace(
        minute=0,
        second=0,
        microsecond=0,
    )

    start = (
        stop
        - timedelta(
            hours=args.lookback_hours
        )
    )

    return (
        start,
        stop,
        False,
    )


def silver_dependency_status(
    client,
    bucket: str,
    gold_window: TimeWindow,
) -> tuple[int, tuple[str, ...]]:
    complete = 0
    missing: list[str] = []

    cursor = gold_window.start

    while cursor < gold_window.stop:
        silver_window = TimeWindow(
            cursor,
            cursor + FIVE_MINUTES,
        )

        if silver_window_manifest_exists(
            client,
            bucket,
            silver_window,
        ):
            complete += 1
        else:
            missing.append(
                utc_text(
                    silver_window.start
                )
            )

        cursor += FIVE_MINUTES

    return (
        complete,
        tuple(missing),
    )


def main() -> None:
    started = time.monotonic()

    logging.basicConfig(
        level=settings.log_level,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    log = logging.getLogger(__name__)

    args = arguments()

    if args.max_windows <= 0:
        raise ValueError(
            "--max-windows must be positive"
        )

    start, stop, manual_mode = (
        resolve_range(
            args
        )
    )

    windows = build_hour_windows(
        start,
        stop,
    )

    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    complete: list[TimeWindow] = []
    ready: list[TimeWindow] = []
    blocked: list[TimeWindow] = []

    log.info(
        "Gold recovery scan start "
        "mode=%s range=%s..%s hours=%s",
        (
            "manual"
            if manual_mode
            else "automatic"
        ),
        utc_text(start),
        utc_text(stop),
        len(windows),
    )

    for index, window in enumerate(
        windows,
        start=1,
    ):
        if gold_window_manifest_exists(
            s3,
            settings.minio_bucket,
            window,
        ):
            complete.append(
                window
            )

            log.info(
                "Gold recovery check "
                "position=%s/%s window=%s..%s "
                "status=COMPLETE",
                index,
                len(windows),
                utc_text(window.start),
                utc_text(window.stop),
            )

            continue

        silver_count, missing_silver = (
            silver_dependency_status(
                s3,
                settings.minio_bucket,
                window,
            )
        )

        if silver_count == 12:
            ready.append(
                window
            )

            log.info(
                "Gold recovery check "
                "position=%s/%s window=%s..%s "
                "status=READY silver_manifests=12/12",
                index,
                len(windows),
                utc_text(window.start),
                utc_text(window.stop),
            )
        else:
            blocked.append(
                window
            )

            log.warning(
                "Gold recovery check "
                "position=%s/%s window=%s..%s "
                "status=BLOCKED silver_manifests=%s/12 "
                "missing_silver=%s",
                index,
                len(windows),
                utc_text(window.start),
                utc_text(window.stop),
                silver_count,
                missing_silver,
            )

    selected = (
        ready[
            : args.max_windows
        ]
    )

    deferred = (
        ready[
            args.max_windows :
        ]
    )

    log.info(
        "Gold recovery plan "
        "complete=%s ready=%s blocked=%s "
        "selected=%s deferred=%s max_windows=%s",
        len(complete),
        len(ready),
        len(blocked),
        len(selected),
        len(deferred),
        args.max_windows,
    )

    if args.dry_run:
        for window in selected:
            log.info(
                "Gold recovery dry-run candidate "
                "window=%s..%s",
                utc_text(window.start),
                utc_text(window.stop),
            )

        log.info(
            "Gold recovery dry-run complete "
            "range=%s..%s",
            utc_text(start),
            utc_text(stop),
        )

        return

    recovered = 0

    for index, window in enumerate(
        selected,
        start=1,
    ):
        window_started = (
            time.monotonic()
        )

        start_text = utc_text(
            window.start
        )

        stop_text = utc_text(
            window.stop
        )

        command = [
            sys.executable,
            str(BACKFILL_SCRIPT),
            "--start",
            start_text,
            "--stop",
            stop_text,
        ]

        log.info(
            "Gold recovery backfill start "
            "position=%s/%s window=%s..%s",
            index,
            len(selected),
            start_text,
            stop_text,
        )

        subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            check=True,
        )

        if not gold_window_manifest_exists(
            s3,
            settings.minio_bucket,
            window,
        ):
            raise RuntimeError(
                "Gold recovery backfill returned successfully "
                "but completion manifest is still missing for "
                f"{start_text}..{stop_text}"
            )

        recovered += 1

        log.info(
            "Gold recovery backfill complete "
            "position=%s/%s window=%s..%s "
            "duration_seconds=%.2f",
            index,
            len(selected),
            start_text,
            stop_text,
            (
                time.monotonic()
                - window_started
            ),
        )

    remaining_selected = [
        window
        for window in selected
        if not gold_window_manifest_exists(
            s3,
            settings.minio_bucket,
            window,
        )
    ]

    if remaining_selected:
        raise RuntimeError(
            "Gold recovery completed but "
            f"{len(remaining_selected)} selected "
            "manifest(s) are still missing"
        )

    log.info(
        "Gold recovery complete "
        "range=%s..%s recovered=%s "
        "blocked=%s deferred=%s "
        "duration_seconds=%.2f",
        utc_text(start),
        utc_text(stop),
        recovered,
        len(blocked),
        len(deferred),
        (
            time.monotonic()
            - started
        ),
    )


if __name__ == "__main__":
    main()
