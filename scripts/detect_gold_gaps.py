#!/usr/bin/env python
"""Detect missing RIPE Atlas Gold hourly completion manifests."""

from __future__ import annotations

import argparse
import logging
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


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Detect missing Gold hourly manifests and determine "
            "whether each missing hour is recoverable from Silver."
        )
    )

    parser.add_argument(
        "--start",
        required=True,
        help="UTC inclusive start on an hour boundary",
    )

    parser.add_argument(
        "--stop",
        required=True,
        help="UTC exclusive stop on an hour boundary",
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

    duration_seconds = (
        stop - start
    ).total_seconds()

    if duration_seconds % 3600 != 0:
        raise ValueError(
            "Range duration must be a whole number of hours"
        )

    windows: list[TimeWindow] = []

    cursor = start

    while cursor < stop:
        window = TimeWindow(
            cursor,
            cursor + ONE_HOUR,
        )

        validate_gold_hour_window(
            window
        )

        windows.append(
            window
        )

        cursor += ONE_HOUR

    return windows


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
    logging.basicConfig(
        level=settings.log_level,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    log = logging.getLogger(__name__)

    args = arguments()

    start = parse_utc(
        args.start
    )

    stop = parse_utc(
        args.stop
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

    complete_count = 0
    ready_count = 0
    blocked_count = 0

    for index, window in enumerate(
        windows,
        start=1,
    ):
        gold_exists = (
            gold_window_manifest_exists(
                s3,
                settings.minio_bucket,
                window,
            )
        )

        if gold_exists:
            complete_count += 1

            log.info(
                "Gold manifest check "
                "position=%s/%s window=%s..%s "
                "status=COMPLETE",
                index,
                len(windows),
                utc_text(window.start),
                utc_text(window.stop),
            )

            continue

        silver_complete, silver_missing = (
            silver_dependency_status(
                s3,
                settings.minio_bucket,
                window,
            )
        )

        if silver_complete == 12:
            ready_count += 1
            status = "READY"
        else:
            blocked_count += 1
            status = "BLOCKED"

        log.info(
            "Gold manifest check "
            "position=%s/%s window=%s..%s "
            "status=%s silver_manifests=%s/12 "
            "missing_silver=%s",
            index,
            len(windows),
            utc_text(window.start),
            utc_text(window.stop),
            status,
            silver_complete,
            silver_missing,
        )

    missing_count = (
        ready_count
        + blocked_count
    )

    log.info(
        "Gold gap detection complete "
        "range=%s..%s hours=%s "
        "complete=%s missing=%s ready=%s blocked=%s",
        utc_text(start),
        utc_text(stop),
        len(windows),
        complete_count,
        missing_count,
        ready_count,
        blocked_count,
    )


if __name__ == "__main__":
    main()
