#!/usr/bin/env python
"""Detect and recover missing RIPE Atlas Silver five-minute windows."""

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
from processing.silver.recovery_watermark import (
    read_silver_recovery_watermark,
    write_silver_recovery_watermark,
)
from processing.silver.window_manifest import (
    silver_window_manifest_exists,
)
from scripts.run_silver_backfill import build_windows
from storage.minio_client import create_s3_client


BACKFILL_SCRIPT = (
    Path(__file__).resolve().parent
    / "run_silver_backfill.py"
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Detect missing Silver completion manifests "
            "and automatically recover those five-minute windows."
        )
    )

    parser.add_argument(
        "--start",
        help=(
            "Optional manual UTC inclusive start. "
            "Must be supplied together with --stop."
        ),
    )

    parser.add_argument(
        "--stop",
        help=(
            "Optional manual UTC exclusive stop. "
            "Must be supplied together with --start."
        ),
    )

    parser.add_argument(
        "--safe-lag-minutes",
        type=int,
        default=15,
        help=(
            "Automatic mode only. Do not scan windows newer "
            "than this many minutes. Default: 15."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect and print gaps without running backfill.",
    )

    return parser.parse_args()


def floor_five_minutes(
    value: datetime,
) -> datetime:
    return value.replace(
        minute=(
            value.minute
            // 5
        ) * 5,
        second=0,
        microsecond=0,
    )


def missing_windows(
    client,
    bucket: str,
    windows: list[TimeWindow],
) -> list[TimeWindow]:
    missing: list[TimeWindow] = []

    for window in windows:
        if not silver_window_manifest_exists(
            client,
            bucket,
            window,
        ):
            missing.append(
                window
            )

    return missing


def contiguous_ranges(
    windows: list[TimeWindow],
) -> list[tuple[object, object]]:
    if not windows:
        return []

    ordered = sorted(
        windows,
        key=lambda window: window.start,
    )

    ranges = []

    range_start = ordered[0].start
    range_stop = ordered[0].stop

    for window in ordered[1:]:
        if window.start == range_stop:
            range_stop = window.stop
            continue

        ranges.append(
            (
                range_start,
                range_stop,
            )
        )

        range_start = window.start
        range_stop = window.stop

    ranges.append(
        (
            range_start,
            range_stop,
        )
    )

    return ranges


def run_backfill(
    start,
    stop,
) -> None:
    command = [
        sys.executable,
        str(BACKFILL_SCRIPT),
        "--start",
        utc_text(start),
        "--stop",
        utc_text(stop),
    ]

    subprocess.run(
        command,
        check=True,
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

    if (
        args.start is None
    ) != (
        args.stop is None
    ):
        raise ValueError(
            "--start and --stop must be supplied together"
        )

    if args.safe_lag_minutes < 10:
        raise ValueError(
            "--safe-lag-minutes must be at least 10"
        )

    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    manual_mode = (
        args.start is not None
    )

    if manual_mode:
        start = parse_utc(
            args.start
        )

        stop = parse_utc(
            args.stop
        )

        log.info(
            "Silver recovery manual mode "
            "range=%s..%s",
            utc_text(start),
            utc_text(stop),
        )

    else:
        start = read_silver_recovery_watermark(
            s3,
            settings.minio_bucket,
        )

        if start is None:
            raise ValueError(
                "Silver recovery watermark is not initialized"
            )

        safe_reference = (
            datetime.now(
                timezone.utc
            )
            - timedelta(
                minutes=args.safe_lag_minutes
            )
        )

        stop = floor_five_minutes(
            safe_reference
        )

        log.info(
            "Silver recovery automatic mode "
            "watermark=%s safe_stop=%s "
            "safe_lag_minutes=%s",
            utc_text(start),
            utc_text(stop),
            args.safe_lag_minutes,
        )

        if start >= stop:
            log.info(
                "Silver recovery has no eligible windows"
            )
            return

    windows = build_windows(
        start,
        stop,
    )

    missing = missing_windows(
        s3,
        settings.minio_bucket,
        windows,
    )

    log.info(
        "Silver recovery scan complete "
        "range=%s..%s windows=%s missing=%s",
        utc_text(start),
        utc_text(stop),
        len(windows),
        len(missing),
    )

    for window in missing:
        log.warning(
            "Silver recovery gap "
            "window=%s..%s",
            utc_text(window.start),
            utc_text(window.stop),
        )

    ranges = contiguous_ranges(
        missing
    )

    log.info(
        "Silver recovery plan "
        "missing_windows=%s backfill_ranges=%s",
        len(missing),
        len(ranges),
    )

    for range_start, range_stop in ranges:
        log.info(
            "Silver recovery range "
            "window=%s..%s",
            utc_text(range_start),
            utc_text(range_stop),
        )

    if args.dry_run:
        log.info(
            "Silver recovery dry-run complete"
        )
        return

    for index, (
        range_start,
        range_stop,
    ) in enumerate(
        ranges,
        start=1,
    ):
        log.info(
            "Silver recovery backfill start "
            "position=%s/%s range=%s..%s",
            index,
            len(ranges),
            utc_text(range_start),
            utc_text(range_stop),
        )

        run_backfill(
            range_start,
            range_stop,
        )

        log.info(
            "Silver recovery backfill complete "
            "position=%s/%s range=%s..%s",
            index,
            len(ranges),
            utc_text(range_start),
            utc_text(range_stop),
        )

    remaining = missing_windows(
        s3,
        settings.minio_bucket,
        windows,
    )

    if remaining:
        for window in remaining:
            log.error(
                "Silver recovery verification failed "
                "window=%s..%s",
                utc_text(window.start),
                utc_text(window.stop),
            )

        raise RuntimeError(
            "Silver recovery completed but "
            f"{len(remaining)} manifest(s) are still missing"
        )

    if not manual_mode:
        watermark_key = (
            write_silver_recovery_watermark(
                s3,
                settings.minio_bucket,
                stop,
            )
        )

        log.info(
            "Silver recovery watermark advanced "
            "verified_through=%s key=%s",
            utc_text(stop),
            watermark_key,
        )

    log.info(
        "Silver recovery complete "
        "range=%s..%s recovered_windows=%s "
        "remaining_missing=0",
        utc_text(start),
        utc_text(stop),
        len(missing),
    )


if __name__ == "__main__":
    main()
