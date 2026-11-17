#!/usr/bin/env python
"""Backfill RIPE Atlas Gold hourly windows sequentially."""

from __future__ import annotations

import argparse
import logging
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
from processing.gold.window_manifest import (
    validate_gold_hour_window,
)


ONE_HOUR = timedelta(hours=1)

GOLD_RUNNER = (
    Path(__file__).resolve().parent
    / "run_gold_global_hour.py"
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill one or more deterministic RIPE Atlas "
            "Gold hourly windows."
        )
    )

    parser.add_argument(
        "--start",
        required=True,
        help="UTC inclusive start on an exact hour boundary",
    )

    parser.add_argument(
        "--stop",
        required=True,
        help="UTC exclusive stop on an exact hour boundary",
    )

    parser.add_argument(
        "--measurement-id",
        action="append",
        type=int,
        dest="measurement_ids",
        help=(
            "Override enabled product measurement IDs. "
            "Repeat for multiple IDs."
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

    if not GOLD_RUNNER.exists():
        raise FileNotFoundError(
            f"Gold runner not found: {GOLD_RUNNER}"
        )

    log.info(
        "Gold backfill start "
        "range=%s..%s windows=%s",
        utc_text(start),
        utc_text(stop),
        len(windows),
    )

    for index, window in enumerate(
        windows,
        start=1,
    ):
        window_started = time.monotonic()

        start_text = utc_text(
            window.start
        )

        stop_text = utc_text(
            window.stop
        )

        log.info(
            "Gold backfill window start "
            "position=%s/%s window=%s..%s",
            index,
            len(windows),
            start_text,
            stop_text,
        )

        command = [
            sys.executable,
            str(GOLD_RUNNER),
            "--start",
            start_text,
            "--stop",
            stop_text,
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

        log.info(
            "Executing Gold backfill window: %s",
            " ".join(command),
        )

        subprocess.run(
            command,
            cwd=str(
                GOLD_RUNNER.parent.parent
            ),
            check=True,
        )

        log.info(
            "Gold backfill window complete "
            "position=%s/%s window=%s..%s "
            "duration_seconds=%.2f",
            index,
            len(windows),
            start_text,
            stop_text,
            (
                time.monotonic()
                - window_started
            ),
        )

    log.info(
        "Gold backfill complete "
        "range=%s..%s windows=%s "
        "duration_seconds=%.2f",
        utc_text(start),
        utc_text(stop),
        len(windows),
        (
            time.monotonic()
            - started
        ),
    )


if __name__ == "__main__":
    main()
