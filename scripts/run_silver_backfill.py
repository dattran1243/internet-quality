#!/usr/bin/env python
"""Backfill deterministic RIPE Atlas Silver five-minute windows."""

from __future__ import annotations

import argparse
from datetime import timedelta
import logging
from pathlib import Path
import sys
import time

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)

from configs.monitoring import (
    get_enabled_countries,
    get_enabled_measurement_ids,
)
from configs.settings import settings
from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
    utc_text,
)
from processing.silver.incremental import (
    validate_five_minute_window,
)
from processing.silver.window_manifest import (
    write_silver_window_manifest,
)
from scripts.run_silver import spark_session
from scripts.run_silver_country import main as run_country
from storage.minio_client import create_s3_client


FIVE_MINUTES = timedelta(minutes=5)


def arguments(
    argv: list[str] | None = None,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill RIPE Atlas Silver over a deterministic "
            "UTC time range using exact five-minute windows."
        )
    )

    parser.add_argument(
        "--start",
        required=True,
        help=(
            "UTC ISO-8601 inclusive backfill start. "
            "Must align to a five-minute boundary."
        ),
    )

    parser.add_argument(
        "--stop",
        required=True,
        help=(
            "UTC ISO-8601 exclusive backfill stop. "
            "Must align to a five-minute boundary."
        ),
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

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print the five-minute windows that would be "
            "processed without starting Spark or writing Silver."
        ),
    )

    return parser.parse_args(argv)


def build_windows(
    start,
    stop,
) -> list[TimeWindow]:
    if start >= stop:
        raise ValueError(
            "Backfill start must be earlier than stop"
        )

    probe = TimeWindow(
        start,
        start + FIVE_MINUTES,
    )

    validate_five_minute_window(
        probe
    )

    stop_probe = TimeWindow(
        stop - FIVE_MINUTES,
        stop,
    )

    validate_five_minute_window(
        stop_probe
    )

    duration_seconds = (
        stop - start
    ).total_seconds()

    if duration_seconds % 300 != 0:
        raise ValueError(
            "Backfill range must contain an exact number "
            "of five-minute windows"
        )

    windows: list[TimeWindow] = []

    cursor = start

    while cursor < stop:
        window = TimeWindow(
            cursor,
            cursor + FIVE_MINUTES,
        )

        validate_five_minute_window(
            window
        )

        windows.append(
            window
        )

        cursor = (
            cursor
            + FIVE_MINUTES
        )

    if not windows:
        raise ValueError(
            "Backfill range produced no windows"
        )

    return windows


def main(
    argv: list[str] | None = None,
) -> None:
    started = time.monotonic()

    logging.basicConfig(
        level=settings.log_level,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    log = logging.getLogger(__name__)

    args = arguments(
        argv
    )

    start = parse_utc(
        args.start
    )

    stop = parse_utc(
        args.stop
    )

    windows = build_windows(
        start,
        stop,
    )

    country_codes = sorted(
        country.code
        for country in get_enabled_countries()
    )

    if not country_codes:
        raise ValueError(
            "No enabled countries found in monitoring configuration"
        )

    measurement_ids = list(
        args.measurement_ids
        or get_enabled_measurement_ids()
    )

    if not measurement_ids:
        raise ValueError(
            "No enabled product measurements found"
        )

    log.info(
        "Silver backfill plan "
        "range=%s..%s windows=%s countries=%s",
        utc_text(start),
        utc_text(stop),
        len(windows),
        country_codes,
    )

    for index, window in enumerate(
        windows,
        start=1,
    ):
        log.info(
            "Silver backfill window planned "
            "position=%s/%s window=%s..%s",
            index,
            len(windows),
            utc_text(window.start),
            utc_text(window.stop),
        )

    if args.dry_run:
        log.info(
            "Silver backfill dry-run complete "
            "windows=%s countries=%s",
            len(windows),
            len(country_codes),
        )
        return

    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    spark = spark_session()

    try:
        for window_index, window in enumerate(
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
                "Silver backfill window start "
                "position=%s/%s window=%s..%s",
                window_index,
                len(windows),
                start_text,
                stop_text,
            )

            for country_index, country_code in enumerate(
                country_codes,
                start=1,
            ):
                country_started = time.monotonic()

                log.info(
                    "Silver backfill country start "
                    "window=%s/%s country=%s "
                    "position=%s/%s",
                    window_index,
                    len(windows),
                    country_code,
                    country_index,
                    len(country_codes),
                )

                country_argv = [
                    "--country-code",
                    country_code,
                    "--start",
                    start_text,
                    "--stop",
                    stop_text,
                ]

                for measurement_id in (
                    args.measurement_ids
                    or []
                ):
                    country_argv.extend(
                        [
                            "--measurement-id",
                            str(measurement_id),
                        ]
                    )

                run_country(
                    argv=country_argv,
                    spark=spark,
                )

                log.info(
                    "Silver backfill country complete "
                    "window=%s/%s country=%s "
                    "position=%s/%s "
                    "duration_seconds=%.2f",
                    window_index,
                    len(windows),
                    country_code,
                    country_index,
                    len(country_codes),
                    time.monotonic() - country_started,
                )

            manifest_key = write_silver_window_manifest(
                client=s3,
                bucket=settings.minio_bucket,
                window=window,
                country_codes=country_codes,
                measurement_ids=measurement_ids,
            )

            log.info(
                "Silver backfill manifest written "
                "window=%s..%s key=%s",
                start_text,
                stop_text,
                manifest_key,
            )

            log.info(
                "Silver backfill window complete "
                "position=%s/%s window=%s..%s "
                "duration_seconds=%.2f",
                window_index,
                len(windows),
                start_text,
                stop_text,
                time.monotonic() - window_started,
            )

        log.info(
            "Silver backfill complete "
            "range=%s..%s windows=%s countries=%s "
            "duration_seconds=%.2f",
            utc_text(start),
            utc_text(stop),
            len(windows),
            len(country_codes),
            time.monotonic() - started,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
