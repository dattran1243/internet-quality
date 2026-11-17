#!/usr/bin/env python
"""Process one RIPE Atlas Silver five-minute window for all enabled countries."""

from __future__ import annotations

import argparse
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


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start",
        required=True,
        help="UTC ISO-8601 five-minute window start",
    )

    parser.add_argument(
        "--stop",
        required=True,
        help="UTC ISO-8601 five-minute window stop",
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

    window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )

    validate_five_minute_window(
        window
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

    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    log.info(
        "Global Silver window start "
        "window=%s..%s countries=%s",
        utc_text(window.start),
        utc_text(window.stop),
        country_codes,
    )

    spark = spark_session()

    try:
        for index, country_code in enumerate(
            country_codes,
            start=1,
        ):
            country_started = time.monotonic()

            log.info(
                "Global Silver country start "
                "country=%s position=%s/%s",
                country_code,
                index,
                len(country_codes),
            )

            country_argv = [
                "--country-code",
                country_code,
                "--start",
                args.start,
                "--stop",
                args.stop,
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
                "Global Silver country complete "
                "country=%s position=%s/%s "
                "duration_seconds=%.2f",
                country_code,
                index,
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
            "Global Silver window manifest written "
            "window=%s..%s key=%s",
            utc_text(window.start),
            utc_text(window.stop),
            manifest_key,
        )

        log.info(
            "Global Silver window complete "
            "window=%s..%s countries=%s "
            "duration_seconds=%.2f",
            utc_text(window.start),
            utc_text(window.stop),
            len(country_codes),
            time.monotonic() - started,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
