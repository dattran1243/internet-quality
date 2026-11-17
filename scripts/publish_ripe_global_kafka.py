#!/usr/bin/env python
"""Publish RIPE Atlas ping windows for multiple countries to Kafka."""

from __future__ import annotations

import argparse
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
import logging
from pathlib import Path
import subprocess
import sys


sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)


from configs.monitoring import (
    get_country,
    get_enabled_countries,
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start",
        required=True,
        help="UTC ISO-8601 window start",
    )

    parser.add_argument(
        "--stop",
        required=True,
        help="UTC ISO-8601 window stop",
    )

    parser.add_argument(
        "--country-code",
        action="append",
        dest="country_codes",
        help=(
            "Country to publish. "
            "Repeat for multiple countries. "
            "If omitted, all enabled countries are used."
        ),
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help=(
            "Maximum countries to publish concurrently. "
            "Default: 1."
        ),
    )

    return parser.parse_args()


def selected_country_codes(
    requested: list[str] | None,
) -> list[str]:
    if requested:
        codes = []

        for value in requested:
            country = get_country(value)

            code = country.code

            if code not in codes:
                codes.append(code)

        return codes

    return [
        country.code
        for country in get_enabled_countries()
    ]


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    log = logging.getLogger(__name__)

    args = arguments()

    if args.max_workers <= 0:
        raise ValueError(
            "max_workers must be positive"
        )

    country_codes = selected_country_codes(
        args.country_codes
    )

    if not country_codes:
        raise ValueError(
            "No countries selected for ingestion"
        )

    country_script = (
        Path(__file__).resolve().parent
        / "publish_ripe_country_kafka.py"
    )

    successful: list[str] = []
    failed: list[str] = []

    log.info(
        "Global RIPE publisher start "
        "countries=%s window=%s..%s "
        "max_workers=%s",
        country_codes,
        args.start,
        args.stop,
        args.max_workers,
    )

    def publish_country(
        country_code: str,
    ) -> tuple[str, int]:
        log.info(
            "Publishing country=%s",
            country_code,
        )

        command = [
            sys.executable,
            str(country_script),
            "--country-code",
            country_code,
            "--start",
            args.start,
            "--stop",
            args.stop,
        ]

        result = subprocess.run(
            command,
            check=False,
        )

        return (
            country_code,
            result.returncode,
        )

    with ThreadPoolExecutor(
        max_workers=args.max_workers,
    ) as executor:

        futures = {
            executor.submit(
                publish_country,
                country_code,
            ): country_code
            for country_code in country_codes
        }

        for future in as_completed(
            futures
        ):
            requested_country = (
                futures[future]
            )

            try:
                (
                    country_code,
                    returncode,
                ) = future.result()

            except Exception:
                failed.append(
                    requested_country
                )

                log.exception(
                    "Country publish FAIL "
                    "country=%s",
                    requested_country,
                )

                continue

            if returncode == 0:
                successful.append(
                    country_code
                )

                log.info(
                    "Country publish PASS "
                    "country=%s",
                    country_code,
                )

            else:
                failed.append(
                    country_code
                )

                log.error(
                    "Country publish FAIL "
                    "country=%s "
                    "returncode=%s",
                    country_code,
                    returncode,
                )

    successful = sorted(
        successful
    )

    failed = sorted(
        failed
    )

    log.info(
        "Global RIPE publisher complete "
        "requested=%s successful=%s failed=%s",
        len(country_codes),
        len(successful),
        len(failed),
    )

    log.info(
        "Successful countries=%s",
        successful,
    )

    if failed:
        log.error(
            "Failed countries=%s",
            failed,
        )

        raise SystemExit(1)


if __name__ == "__main__":
    main()