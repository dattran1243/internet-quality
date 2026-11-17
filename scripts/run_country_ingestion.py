#!/usr/bin/env python
"""Ingest product RIPE Atlas Ping measurements for one country."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
import time


sys.path.insert(
    0,
    str(
        Path(__file__)
        .resolve()
        .parents[1]
    ),
)


from configs.monitoring import (
    DEFAULT_COUNTRY_CODE,
    get_country,
    get_enabled_measurement_ids,
)

from configs.settings import settings

from ingestion.ripe_atlas.client import (
    RipeAtlasClient,
)

from ingestion.ripe_atlas.ingest_country import (
    ingest_country_measurements,
)

from scripts.run_ingestion import (
    selected_window,
)

from storage.minio_client import (
    create_s3_client,
)


def arguments() -> argparse.Namespace:
    """Parse country, measurement scope, and bounded UTC window."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--country-code",
        default=DEFAULT_COUNTRY_CODE,
        help=(
            "Configured two-letter country code. "
            f"Default: {DEFAULT_COUNTRY_CODE}"
        ),
    )

    parser.add_argument(
        "--measurement-id",
        type=int,
        action="append",
        dest="measurement_ids",
        help=(
            "Override enabled product measurements. "
            "Repeat this option for multiple IDs."
        ),
    )

    parser.add_argument(
        "--hours",
        type=float,
        default=1.0,
        help=(
            "Relative lookback window when "
            "--start/--stop are omitted."
        ),
    )

    parser.add_argument(
        "--start",
        help="UTC ISO-8601 window start",
    )

    parser.add_argument(
        "--stop",
        help="UTC ISO-8601 window stop",
    )

    args = parser.parse_args()

    if args.measurement_ids is None:
        args.measurement_ids = (
            get_enabled_measurement_ids()
        )

    return args


def normalize_measurement_ids(
    values: list[int],
) -> list[int]:
    """Return deterministic unique positive measurement IDs."""

    ids = list(
        dict.fromkeys(
            int(value)
            for value in values
        )
    )

    if (
        not ids
        or any(
            measurement_id <= 0
            for measurement_id in ids
        )
    ):
        raise ValueError(
            "measurement_ids must contain positive IDs"
        )

    return ids


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

    settings.require_storage_credentials()

    country = get_country(
        args.country_code
    )

    if not country.enabled:
        raise ValueError(
            f"Country {country.code} "
            "exists in monitoring registry "
            "but is currently disabled"
        )

    measurement_ids = (
        normalize_measurement_ids(
            args.measurement_ids
        )
    )

    window = selected_window(
        args
    )

    client = RipeAtlasClient(
        settings.ripe_atlas_base_url,
        settings.request_timeout,
        settings.max_retries,
        settings.backoff_seconds,
    )

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    log.info(
        "Country ingestion start "
        "country=%s "
        "measurements=%s "
        "start=%s "
        "stop=%s",
        country.code,
        measurement_ids,
        window.start.isoformat(),
        window.stop.isoformat(),
    )

    summaries = (
        ingest_country_measurements(
            client=client,
            s3_client=s3,
            bucket=settings.minio_bucket,
            measurement_ids=measurement_ids,
            window=window,
            country_code=country.code,
            status_name="Connected",
        )
    )

    total_records = sum(
        int(
            item.get(
                "record_count",
                0,
            )
        )
        for item in summaries
    )

    log.info(
        "Country ingestion complete "
        "country=%s "
        "measurements=%s "
        "records=%s "
        "duration_seconds=%.2f",
        country.code,
        len(summaries),
        total_records,
        time.monotonic() - started,
    )

    for item in summaries:
        log.info(
            "Country ingestion summary "
            "country=%s "
            "measurement=%s "
            "records=%s "
            "requested_probes=%s "
            "returned_probes=%s "
            "missing_probes=%s "
            "coverage_pct=%.2f "
            "object_key=%s",
            country.code,
            item["measurement_id"],
            item["record_count"],
            item["requested_probe_count"],
            item[
                "returned_unique_probe_count"
            ],
            item["missing_probe_count"],
            item["coverage_pct"],
            item["object_key"],
        )


if __name__ == "__main__":
    main()