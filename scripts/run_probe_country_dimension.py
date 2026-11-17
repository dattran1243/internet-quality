#!/usr/bin/env python
"""Build a country-scoped current RIPE Atlas probe Silver dimension."""

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
)

from configs.settings import settings

from ingestion.ripe_atlas.client import (
    RipeAtlasClient,
)

from processing.silver.ripe_atlas_probe import (
    build_probe_dimension,
    probe_dimension_summary,
    validate_probe_dimension,
)

from scripts.run_silver import (
    spark_session,
)


DEFAULT_STATUS_NAME = "Connected"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--country-code",
        default=DEFAULT_COUNTRY_CODE,
        help=(
            "Two-letter country code. "
            f"Default: {DEFAULT_COUNTRY_CODE}"
        ),
    )

    parser.add_argument(
        "--status-name",
        default=DEFAULT_STATUS_NAME,
        help=(
            "RIPE Atlas probe status. "
            f"Default: {DEFAULT_STATUS_NAME}"
        ),
    )

    parser.add_argument(
        "--output-path",
        help=(
            "Optional complete output path override. "
            "Normally this should not be needed."
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

    status_name = (
        args.status_name
        .strip()
    )

    if not status_name:
        raise ValueError(
            "status_name must not be empty"
        )

    output_path = (
        args.output_path
        or (
            f"s3a://{settings.minio_bucket}/"
            "silver/ripe_atlas/"
            "probes_by_country/"
            f"country={country.code}/"
        )
    )

    client = RipeAtlasClient(
        settings.ripe_atlas_base_url,
        settings.request_timeout,
        settings.max_retries,
        settings.backoff_seconds,
    )

    log.info(
        "Fetching probe dimension "
        "country=%s status=%s",
        country.code,
        status_name,
    )

    probes = client.get_connected_probes(
        country.code,
        status_name,
    )

    log.info(
        "Probe API discovery complete "
        "country=%s status=%s probes=%s",
        country.code,
        status_name,
        len(probes),
    )

    spark = spark_session()

    try:
        dimension = (
            build_probe_dimension(
                spark,
                probes,
            )
            .cache()
        )

        validate_probe_dimension(
            dimension,
            country.code,
            status_name,
        )

        summary = probe_dimension_summary(
            dimension
        )

        distributions = [
            row.asDict()
            for row in (
                dimension
                .groupBy(
                    "country_code",
                    "status_name",
                )
                .count()
                .collect()
            )
        ]

        log.info(
            "Probe dimension summary "
            "country=%s "
            "status=%s "
            "probes=%s "
            "distinct_probe_ids=%s "
            "distinct_asn_v4=%s "
            "missing_asn_v4=%s "
            "missing_asn_v6=%s "
            "missing_coordinates=%s "
            "anchors=%s "
            "distribution=%s",
            country.code,
            status_name,
            summary["probes"],
            summary["distinct_probe_ids"],
            summary["distinct_asn_v4"],
            summary["missing_asn_v4"],
            summary["missing_asn_v6"],
            summary["missing_coordinates"],
            summary["anchors"],
            distributions,
        )

        log.info(
            "Writing current probe snapshot "
            "country=%s path=%s mode=overwrite",
            country.code,
            output_path,
        )

        (
            dimension
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(
                output_path
            )
        )

        written = (
            spark
            .read
            .parquet(
                output_path
            )
            .cache()
        )

        validate_probe_dimension(
            written,
            country.code,
            status_name,
        )

        written_summary = (
            probe_dimension_summary(
                written
            )
        )

        if (
            written_summary["probes"]
            != summary["probes"]
        ):
            raise ValueError(
                "Probe dimension row count changed "
                "after write/read validation"
            )

        log.info(
            "Country probe dimension complete "
            "country=%s "
            "rows=%s "
            "path=%s "
            "duration_seconds=%.2f",
            country.code,
            written_summary["probes"],
            output_path,
            time.monotonic() - started,
        )

        written.unpersist()
        dimension.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()