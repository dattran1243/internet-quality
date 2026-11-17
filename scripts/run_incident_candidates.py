#!/usr/bin/env python
"""Build country-scoped unified network incident candidates."""

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

from pyspark.sql import functions as F

from configs.monitoring import (
    DEFAULT_COUNTRY_CODE,
    get_country,
    get_enabled_measurement_ids,
)
from configs.settings import settings
from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
    utc_text,
)
from processing.gold.ripe_atlas_incident_candidates import (
    build_incident_candidates,
    validate_incident_candidates,
    write_incident_candidates,
)
from processing.silver.incremental import (
    existing_window_rows,
    validate_hour_aligned_window,
)
from scripts.run_silver import spark_session


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--country-code",
        default=DEFAULT_COUNTRY_CODE,
        help=(
            "Configured enabled country scope "
            f"(default: {DEFAULT_COUNTRY_CODE})"
        ),
    )

    parser.add_argument(
        "--start",
        required=True,
    )

    parser.add_argument(
        "--stop",
        required=True,
    )

    return parser.parse_args()


def rows_in_window(
    df,
    window,
):
    return (
        df
        .filter(
            (
                F.col(
                    "hour_start_utc"
                )
                >= F.lit(
                    window.start
                )
            )
            & (
                F.col(
                    "hour_start_utc"
                )
                < F.lit(
                    window.stop
                )
            )
        )
    )


def validate_source_scope(
    df,
    *,
    source_name: str,
    country_code: str,
    measurement_ids: tuple[int, ...],
) -> None:
    wrong_country = (
        df
        .filter(
            F.col(
                "country_code"
            ).isNull()
            | (
                F.upper(
                    F.trim(
                        F.col(
                            "country_code"
                        )
                    )
                )
                != F.lit(
                    country_code
                )
            )
        )
        .limit(
            1
        )
        .count()
    )

    if wrong_country:
        raise ValueError(
            f"{source_name} contains rows outside "
            f"country={country_code}"
        )

    unexpected_measurement = (
        df
        .filter(
            ~F.col(
                "measurement_id"
            ).isin(
                list(
                    measurement_ids
                )
            )
        )
        .limit(
            1
        )
        .count()
    )

    if unexpected_measurement:
        raise ValueError(
            f"{source_name} contains unexpected "
            "measurement IDs"
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

    log = logging.getLogger(
        __name__
    )

    args = arguments()

    country = get_country(
        args.country_code
    )

    if not country.enabled:
        raise ValueError(
            f"Country {country.code} "
            "is configured but disabled"
        )

    window = TimeWindow(
        parse_utc(
            args.start
        ),
        parse_utc(
            args.stop
        ),
    )

    validate_hour_aligned_window(
        window
    )

    measurement_ids = tuple(
        sorted(
            int(
                value
            )
            for value
            in get_enabled_measurement_ids()
        )
    )

    if not measurement_ids:
        raise ValueError(
            "No enabled product measurements "
            "are configured"
        )

    root = (
        f"s3a://{settings.minio_bucket}/"
        "gold/ripe_atlas"
    )

    probe_source = (
        f"{root}/hourly_anomalies/"
        f"country={country.code}/"
    )

    asn_source = (
        f"{root}/asn_degradation/"
        f"country={country.code}/"
    )

    coverage_source = (
        f"{root}/coverage_degradation/"
        f"country={country.code}/"
    )

    target = (
        f"{root}/incident_candidates/"
        f"country={country.code}/"
    )

    spark = spark_session()

    spark.conf.set(
        "spark.sql.sources."
        "partitionColumnTypeInference.enabled",
        "false",
    )

    try:
        log.info(
            "Country incident candidate "
            "country=%s window=%s..%s",
            country.code,
            utc_text(
                window.start
            ),
            utc_text(
                window.stop
            ),
        )

        log.info(
            "Incident sources "
            "probe=%s asn=%s coverage=%s",
            probe_source,
            asn_source,
            coverage_source,
        )

        probe = existing_window_rows(
            spark,
            probe_source,
            window,
        )

        asn = existing_window_rows(
            spark,
            asn_source,
            window,
        )

        coverage = existing_window_rows(
            spark,
            coverage_source,
            window,
        )

        if probe is None:
            raise ValueError(
                "Country probe anomaly output missing"
            )

        if asn is None:
            raise ValueError(
                "Country ASN degradation output missing"
            )

        if coverage is None:
            raise ValueError(
                "Country coverage degradation output missing"
            )

        probe = (
            rows_in_window(
                probe,
                window,
            )
            .cache()
        )

        asn = (
            rows_in_window(
                asn,
                window,
            )
            .cache()
        )

        coverage = (
            rows_in_window(
                coverage,
                window,
            )
            .cache()
        )

        probe_rows = (
            probe.count()
        )

        asn_rows = (
            asn.count()
        )

        coverage_rows = (
            coverage.count()
        )

        if probe_rows == 0:
            raise ValueError(
                "Country probe anomaly input is empty"
            )

        if asn_rows == 0:
            raise ValueError(
                "Country ASN degradation input is empty"
            )

        if coverage_rows == 0:
            raise ValueError(
                "Country coverage input is empty"
            )

        validate_source_scope(
            probe,
            source_name="probe anomaly source",
            country_code=country.code,
            measurement_ids=measurement_ids,
        )

        validate_source_scope(
            asn,
            source_name="ASN degradation source",
            country_code=country.code,
            measurement_ids=measurement_ids,
        )

        validate_source_scope(
            coverage,
            source_name="coverage source",
            country_code=country.code,
            measurement_ids=measurement_ids,
        )

        log.info(
            "Country incident inputs "
            "country=%s probe_rows=%s "
            "asn_rows=%s coverage_rows=%s",
            country.code,
            probe_rows,
            asn_rows,
            coverage_rows,
        )

        result = (
            build_incident_candidates(
                probe,
                asn,
                coverage,
            )
            .cache()
        )

        validate_incident_candidates(
            result,
            measurement_ids,
            country.code,
        )

        row_count = (
            result.count()
        )

        target_hours = int(
            (
                window.stop
                - window.start
            ).total_seconds()
            // 3600
        )

        expected_rows = (
            target_hours
            * len(
                measurement_ids
            )
        )

        if row_count != expected_rows:
            raise ValueError(
                "Incident candidate grid row count mismatch: "
                f"expected={expected_rows} "
                f"actual={row_count}"
            )

        print(
            "\n=== INCIDENT STATUS ==="
        )

        (
            result
            .groupBy(
                "country_code",
                "incident_status",
            )
            .count()
            .orderBy(
                "country_code",
                "incident_status",
            )
            .show(
                100,
                truncate=False,
            )
        )

        print(
            "\n=== INCIDENT SIGNALS ==="
        )

        (
            result
            .select(
                "hour_start_utc",
                "country_code",
                "measurement_id",
                "target",
                "observed_probe_count",
                "eligible_probe_count",
                "affected_probe_count",
                "critical_probe_count",
                "observed_asn_count",
                "eligible_asn_count",
                "affected_asn_count",
                "critical_asn_count",
                "affected_asns",
                "coverage_status",
                "probe_coverage_ratio",
                "max_asn_affected_ratio",
                "max_probe_latency_ratio",
                "max_probe_packet_loss_pct",
                "incident_candidate",
                "incident_status",
                "severity_score",
                "signal_types",
                "incident_summary",
            )
            .orderBy(
                F.col(
                    "severity_score"
                ).desc_nulls_last(),
                F.col(
                    "hour_start_utc"
                ),
                F.col(
                    "measurement_id"
                ),
            )
            .show(
                200,
                truncate=False,
            )
        )

        print(
            "\n=== INCIDENT CANDIDATES ==="
        )

        (
            result
            .filter(
                F.col(
                    "incident_candidate"
                )
            )
            .select(
                "hour_start_utc",
                "country_code",
                "measurement_id",
                "target",
                "affected_probe_count",
                "critical_probe_count",
                "affected_asn_count",
                "critical_asn_count",
                "affected_asns",
                "coverage_status",
                "probe_coverage_ratio",
                "max_asn_affected_ratio",
                "max_probe_latency_ratio",
                "max_probe_packet_loss_pct",
                "incident_status",
                "severity_score",
                "signal_types",
                "incident_summary",
            )
            .orderBy(
                F.col(
                    "severity_score"
                ).desc_nulls_last(),
                F.col(
                    "hour_start_utc"
                ),
            )
            .show(
                200,
                truncate=False,
            )
        )

        log.info(
            "Writing Country incident candidates "
            "country=%s rows=%s path=%s",
            country.code,
            row_count,
            target,
        )

        write_incident_candidates(
            result,
            target,
        )

        written = existing_window_rows(
            spark,
            target,
            window,
        )

        if written is None:
            raise ValueError(
                "Country incident candidate write "
                "produced no output"
            )

        written = (
            rows_in_window(
                written,
                window,
            )
            .cache()
        )

        validate_incident_candidates(
            written,
            measurement_ids,
            country.code,
        )

        written_rows = (
            written.count()
        )

        if written_rows != row_count:
            raise ValueError(
                "Country incident candidate read-back "
                "row count mismatch: "
                f"expected={row_count} "
                f"actual={written_rows}"
            )

        written_measurements = [
            int(
                row[
                    "measurement_id"
                ]
            )
            for row in (
                written
                .select(
                    "measurement_id"
                )
                .distinct()
                .orderBy(
                    "measurement_id"
                )
                .collect()
            )
        ]

        if (
            tuple(
                written_measurements
            )
            != measurement_ids
        ):
            raise ValueError(
                "Country incident candidate read-back "
                "measurement set mismatch"
            )

        log.info(
            "Country incident candidate complete "
            "country=%s rows=%s measurements=%s "
            "path=%s duration_seconds=%.2f",
            country.code,
            written_rows,
            written_measurements,
            target,
            time.monotonic()
            - started,
        )

        written.unpersist()
        result.unpersist()
        probe.unpersist()
        asn.unpersist()
        coverage.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()