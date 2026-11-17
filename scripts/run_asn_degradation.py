#!/usr/bin/env python
"""Build country-scoped ASN degradation signals from probe anomaly output."""

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
from processing.gold.ripe_atlas_asn_degradation import (
    build_asn_degradation,
    validate_asn_degradation,
    write_asn_degradation,
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

    parser.add_argument(
        "--min-eligible-probes",
        type=int,
        default=5,
    )

    parser.add_argument(
        "--min-affected-probes",
        type=int,
        default=2,
    )

    parser.add_argument(
        "--warning-ratio",
        type=float,
        default=0.30,
    )

    parser.add_argument(
        "--critical-ratio",
        type=float,
        default=0.50,
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
        f"s3a://{settings.minio_bucket}"
    )

    source = (
        f"{root}/gold/ripe_atlas/"
        "hourly_anomalies/"
        f"country={country.code}/"
    )

    target = (
        f"{root}/gold/ripe_atlas/"
        "asn_degradation/"
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
            "Country ASN degradation "
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
            "Reading Country anomaly source=%s",
            source,
        )

        anomalies = existing_window_rows(
            spark,
            source,
            window,
        )

        if anomalies is None:
            raise ValueError(
                "No country hourly anomaly partitions "
                "exist for requested window"
            )

        anomalies = (
            anomalies
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
            .cache()
        )

        total_probe_rows = (
            anomalies.count()
        )

        if total_probe_rows == 0:
            raise ValueError(
                "Country anomaly input is empty "
                f"for country={country.code}"
            )

        wrong_country = (
            anomalies
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
                        country.code
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
                "Country anomaly input contains rows "
                f"outside country={country.code}"
            )

        unexpected_measurement = (
            anomalies
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
                "Country anomaly input contains "
                "unexpected measurement IDs"
            )

        missing_asn_rows = (
            anomalies
            .filter(
                F.col(
                    "asn_v4"
                ).isNull()
                | (
                    F.col(
                        "asn_v4"
                    )
                    <= 0
                )
            )
            .count()
        )

        input_measurements = [
            int(
                row[
                    "measurement_id"
                ]
            )
            for row in (
                anomalies
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

        log.info(
            "Country ASN anomaly input "
            "country=%s rows=%s "
            "missing_asn_v4_rows=%s "
            "measurements=%s",
            country.code,
            total_probe_rows,
            missing_asn_rows,
            input_measurements,
        )

        result = (
            build_asn_degradation(
                anomalies,
                min_eligible_probes=(
                    args.min_eligible_probes
                ),
                min_affected_probes=(
                    args.min_affected_probes
                ),
                warning_ratio=(
                    args.warning_ratio
                ),
                critical_ratio=(
                    args.critical_ratio
                ),
            )
            .cache()
        )

        validate_asn_degradation(
            result,
            measurement_ids,
            country.code,
        )

        result_rows = (
            result.count()
        )

        print(
            "\n=== ASN SIGNAL DISTRIBUTION ==="
        )

        (
            result
            .groupBy(
                "country_code",
                "asn_signal_status",
            )
            .count()
            .orderBy(
                "country_code",
                "asn_signal_status",
            )
            .show(
                100,
                truncate=False,
            )
        )

        print(
            "\n=== TOP ASN DEGRADATIONS ==="
        )

        (
            result
            .filter(
                F.col(
                    "asn_signal_status"
                ).isin(
                    "warning",
                    "critical",
                )
            )
            .select(
                "hour_start_utc",
                "country_code",
                "measurement_id",
                "target",
                "asn_v4",
                "observed_probe_count",
                "eligible_probe_count",
                "insufficient_history_probe_count",
                "affected_probe_count",
                "critical_probe_count",
                "eligible_ratio",
                "affected_ratio",
                "critical_ratio",
                "avg_latency_ratio",
                "max_latency_ratio",
                "avg_packet_loss_pct",
                "max_packet_loss_pct",
                "asn_signal_status",
                "severity_score",
                "signal_reason",
            )
            .orderBy(
                F.col(
                    "severity_score"
                ).desc_nulls_last(),
                F.col(
                    "affected_ratio"
                ).desc_nulls_last(),
            )
            .show(
                100,
                truncate=False,
            )
        )

        log.info(
            "Writing Country ASN degradation "
            "country=%s path=%s",
            country.code,
            target,
        )

        write_asn_degradation(
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
                "Country ASN degradation write "
                "produced no output"
            )

        written = (
            written
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
            .cache()
        )

        validate_asn_degradation(
            written,
            measurement_ids,
            country.code,
        )

        written_rows = (
            written.count()
        )

        if written_rows != result_rows:
            raise ValueError(
                "Country ASN degradation read-back "
                "row count mismatch: "
                f"expected={result_rows} "
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

        log.info(
            "Country ASN degradation complete "
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
        anomalies.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()