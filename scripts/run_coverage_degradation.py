#!/usr/bin/env python
"""Build country-scoped measurement-level coverage degradation signals."""

from __future__ import annotations

import argparse
from datetime import timedelta
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
from processing.gold.ripe_atlas_coverage_degradation import (
    build_coverage_degradation,
    validate_coverage_output,
    write_coverage_degradation,
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
        "--lookback-hours",
        type=int,
        default=72,
    )

    parser.add_argument(
        "--min-history-hours",
        type=int,
        default=24,
    )

    parser.add_argument(
        "--warning-ratio",
        type=float,
        default=0.80,
    )

    parser.add_argument(
        "--critical-ratio",
        type=float,
        default=0.50,
    )

    parser.add_argument(
        "--min-probe-drop",
        type=int,
        default=2,
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

    if args.lookback_hours <= 0:
        raise ValueError(
            "lookback-hours must be positive"
        )

    if args.min_history_hours <= 0:
        raise ValueError(
            "min-history-hours must be positive"
        )

    if (
        args.min_history_hours
        > args.lookback_hours
    ):
        raise ValueError(
            "min-history-hours cannot exceed "
            "lookback-hours"
        )

    target_window = TimeWindow(
        parse_utc(
            args.start
        ),
        parse_utc(
            args.stop
        ),
    )

    validate_hour_aligned_window(
        target_window
    )

    history_start = (
        target_window.start
        - timedelta(
            hours=args.lookback_hours
        )
    )

    history_window = TimeWindow(
        history_start,
        target_window.stop,
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
        f"{root}/silver/ripe_atlas/"
        "ping_by_country/"
        f"country={country.code}/"
    )

    target = (
        f"{root}/gold/ripe_atlas/"
        "coverage_degradation/"
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
            "Country coverage target "
            "country=%s window=%s..%s",
            country.code,
            utc_text(
                target_window.start
            ),
            utc_text(
                target_window.stop
            ),
        )

        log.info(
            "Country coverage history "
            "country=%s window=%s..%s "
            "lookback_hours=%s "
            "min_history_hours=%s",
            country.code,
            utc_text(
                history_window.start
            ),
            utc_text(
                history_window.stop
            ),
            args.lookback_hours,
            args.min_history_hours,
        )

        log.info(
            "Reading Country Silver coverage source=%s",
            source,
        )

        silver = existing_window_rows(
            spark,
            source,
            history_window,
        )

        if silver is None:
            raise ValueError(
                "No country-scoped Silver Ping history "
                "exists for coverage calculation"
            )

        silver = (
            silver
            .filter(
                (
                    F.col(
                        "event_time_utc"
                    )
                    >= F.lit(
                        history_window.start
                    )
                )
                & (
                    F.col(
                        "event_time_utc"
                    )
                    < F.lit(
                        history_window.stop
                    )
                )
            )
            .cache()
        )

        input_rows = (
            silver.count()
        )

        if input_rows == 0:
            raise ValueError(
                "Country Silver coverage input is empty"
            )

        wrong_country = (
            silver
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
                "Coverage source contains rows outside "
                f"country={country.code}"
            )

        unexpected_measurement = (
            silver
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
                "Coverage source contains unexpected "
                "measurement IDs"
            )

        log.info(
            "Country coverage input "
            "country=%s rows=%s "
            "measurements=%s",
            country.code,
            input_rows,
            [
                int(
                    row[
                        "measurement_id"
                    ]
                )
                for row in (
                    silver
                    .select(
                        "measurement_id"
                    )
                    .distinct()
                    .orderBy(
                        "measurement_id"
                    )
                    .collect()
                )
            ],
        )

        result = (
            build_coverage_degradation(
                silver,
                measurement_ids,
                country_code=(
                    country.code
                ),
                series_start=(
                    history_start
                ),
                series_stop=(
                    target_window.stop
                ),
                lookback_hours=(
                    args.lookback_hours
                ),
                min_history_hours=(
                    args.min_history_hours
                ),
                warning_ratio=(
                    args.warning_ratio
                ),
                critical_ratio=(
                    args.critical_ratio
                ),
                min_probe_drop=(
                    args.min_probe_drop
                ),
            )
            .filter(
                (
                    F.col(
                        "hour_start_utc"
                    )
                    >= F.lit(
                        target_window.start
                    )
                )
                & (
                    F.col(
                        "hour_start_utc"
                    )
                    < F.lit(
                        target_window.stop
                    )
                )
            )
            .cache()
        )

        validate_coverage_output(
            result,
            measurement_ids,
            country.code,
        )

        rows = result.count()

        target_hours = int(
            (
                target_window.stop
                - target_window.start
            ).total_seconds()
            // 3600
        )

        expected_rows = (
            target_hours
            * len(
                measurement_ids
            )
        )

        if rows != expected_rows:
            raise ValueError(
                "Coverage grid row count mismatch: "
                f"expected={expected_rows} "
                f"actual={rows}"
            )

        print(
            "\n=== COVERAGE STATUS ==="
        )

        (
            result
            .groupBy(
                "country_code",
                "coverage_status",
            )
            .count()
            .orderBy(
                "country_code",
                "coverage_status",
            )
            .show(
                100,
                truncate=False,
            )
        )

        print(
            "\n=== COVERAGE SIGNALS ==="
        )

        (
            result
            .select(
                "hour_start_utc",
                "country_code",
                "measurement_id",
                "target",
                "source_hour_present",
                "observed_probe_count",
                "baseline_history_count",
                "baseline_probe_count_mean",
                "baseline_probe_count_median",
                "probe_drop_count",
                "probe_drop_pct",
                "probe_coverage_ratio",
                "result_count",
                "baseline_result_count_median",
                "result_coverage_ratio",
                "coverage_status",
                "severity_score",
                "signal_reason",
            )
            .orderBy(
                F.col(
                    "severity_score"
                ).desc_nulls_last(),
                F.col(
                    "probe_coverage_ratio"
                ).asc_nulls_last(),
            )
            .show(
                200,
                truncate=False,
            )
        )

        log.info(
            "Writing Country coverage "
            "country=%s rows=%s path=%s",
            country.code,
            rows,
            target,
        )

        write_coverage_degradation(
            result,
            target,
        )

        written = existing_window_rows(
            spark,
            target,
            target_window,
        )

        if written is None:
            raise ValueError(
                "Country coverage write produced "
                "no output"
            )

        written = (
            written
            .filter(
                (
                    F.col(
                        "hour_start_utc"
                    )
                    >= F.lit(
                        target_window.start
                    )
                )
                & (
                    F.col(
                        "hour_start_utc"
                    )
                    < F.lit(
                        target_window.stop
                    )
                )
            )
            .cache()
        )

        validate_coverage_output(
            written,
            measurement_ids,
            country.code,
        )

        written_rows = (
            written.count()
        )

        if written_rows != rows:
            raise ValueError(
                "Country coverage read-back "
                "row count mismatch: "
                f"expected={rows} "
                f"actual={written_rows}"
            )

        actual_measurements = [
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
                actual_measurements
            )
            != measurement_ids
        ):
            raise ValueError(
                "Country coverage read-back "
                "measurement set mismatch"
            )

        log.info(
            "Country coverage degradation complete "
            "country=%s rows=%s measurements=%s "
            "path=%s duration_seconds=%.2f",
            country.code,
            written_rows,
            actual_measurements,
            target,
            time.monotonic()
            - started,
        )

        written.unpersist()
        result.unpersist()
        silver.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()