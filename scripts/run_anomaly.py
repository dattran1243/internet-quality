#!/usr/bin/env python
"""Build country-scoped hourly rolling baseline and anomaly output."""

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
from processing.gold.ripe_atlas_anomaly import (
    build_hourly_anomalies,
    validate_anomaly_output,
    write_hourly_anomalies,
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
        default=168,
    )

    parser.add_argument(
        "--min-history-hours",
        type=int,
        default=24,
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

    country = get_country(
        args.country_code
    )

    if not country.enabled:
        raise ValueError(
            f"Country {country.code} is configured but disabled"
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
            "min-history-hours cannot exceed lookback-hours"
        )

    target_window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )

    validate_hour_aligned_window(
        target_window
    )

    history_window = TimeWindow(
        target_window.start
        - timedelta(
            hours=args.lookback_hours
        ),
        target_window.stop,
    )

    measurement_ids = tuple(
        sorted(
            int(value)
            for value
            in get_enabled_measurement_ids()
        )
    )

    if not measurement_ids:
        raise ValueError(
            "No enabled product measurements are configured"
        )

    root = (
        f"s3a://{settings.minio_bucket}"
    )

    # Country-scoped Gold quality history.
    source = (
        f"{root}/gold/ripe_atlas/"
        "hourly_network_quality/"
        f"country={country.code}/"
    )

    # Country-scoped anomaly output.
    target = (
        f"{root}/gold/ripe_atlas/"
        "hourly_anomalies/"
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
            "Country anomaly "
            "country=%s target_window=%s..%s",
            country.code,
            utc_text(
                target_window.start
            ),
            utc_text(
                target_window.stop
            ),
        )

        log.info(
            "Country anomaly baseline "
            "country=%s history_window=%s..%s "
            "lookback_hours=%s min_history_hours=%s",
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
            "Reading Country Gold source=%s",
            source,
        )

        history = existing_window_rows(
            spark,
            source,
            history_window,
        )

        if history is None:
            raise ValueError(
                "No country Gold history exists "
                f"for country={country.code} "
                "and anomaly calculation"
            )

        history = history.cache()

        if (
            history.limit(1).count()
            == 0
        ):
            raise ValueError(
                "Country Gold history is empty "
                f"for country={country.code}"
            )

        wrong_country = (
            history
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
                    != country.code
                )
            )
            .limit(1)
            .count()
        )

        if wrong_country:
            raise ValueError(
                "Country Gold history contains rows outside "
                f"country={country.code}"
            )

        unexpected_measurement = (
            history
            .filter(
                ~F.col(
                    "measurement_id"
                ).isin(
                    list(
                        measurement_ids
                    )
                )
            )
            .limit(1)
            .count()
        )

        if unexpected_measurement:
            raise ValueError(
                "Country Gold history contains "
                "unexpected measurement IDs"
            )

        history_rows = history.count()

        history_measurements = [
            int(
                row["measurement_id"]
            )
            for row in (
                history
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
            "Country anomaly history "
            "country=%s rows=%s measurements=%s",
            country.code,
            history_rows,
            history_measurements,
        )

        scored = (
            build_hourly_anomalies(
                history,
                lookback_hours=(
                    args.lookback_hours
                ),
                min_history_hours=(
                    args.min_history_hours
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

        validate_anomaly_output(
            scored,
            measurement_ids,
            country.code,
        )

        total_rows = scored.count()

        log.info(
            "Country anomaly target "
            "country=%s rows=%s",
            country.code,
            total_rows,
        )

        (
            scored
            .groupBy(
                "country_code",
                "status",
            )
            .count()
            .orderBy(
                "country_code",
                "status",
            )
            .show(
                100,
                truncate=False,
            )
        )

        (
            scored
            .filter(
                F.col(
                    "status"
                ).isin(
                    "warning",
                    "critical",
                )
            )
            .select(
                "hour_start_utc",
                "country_code",
                "measurement_id",
                "probe_id",
                "target",
                "avg_rtt_ms",
                "baseline_rtt_median_ms",
                "latency_ratio",
                "avg_packet_loss_pct",
                "baseline_loss_mean_pct",
                "loss_delta_pct_points",
                "status",
                "anomaly_reasons",
            )
            .orderBy(
                F.col(
                    "severity_score"
                ).desc(),
                F.col(
                    "latency_ratio"
                ).desc_nulls_last(),
            )
            .show(
                100,
                truncate=False,
            )
        )

        log.info(
            "Writing Country anomaly "
            "country=%s path=%s",
            country.code,
            target,
        )

        write_hourly_anomalies(
            scored,
            target,
        )

        # Read back the target window.
        written = existing_window_rows(
            spark,
            target,
            target_window,
        )

        if written is None:
            raise ValueError(
                "Country anomaly write produced no output"
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

        validate_anomaly_output(
            written,
            measurement_ids,
            country.code,
        )

        written_rows = written.count()

        if written_rows != total_rows:
            raise ValueError(
                "Country anomaly read-back row count mismatch: "
                f"expected={total_rows} "
                f"actual={written_rows}"
            )

        log.info(
            "Country anomaly complete "
            "country=%s rows=%s path=%s "
            "duration_seconds=%.2f",
            country.code,
            written_rows,
            target,
            time.monotonic() - started,
        )

        written.unpersist()
        scored.unpersist()
        history.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()