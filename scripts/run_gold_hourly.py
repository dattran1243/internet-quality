#!/usr/bin/env python
"""Build incremental country-scoped RIPE Atlas hourly Gold quality."""

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
from processing.gold.ripe_atlas_hourly import (
    build_hourly_network_quality,
    validate_gold_input,
    validate_hourly_gold,
    write_hourly_gold,
)
from processing.silver.incremental import (
    existing_window_rows,
    filter_event_window,
    hourly_partition_paths,
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
        help="UTC ISO-8601 start",
    )

    parser.add_argument(
        "--stop",
        required=True,
        help="UTC ISO-8601 stop",
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

    window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )

    validate_hour_aligned_window(
        window
    )

    selected_ids = tuple(
        sorted(
            int(value)
            for value
            in get_enabled_measurement_ids()
        )
    )

    if not selected_ids:
        raise ValueError(
            "No enabled product measurements are configured"
        )

    root = (
        f"s3a://{settings.minio_bucket}"
    )

    # Country-scoped enriched Silver input.
    source = (
        f"{root}/silver/ripe_atlas/"
        "ping_enriched/"
        f"country={country.code}/"
    )

    # Country-scoped Gold output.
    #
    # Keeping each country under an isolated root makes dynamic
    # year/month/day/hour overwrite safe across countries.
    target = (
        f"{root}/gold/ripe_atlas/"
        "hourly_network_quality/"
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
            "Country Gold hourly "
            "country=%s window=%s..%s "
            "selected_measurements=%s",
            country.code,
            utc_text(window.start),
            utc_text(window.stop),
            selected_ids,
        )

        log.info(
            "Reading country Silver partitions=%s",
            hourly_partition_paths(
                source,
                window,
            ),
        )

        silver = existing_window_rows(
            spark,
            source,
            window,
        )

        if silver is None:
            raise ValueError(
                "No country enriched Silver partition exists "
                f"for country={country.code} "
                "and requested Gold window"
            )

        silver = (
            filter_event_window(
                silver,
                window,
            )
            .cache()
        )

        validate_gold_input(
            silver,
            selected_ids,
            country.code,
        )

        silver_rows = silver.count()

        source_measurements = [
            int(row["measurement_id"])
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
        ]

        expected_measurements = list(
            selected_ids
        )

        if (
            source_measurements
            != expected_measurements
        ):
            raise ValueError(
                "Country Gold input measurement set mismatch: "
                f"expected={expected_measurements} "
                f"actual={source_measurements}"
            )

        source_countries = [
            row["country_code"]
            for row in (
                silver
                .select(
                    F.upper(
                        F.trim(
                            F.col(
                                "country_code"
                            )
                        )
                    ).alias(
                        "country_code"
                    )
                )
                .distinct()
                .collect()
            )
        ]

        if source_countries != [
            country.code
        ]:
            raise ValueError(
                "Country Gold input country scope mismatch: "
                f"expected={[country.code]} "
                f"actual={source_countries}"
            )

        gold = (
            build_hourly_network_quality(
                silver
            )
            .cache()
        )

        validate_hourly_gold(
            gold,
            selected_ids,
            country.code,
        )

        gold_rows = gold.count()

        log.info(
            "Country Gold aggregation "
            "country=%s silver_rows=%s gold_rows=%s",
            country.code,
            silver_rows,
            gold_rows,
        )

        (
            gold
            .groupBy(
                "country_code",
                "measurement_id",
                "target",
            )
            .agg(
                F.count(
                    "*"
                ).alias(
                    "probe_hour_rows"
                ),
                F.sum(
                    "sample_count"
                ).alias(
                    "source_samples"
                ),
                F.avg(
                    "sample_count"
                ).alias(
                    "avg_samples_per_probe_hour"
                ),
                F.avg(
                    "avg_rtt_ms"
                ).alias(
                    "mean_probe_hour_rtt_ms"
                ),
                F.avg(
                    "avg_packet_loss_pct"
                ).alias(
                    "mean_probe_hour_loss_pct"
                ),
            )
            .orderBy(
                "country_code",
                "measurement_id",
            )
            .show(
                100,
                truncate=False,
            )
        )

        log.info(
            "Replacing Country Gold partitions=%s "
            "country=%s",
            hourly_partition_paths(
                target,
                window,
            ),
            country.code,
        )

        write_hourly_gold(
            gold,
            target,
        )

        written = existing_window_rows(
            spark,
            target,
            window,
        )

        if written is None:
            raise ValueError(
                "Country Gold write produced no output"
            )

        # Gold uses hour_start_utc instead of event_time_utc.
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

        validate_hourly_gold(
            written,
            selected_ids,
            country.code,
        )

        written_rows = written.count()

        if written_rows != gold_rows:
            raise ValueError(
                "Country Gold read-back row count mismatch: "
                f"expected={gold_rows} "
                f"actual={written_rows}"
            )

        written_measurements = [
            int(row["measurement_id"])
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
            written_measurements
            != expected_measurements
        ):
            raise ValueError(
                "Country Gold read-back measurement set mismatch: "
                f"expected={expected_measurements} "
                f"actual={written_measurements}"
            )

        log.info(
            "Country Gold hourly complete "
            "country=%s rows=%s "
            "measurements=%s path=%s "
            "duration_seconds=%.2f",
            country.code,
            written_rows,
            written_measurements,
            target,
            time.monotonic() - started,
        )

        written.unpersist()
        gold.unpersist()
        silver.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()