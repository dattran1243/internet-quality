#!/usr/bin/env python
"""Build an incremental country-scoped enriched Ping Silver window."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import logging
from pathlib import Path
import sys
import time


sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)


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

from processing.silver.incremental import (
    existing_window_rows,
    filter_event_window,
    hourly_partition_paths,
    replace_measurements_in_window,
    validate_hour_aligned_window,
)

from processing.silver.ripe_atlas_ping_country import (
    enrich_ping_country,
    enriched_summary,
    filter_selected_measurements,
    normalize_measurement_ids,
    validate_enriched_ping,
)

from scripts.run_silver import spark_session


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
        default=DEFAULT_COUNTRY_CODE,
        help=(
            "Two-letter country code. "
            f"Default: {DEFAULT_COUNTRY_CODE}"
        ),
    )

    parser.add_argument(
        "--measurement-id",
        action="append",
        type=int,
        help=(
            "Override selected product measurement IDs; "
            "repeat this option"
        ),
    )

    return parser.parse_args()


def selected_window(
    args: argparse.Namespace,
) -> TimeWindow:
    """Build and validate an hour-aligned UTC processing window."""

    window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )

    validate_hour_aligned_window(
        window
    )

    return window


def normalize_partition_columns(df):
    """Derive canonical zero-padded partitions from event time."""

    from pyspark.sql import functions as f

    return (
        df
        .withColumn(
            "year",
            f.date_format(
                "event_time_utc",
                "yyyy",
            ),
        )
        .withColumn(
            "month",
            f.date_format(
                "event_time_utc",
                "MM",
            ),
        )
        .withColumn(
            "day",
            f.date_format(
                "event_time_utc",
                "dd",
            ),
        )
        .withColumn(
            "hour",
            f.date_format(
                "event_time_utc",
                "HH",
            ),
        )
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

    log = logging.getLogger(__name__)

    args = arguments()

    window = selected_window(
        args
    )

    country = get_country(
        args.country_code
    )

    if not country.enabled:
        raise ValueError(
            f"Country {country.code} "
            "exists in monitoring registry "
            "but is currently disabled"
        )

    measurement_ids = normalize_measurement_ids(
        args.measurement_id
        or get_enabled_measurement_ids()
    )

    root = (
        f"s3a://{settings.minio_bucket}/"
        "silver/ripe_atlas"
    )

    # Country-scoped Silver Ping facts.
    ping_path = (
        f"{root}/ping_by_country/"
        f"country={country.code}/"
    )

    # Country-scoped current Probe Dimension.
    #
    # Example:
    # silver/ripe_atlas/probes_by_country/country=VN/
    probe_path = (
        f"{root}/probes_by_country/"
        f"country={country.code}/"
    )

    # Country-scoped enriched Ping output.
    #
    # Example:
    # silver/ripe_atlas/ping_enriched/country=VN/
    output_path = (
        f"{root}/ping_enriched/"
        f"country={country.code}/"
    )

    spark = spark_session()

    # Preserve zero-padded partition values:
    # month=09, hour=00, etc.
    spark.conf.set(
        "spark.sql.sources."
        "partitionColumnTypeInference.enabled",
        "false",
    )

    try:
        log.info(
            "Country enriched Ping "
            "country=%s "
            "window=%s..%s "
            "selected_measurements=%s",
            country.code,
            utc_text(window.start),
            utc_text(window.stop),
            measurement_ids,
        )

        log.info(
            "Reading Ping partitions=%s",
            hourly_partition_paths(
                ping_path,
                window,
            ),
        )

        log.info(
            "Reading Probe Dimension "
            "country=%s path=%s",
            country.code,
            probe_path,
        )

        ping = existing_window_rows(
            spark,
            ping_path,
            window,
        )

        if ping is None:
            raise ValueError(
                "No Silver Ping partitions exist "
                "for the requested window"
            )

        ping = (
            normalize_partition_columns(
                filter_event_window(
                    ping,
                    window,
                )
            )
            .cache()
        )

        if ping.limit(1).count() == 0:
            raise ValueError(
                "Silver Ping contains no rows "
                "inside the requested event-time window"
            )

        try:
            probes = (
                spark
                .read
                .parquet(
                    probe_path
                )
            )
        except Exception as exc:
            raise ValueError(
                "Country Probe Dimension does not exist "
                f"or cannot be read: "
                f"country={country.code} "
                f"path={probe_path}"
            ) from exc

        (
            enriched,
            join_metrics,
        ) = enrich_ping_country(
            ping,
            probes,
            measurement_ids,
            country.code,
        )

        if join_metrics.unmatched_ping_rows:
            log.warning(
                "Unmatched current probes "
                "country=%s "
                "rows=%s "
                "distinct=%s "
                "ids=%s",
                country.code,
                join_metrics.unmatched_ping_rows,
                join_metrics.unmatched_distinct_probes,
                join_metrics.unmatched_probe_ids,
            )

        validate_enriched_ping(
            enriched,
            measurement_ids,
            country.code,
            join_metrics.input_ping_rows,
        )

        summary = enriched_summary(
            enriched
        )

        log.info(
            "Country enriched ping "
            "join metrics=%s",
            asdict(
                join_metrics
            ),
        )

        log.info(
            "Country enriched ping "
            "quality summary=%s",
            summary,
        )

        log.info(
            "Replacing output partitions=%s "
            "country=%s "
            "measurement_ids=%s",
            hourly_partition_paths(
                output_path,
                window,
            ),
            country.code,
            measurement_ids,
        )

        replace_measurements_in_window(
            enriched,
            output_path,
            window,
            measurement_ids,
        )

        # Read back only affected partitions
        # and validate the persisted result.
        written = existing_window_rows(
            spark,
            output_path,
            window,
        )

        if written is None:
            raise ValueError(
                "Incremental country Ping write "
                "produced no output partition"
            )

        written = filter_event_window(
            written,
            window,
        )

        # A CLI update may contain only a subset of measurements.
        # Other measurements sharing the same hour are preserved
        # by replace_measurements_in_window().
        written_selected = (
            filter_selected_measurements(
                written,
                measurement_ids,
            )
            .cache()
        )

        validate_enriched_ping(
            written_selected,
            measurement_ids,
            country.code,
            join_metrics.input_ping_rows,
        )

        written_rows = (
            written_selected.count()
        )

        log.info(
            "Country enriched ping complete "
            "country=%s "
            "window_rows=%s "
            "path=%s "
            "duration_seconds=%.2f",
            country.code,
            written_rows,
            output_path,
            time.monotonic() - started,
        )

        written_selected.unpersist()
        ping.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
