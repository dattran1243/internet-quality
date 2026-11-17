#!/usr/bin/env python
"""Build one incremental Vietnam enriched Ping Silver window."""

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

from configs.settings import (
    DEFAULT_VN_PING_MEASUREMENT_IDS,
    settings,
)
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
from processing.silver.ripe_atlas_ping_vietnam import (
    enrich_ping_vietnam,
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
        "--measurement-id",
        action="append",
        type=int,
        help=(
            "Override selected measurement IDs; "
            "repeat this option"
        ),
    )

    return parser.parse_args()


def selected_window(
    args: argparse.Namespace,
) -> TimeWindow:
    window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )

    validate_hour_aligned_window(window)

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

    window = selected_window(args)

    measurement_ids = normalize_measurement_ids(
        args.measurement_id
        or DEFAULT_VN_PING_MEASUREMENT_IDS
    )

    root = (
        f"s3a://{settings.minio_bucket}/"
        "silver/ripe_atlas"
    )

    ping_path = f"{root}/ping/"
    probe_path = f"{root}/probes/"
    output_path = f"{root}/ping_vietnam/"

    spark = spark_session()

    # Prevent partition paths such as month=09 from being inferred as
    # integer month=9.
    spark.conf.set(
        "spark.sql.sources.partitionColumnTypeInference.enabled",
        "false",
    )

    try:
        log.info(
            "Vietnam enriched Ping window=%s..%s "
            "selected_measurements=%s",
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

        ping = existing_window_rows(
            spark,
            ping_path,
            window,
        )

        if ping is None:
            raise ValueError(
                "No Silver Ping partitions exist for "
                "the requested window"
            )

        ping = normalize_partition_columns(
            filter_event_window(
                ping,
                window,
            )
        ).cache()

        if ping.limit(1).count() == 0:
            raise ValueError(
                "Silver Ping contains no rows inside "
                "the requested event-time window"
            )

        probes = spark.read.parquet(
            probe_path
        )

        enriched, join_metrics = (
            enrich_ping_vietnam(
                ping,
                probes,
                measurement_ids,
            )
        )

        if join_metrics.unmatched_ping_rows:
            log.warning(
                "Unmatched current probes "
                "rows=%s distinct=%s ids=%s",
                join_metrics.unmatched_ping_rows,
                join_metrics.unmatched_distinct_probes,
                join_metrics.unmatched_probe_ids,
            )

        validate_enriched_ping(
            enriched,
            measurement_ids,
            join_metrics.matched_ping_rows,
        )

        summary = enriched_summary(
            enriched
        )

        log.info(
            "Vietnam enriched ping join metrics=%s",
            asdict(join_metrics),
        )

        log.info(
            "Vietnam enriched ping quality summary=%s",
            summary,
        )

        log.info(
            "Replacing output partitions=%s "
            "measurement_ids=%s",
            hourly_partition_paths(
                output_path,
                window,
            ),
            measurement_ids,
        )

        replace_measurements_in_window(
            enriched,
            output_path,
            window,
            measurement_ids,
        )

        # Read back ONLY the affected partitions.
        written = existing_window_rows(
            spark,
            output_path,
            window,
        )

        if written is None:
            raise ValueError(
                "Incremental ping_vietnam write produced "
                "no output partition"
            )

        written = filter_event_window(
            written,
            window,
        )

        # If CLI updates only a subset of measurements,
        # other measurements in the partition are intentionally
        # preserved. Validate just the current update scope.
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
            join_metrics.matched_ping_rows,
        )

        written_rows = (
            written_selected.count()
        )

        log.info(
            "Vietnam enriched ping complete "
            "window_rows=%s path=%s "
            "duration_seconds=%.2f",
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