#!/usr/bin/env python
"""Build one global RIPE Atlas Gold hour for all enabled countries."""

from __future__ import annotations

import argparse
from dataclasses import asdict
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
    get_enabled_countries,
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
from processing.gold.window_manifest import (
    validate_gold_hour_window,
    write_gold_window_manifest,
)

from processing.silver.incremental import (
    existing_window_rows,
    filter_event_window,
    hourly_partition_paths,
    replace_measurements_in_window,
)
from processing.silver.ripe_atlas_ping_country import (
    enrich_ping_country,
    enriched_summary,
    filter_selected_measurements,
    normalize_measurement_ids,
    validate_enriched_ping,
)
from processing.silver.window_manifest import (
    silver_window_manifest_exists,
    silver_window_manifest_key,
)

from scripts.run_silver import spark_session
from storage.minio_client import create_s3_client


FIVE_MINUTES = timedelta(
    minutes=5
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build one global RIPE Atlas hourly Gold window "
            "for every enabled country."
        )
    )

    parser.add_argument(
        "--start",
        required=True,
        help="UTC ISO-8601 hour start",
    )

    parser.add_argument(
        "--stop",
        required=True,
        help="UTC ISO-8601 hour stop",
    )

    parser.add_argument(
        "--measurement-id",
        action="append",
        type=int,
        dest="measurement_ids",
        help=(
            "Override enabled product measurement IDs. "
            "Repeat for multiple IDs."
        ),
    )

    # PATCH03: opt-in signed-plan CHECK ONLY. No candidate Gold execution.
    parser.add_argument(
        "--verified-input-plan", default=None,
        help="Local signed test-only plan; checked before any S3/Spark access",
    )
    parser.add_argument(
        "--test-public-keys", default=None,
        help="External test-only ED25519 public-key JSON allowlist",
    )
    parser.add_argument(
        "--local-parquet-bundle", default=None,
        help="Optional explicit local 60-window Parquet plus independent JSONL proof",
    )
    parser.add_argument(
        "--verified-plan-preflight-only", action="store_true",
        help="Validate local plan and exit; does not run Gold",
    )

    return parser.parse_args()


def five_minute_windows(
    window: TimeWindow,
) -> list[TimeWindow]:
    """Return the twelve Silver windows inside one Gold hour."""

    result: list[TimeWindow] = []

    cursor = window.start

    while cursor < window.stop:
        stop = cursor + FIVE_MINUTES

        if stop > window.stop:
            raise ValueError(
                "Gold hour cannot be divided into exact "
                "five-minute Silver windows"
            )

        result.append(
            TimeWindow(
                cursor,
                stop,
            )
        )

        cursor = stop

    if len(result) != 12:
        raise ValueError(
            "Expected exactly 12 Silver windows "
            f"inside one Gold hour, got {len(result)}"
        )

    return result


def require_complete_silver_hour(
    client,
    bucket: str,
    window: TimeWindow,
) -> None:
    """
    Refuse Gold processing unless all twelve Silver manifests exist.

    This prevents a Gold _SUCCESS marker from being written for an
    incomplete source hour.
    """

    missing: list[str] = []

    windows = five_minute_windows(
        window
    )

    for silver_window in windows:
        if not silver_window_manifest_exists(
            client,
            bucket,
            silver_window,
        ):
            missing.append(
                silver_window_manifest_key(
                    silver_window
                )
            )

    if missing:
        raise RuntimeError(
            "Gold source hour is incomplete: "
            f"{len(missing)} Silver manifest(s) missing: "
            f"{missing}"
        )


def normalize_partition_columns(df):
    """Derive canonical event-time partition columns."""

    return (
        df
        .withColumn(
            "year",
            F.date_format(
                "event_time_utc",
                "yyyy",
            ),
        )
        .withColumn(
            "month",
            F.date_format(
                "event_time_utc",
                "MM",
            ),
        )
        .withColumn(
            "day",
            F.date_format(
                "event_time_utc",
                "dd",
            ),
        )
        .withColumn(
            "hour",
            F.date_format(
                "event_time_utc",
                "HH",
            ),
        )
    )


def measurement_values(df) -> list[int]:
    """Return sorted distinct measurement IDs."""

    return [
        int(row["measurement_id"])
        for row in (
            df
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


def gold_source_samples(df) -> int:
    """Return total source sample_count represented by Gold rows."""

    row = (
        df
        .agg(
            F.sum(
                "sample_count"
            ).alias(
                "source_samples"
            )
        )
        .collect()[0]
    )

    value = row[
        "source_samples"
    ]

    return (
        int(value)
        if value is not None
        else 0
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

    # PATCH03: reject partial/unsafe candidate flags BEFORE MinIO/Spark writes.
    if (args.verified_input_plan is not None or args.test_public_keys is not None
            or args.verified_plan_preflight_only or args.local_parquet_bundle is not None):
        if not (args.verified_input_plan and args.test_public_keys
                and args.verified_plan_preflight_only):
            raise RuntimeError(
                "PATCH03_VERIFIED_GOLD_EXECUTION_BLOCKED_USE_PREFLIGHT_ONLY"
            )
        from processing.gold.verified_hour_plan import verify_hour_plan_file
        result = verify_hour_plan_file(
            args.verified_input_plan, args.test_public_keys, args.start,
            args.local_parquet_bundle,
        )
        log.info("PATCH03_TEST_ONLY_GOLD_INPUT_PLAN_PREFLIGHT=%s", result)
        print("RESULT=PASS_PATCH03_TEST_ONLY_GOLD_INPUT_PREFLIGHT_NO_EXECUTION")
        return

    window = TimeWindow(
        parse_utc(
            args.start
        ),
        parse_utc(
            args.stop
        ),
    )

    validate_gold_hour_window(
        window
    )

    country_codes = sorted(
        country.code
        for country in get_enabled_countries()
    )

    if not country_codes:
        raise ValueError(
            "No enabled countries found "
            "in monitoring configuration"
        )

    measurement_ids = normalize_measurement_ids(
        args.measurement_ids
        or get_enabled_measurement_ids()
    )

    if not measurement_ids:
        raise ValueError(
            "No enabled product measurements found"
        )

    expected_measurements = list(
        measurement_ids
    )

    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    log.info(
        "Global Gold preflight "
        "window=%s..%s countries=%s "
        "measurements=%s",
        utc_text(
            window.start
        ),
        utc_text(
            window.stop
        ),
        country_codes,
        measurement_ids,
    )

    require_complete_silver_hour(
        s3,
        settings.minio_bucket,
        window,
    )

    log.info(
        "Global Gold Silver dependency PASS "
        "window=%s..%s manifests=12",
        utc_text(
            window.start
        ),
        utc_text(
            window.stop
        ),
    )

    root = (
        f"s3a://{settings.minio_bucket}"
    )

    spark = spark_session()

    spark.conf.set(
        "spark.sql.sources."
        "partitionColumnTypeInference.enabled",
        "false",
    )

    completed_countries: list[str] = []
    total_enriched_rows = 0
    total_gold_rows = 0

    try:
        for index, country_code in enumerate(
            country_codes,
            start=1,
        ):
            country_started = (
                time.monotonic()
            )

            ping = None
            enriched = None
            written_enriched = None
            gold = None
            written_gold = None

            log.info(
                "Global Gold country start "
                "country=%s position=%s/%s "
                "window=%s..%s",
                country_code,
                index,
                len(
                    country_codes
                ),
                utc_text(
                    window.start
                ),
                utc_text(
                    window.stop
                ),
            )

            silver_root = (
                f"{root}/silver/ripe_atlas"
            )

            ping_path = (
                f"{silver_root}/ping_by_country/"
                f"country={country_code}/"
            )

            probe_path = (
                f"{silver_root}/probes_by_country/"
                f"country={country_code}/"
            )

            enriched_path = (
                f"{silver_root}/ping_enriched/"
                f"country={country_code}/"
            )

            gold_path = (
                f"{root}/gold/ripe_atlas/"
                "hourly_network_quality/"
                f"country={country_code}/"
            )

            try:
                # --------------------------------------------------
                # Read exact raw Silver hour
                # --------------------------------------------------

                log.info(
                    "Reading Silver Ping "
                    "country=%s partitions=%s",
                    country_code,
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
                        "No Silver Ping partition exists "
                        f"country={country_code} "
                        f"window={utc_text(window.start)}"
                        f"..{utc_text(window.stop)}"
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

                input_rows = ping.count()

                if input_rows == 0:
                    raise ValueError(
                        "Silver Ping contains no rows "
                        f"country={country_code}"
                    )

                # --------------------------------------------------
                # Current probe dimension
                # --------------------------------------------------

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
                        "Probe Dimension missing or unreadable "
                        f"country={country_code} "
                        f"path={probe_path}"
                    ) from exc

                # --------------------------------------------------
                # Lossless enrichment
                # --------------------------------------------------

                (
                    enriched,
                    join_metrics,
                ) = enrich_ping_country(
                    ping,
                    probes,
                    measurement_ids,
                    country_code,
                )

                enriched = (
                    enriched
                    .cache()
                )

                if (
                    join_metrics.output_rows
                    != join_metrics.input_ping_rows
                ):
                    raise ValueError(
                        "Lossless enrichment invariant failed "
                        f"country={country_code}: "
                        f"input={join_metrics.input_ping_rows} "
                        f"output={join_metrics.output_rows}"
                    )

                if (
                    join_metrics.input_ping_rows
                    != input_rows
                ):
                    raise ValueError(
                        "Enrichment input row count mismatch "
                        f"country={country_code}: "
                        f"source={input_rows} "
                        f"join={join_metrics.input_ping_rows}"
                    )

                if (
                    join_metrics.unmatched_ping_rows
                ):
                    log.warning(
                        "Global Gold unmatched current probes "
                        "country=%s rows=%s "
                        "distinct=%s ids=%s",
                        country_code,
                        join_metrics.unmatched_ping_rows,
                        join_metrics.unmatched_distinct_probes,
                        join_metrics.unmatched_probe_ids,
                    )

                validate_enriched_ping(
                    enriched,
                    measurement_ids,
                    country_code,
                    join_metrics.input_ping_rows,
                )

                log.info(
                    "Global Gold enrichment "
                    "country=%s join_metrics=%s "
                    "quality=%s",
                    country_code,
                    asdict(
                        join_metrics
                    ),
                    enriched_summary(
                        enriched
                    ),
                )

                # --------------------------------------------------
                # Persist enriched Silver
                # --------------------------------------------------

                replace_measurements_in_window(
                    enriched,
                    enriched_path,
                    window,
                    measurement_ids,
                )

                persisted_enriched = (
                    existing_window_rows(
                        spark,
                        enriched_path,
                        window,
                    )
                )

                if persisted_enriched is None:
                    raise ValueError(
                        "Enriched Silver write produced "
                        "no output "
                        f"country={country_code}"
                    )

                persisted_enriched = (
                    filter_event_window(
                        persisted_enriched,
                        window,
                    )
                )

                written_enriched = (
                    filter_selected_measurements(
                        persisted_enriched,
                        measurement_ids,
                    )
                    .cache()
                )

                validate_enriched_ping(
                    written_enriched,
                    measurement_ids,
                    country_code,
                    join_metrics.input_ping_rows,
                )

                enriched_rows = (
                    written_enriched.count()
                )

                if (
                    enriched_rows
                    != input_rows
                ):
                    raise ValueError(
                        "Enriched Silver read-back "
                        "row count mismatch "
                        f"country={country_code}: "
                        f"expected={input_rows} "
                        f"actual={enriched_rows}"
                    )

                # --------------------------------------------------
                # Build hourly Gold from persisted enriched Silver
                # --------------------------------------------------

                validate_gold_input(
                    written_enriched,
                    measurement_ids,
                    country_code,
                )

                source_measurements = (
                    measurement_values(
                        written_enriched
                    )
                )

                if (
                    source_measurements
                    != expected_measurements
                ):
                    raise ValueError(
                        "Gold input measurement set mismatch "
                        f"country={country_code}: "
                        f"expected={expected_measurements} "
                        f"actual={source_measurements}"
                    )

                gold = (
                    build_hourly_network_quality(
                        written_enriched
                    )
                    .cache()
                )

                validate_hourly_gold(
                    gold,
                    measurement_ids,
                    country_code,
                )

                gold_rows = (
                    gold.count()
                )

                source_samples = (
                    gold_source_samples(
                        gold
                    )
                )

                if (
                    source_samples
                    != enriched_rows
                ):
                    raise ValueError(
                        "Gold source sample conservation "
                        "failed "
                        f"country={country_code}: "
                        f"enriched_rows={enriched_rows} "
                        f"source_samples={source_samples}"
                    )

                log.info(
                    "Global Gold aggregation "
                    "country=%s enriched_rows=%s "
                    "gold_rows=%s source_samples=%s",
                    country_code,
                    enriched_rows,
                    gold_rows,
                    source_samples,
                )

                # --------------------------------------------------
                # Persist Gold
                # --------------------------------------------------

                write_hourly_gold(
                    gold,
                    gold_path,
                )

                persisted_gold = (
                    existing_window_rows(
                        spark,
                        gold_path,
                        window,
                    )
                )

                if persisted_gold is None:
                    raise ValueError(
                        "Gold write produced no output "
                        f"country={country_code}"
                    )

                written_gold = (
                    persisted_gold
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
                    written_gold,
                    measurement_ids,
                    country_code,
                )

                written_gold_rows = (
                    written_gold.count()
                )

                if (
                    written_gold_rows
                    != gold_rows
                ):
                    raise ValueError(
                        "Gold read-back row count mismatch "
                        f"country={country_code}: "
                        f"expected={gold_rows} "
                        f"actual={written_gold_rows}"
                    )

                written_measurements = (
                    measurement_values(
                        written_gold
                    )
                )

                if (
                    written_measurements
                    != expected_measurements
                ):
                    raise ValueError(
                        "Gold read-back measurement set mismatch "
                        f"country={country_code}: "
                        f"expected={expected_measurements} "
                        f"actual={written_measurements}"
                    )

                written_source_samples = (
                    gold_source_samples(
                        written_gold
                    )
                )

                if (
                    written_source_samples
                    != enriched_rows
                ):
                    raise ValueError(
                        "Gold read-back sample conservation "
                        "failed "
                        f"country={country_code}: "
                        f"expected={enriched_rows} "
                        f"actual={written_source_samples}"
                    )

                completed_countries.append(
                    country_code
                )

                total_enriched_rows += (
                    enriched_rows
                )

                total_gold_rows += (
                    written_gold_rows
                )

                log.info(
                    "Global Gold country complete "
                    "country=%s position=%s/%s "
                    "enriched_rows=%s gold_rows=%s "
                    "source_samples=%s "
                    "duration_seconds=%.2f",
                    country_code,
                    index,
                    len(
                        country_codes
                    ),
                    enriched_rows,
                    written_gold_rows,
                    written_source_samples,
                    (
                        time.monotonic()
                        - country_started
                    ),
                )

            finally:
                if written_gold is not None:
                    written_gold.unpersist()

                if gold is not None:
                    gold.unpersist()

                if written_enriched is not None:
                    written_enriched.unpersist()

                if enriched is not None:
                    enriched.unpersist()

                if ping is not None:
                    ping.unpersist()

        # ----------------------------------------------------------
        # Global completion verification
        # ----------------------------------------------------------

        if (
            completed_countries
            != country_codes
        ):
            raise RuntimeError(
                "Global Gold completed country set mismatch: "
                f"expected={country_codes} "
                f"actual={completed_countries}"
            )

        manifest_key = (
            write_gold_window_manifest(
                client=s3,
                bucket=settings.minio_bucket,
                window=window,
                country_codes=completed_countries,
                measurement_ids=list(
                    measurement_ids
                ),
            )
        )

        log.info(
            "Global Gold manifest written "
            "window=%s..%s key=%s",
            utc_text(
                window.start
            ),
            utc_text(
                window.stop
            ),
            manifest_key,
        )

        log.info(
            "Global Gold hour complete "
            "window=%s..%s countries=%s "
            "enriched_rows=%s gold_rows=%s "
            "duration_seconds=%.2f",
            utc_text(
                window.start
            ),
            utc_text(
                window.stop
            ),
            len(
                completed_countries
            ),
            total_enriched_rows,
            total_gold_rows,
            (
                time.monotonic()
                - started
            ),
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
