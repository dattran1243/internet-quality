#!/usr/bin/env python
"""Build one global RIPE Atlas anomaly/incident hour."""

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
    get_enabled_countries,
    get_enabled_measurement_ids,
)
from configs.settings import settings
from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
    utc_text,
)

from processing.gold.incident_window_manifest import (
    validate_incident_hour_window,
    write_incident_window_manifest,
)
from processing.gold.ripe_atlas_anomaly import (
    build_hourly_anomalies,
    normalize_measurement_ids,
    validate_anomaly_output,
    write_hourly_anomalies,
)
from processing.gold.ripe_atlas_asn_degradation import (
    build_asn_degradation,
    validate_asn_degradation,
    write_asn_degradation,
)
from processing.gold.ripe_atlas_coverage_degradation import (
    build_coverage_degradation,
    validate_coverage_output,
    write_coverage_degradation,
)
from processing.gold.ripe_atlas_hourly import (
    validate_hourly_gold,
)
from processing.gold.ripe_atlas_incident_candidates import (
    build_incident_candidates,
    validate_incident_candidates,
    write_incident_candidates,
)
from processing.gold.window_manifest import (
    gold_window_manifest_exists,
)

from processing.silver.incremental import (
    existing_window_rows,
    filter_event_window,
)
from scripts.run_silver import spark_session
from storage.minio_client import create_s3_client


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build anomaly, ASN degradation, coverage degradation "
            "and incident candidates for one global RIPE Atlas hour."
        )
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
        "--measurement-id",
        action="append",
        type=int,
        dest="measurement_ids",
    )

    parser.add_argument(
        "--anomaly-lookback-hours",
        type=int,
        default=168,
    )

    parser.add_argument(
        "--anomaly-min-history-hours",
        type=int,
        default=24,
    )

    parser.add_argument(
        "--coverage-lookback-hours",
        type=int,
        default=72,
    )

    parser.add_argument(
        "--coverage-min-history-hours",
        type=int,
        default=24,
    )

    parser.add_argument(
        "--asn-min-eligible-probes",
        type=int,
        default=5,
    )

    parser.add_argument(
        "--asn-min-affected-probes",
        type=int,
        default=2,
    )

    parser.add_argument(
        "--asn-warning-ratio",
        type=float,
        default=0.30,
    )

    parser.add_argument(
        "--asn-critical-ratio",
        type=float,
        default=0.50,
    )

    parser.add_argument(
        "--coverage-warning-ratio",
        type=float,
        default=0.80,
    )

    parser.add_argument(
        "--coverage-critical-ratio",
        type=float,
        default=0.50,
    )

    parser.add_argument(
        "--coverage-min-probe-drop",
        type=int,
        default=2,
    )

    return parser.parse_args()


def rows_in_hour(
    df,
    window: TimeWindow,
):
    return df.filter(
        (
            F.col("hour_start_utc")
            >= F.lit(window.start)
        )
        & (
            F.col("hour_start_utc")
            < F.lit(window.stop)
        )
    )


def validate_source_scope(
    df,
    *,
    country_code: str,
    measurement_ids: tuple[int, ...],
    source_name: str,
) -> None:
    wrong_country = (
        df
        .filter(
            F.col("country_code").isNull()
            | (
                F.upper(
                    F.trim(
                        F.col("country_code")
                    )
                )
                != F.lit(country_code)
            )
        )
        .limit(1)
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
            ~F.col("measurement_id").isin(
                list(measurement_ids)
            )
        )
        .limit(1)
        .count()
    )

    if unexpected_measurement:
        raise ValueError(
            f"{source_name} contains unexpected "
            "measurement IDs"
        )


def measurement_values(df) -> list[int]:
    return [
        int(row["measurement_id"])
        for row in (
            df
            .select("measurement_id")
            .distinct()
            .orderBy("measurement_id")
            .collect()
        )
    ]


def persist_stage(
    *,
    spark,
    result,
    path: str,
    window: TimeWindow,
    writer,
    validator,
    measurement_ids: tuple[int, ...],
    country_code: str,
    stage_name: str,
):
    validator(
        result,
        measurement_ids,
        country_code,
    )

    expected_rows = result.count()

    if expected_rows == 0:
        raise ValueError(
            f"{stage_name} output is empty "
            f"country={country_code}"
        )

    writer(
        result,
        path,
    )

    written = existing_window_rows(
        spark,
        path,
        window,
    )

    if written is None:
        raise ValueError(
            f"{stage_name} write produced no output "
            f"country={country_code}"
        )

    written = (
        rows_in_hour(
            written,
            window,
        )
        .cache()
    )

    validator(
        written,
        measurement_ids,
        country_code,
    )

    actual_rows = written.count()

    if actual_rows != expected_rows:
        raise ValueError(
            f"{stage_name} read-back row count mismatch "
            f"country={country_code}: "
            f"expected={expected_rows} "
            f"actual={actual_rows}"
        )

    return written, actual_rows


def validate_arguments(
    args: argparse.Namespace,
) -> None:
    positive_values = {
        "anomaly-lookback-hours":
            args.anomaly_lookback_hours,
        "anomaly-min-history-hours":
            args.anomaly_min_history_hours,
        "coverage-lookback-hours":
            args.coverage_lookback_hours,
        "coverage-min-history-hours":
            args.coverage_min_history_hours,
        "asn-min-eligible-probes":
            args.asn_min_eligible_probes,
        "asn-min-affected-probes":
            args.asn_min_affected_probes,
        "coverage-min-probe-drop":
            args.coverage_min_probe_drop,
    }

    for name, value in positive_values.items():
        if value <= 0:
            raise ValueError(
                f"{name} must be positive"
            )

    if (
        args.anomaly_min_history_hours
        > args.anomaly_lookback_hours
    ):
        raise ValueError(
            "anomaly-min-history-hours cannot exceed "
            "anomaly-lookback-hours"
        )

    if (
        args.coverage_min_history_hours
        > args.coverage_lookback_hours
    ):
        raise ValueError(
            "coverage-min-history-hours cannot exceed "
            "coverage-lookback-hours"
        )

    for name, value in {
        "asn-warning-ratio":
            args.asn_warning_ratio,
        "asn-critical-ratio":
            args.asn_critical_ratio,
        "coverage-warning-ratio":
            args.coverage_warning_ratio,
        "coverage-critical-ratio":
            args.coverage_critical_ratio,
    }.items():
        if not 0.0 <= value <= 1.0:
            raise ValueError(
                f"{name} must be between 0 and 1"
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

    validate_arguments(args)

    window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )

    validate_incident_hour_window(window)

    country_codes = sorted(
        country.code
        for country in get_enabled_countries()
    )

    if not country_codes:
        raise ValueError(
            "No enabled countries configured"
        )

    measurement_ids = normalize_measurement_ids(
        args.measurement_ids
        or get_enabled_measurement_ids()
    )

    expected_measurements = list(
        measurement_ids
    )

    anomaly_history_window = TimeWindow(
        window.start
        - timedelta(
            hours=args.anomaly_lookback_hours
        ),
        window.stop,
    )

    coverage_history_start = (
        window.start
        - timedelta(
            hours=args.coverage_lookback_hours
        )
    )

    coverage_history_window = TimeWindow(
        coverage_history_start,
        window.stop,
    )

    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    if not gold_window_manifest_exists(
        s3,
        settings.minio_bucket,
        window,
    ):
        raise RuntimeError(
            "Incident processing requires completed "
            "global Gold manifest for "
            f"{utc_text(window.start)}.."
            f"{utc_text(window.stop)}"
        )

    log.info(
        "Global incident preflight "
        "window=%s..%s countries=%s "
        "measurements=%s "
        "anomaly_lookback=%s "
        "coverage_lookback=%s",
        utc_text(window.start),
        utc_text(window.stop),
        country_codes,
        measurement_ids,
        args.anomaly_lookback_hours,
        args.coverage_lookback_hours,
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

    total_anomaly_rows = 0
    total_asn_rows = 0
    total_coverage_rows = 0
    total_incident_rows = 0
    total_candidate_rows = 0

    try:
        for index, country_code in enumerate(
            country_codes,
            start=1,
        ):
            country_started = time.monotonic()

            gold_history = None
            current_gold = None
            anomaly = None
            written_anomaly = None
            asn = None
            written_asn = None
            silver_history = None
            coverage = None
            written_coverage = None
            incident = None
            written_incident = None

            log.info(
                "Global incident country start "
                "country=%s position=%s/%s "
                "window=%s..%s",
                country_code,
                index,
                len(country_codes),
                utc_text(window.start),
                utc_text(window.stop),
            )

            gold_root = (
                f"{root}/gold/ripe_atlas"
            )

            silver_root = (
                f"{root}/silver/ripe_atlas"
            )

            gold_path = (
                f"{gold_root}/hourly_network_quality/"
                f"country={country_code}/"
            )

            anomaly_path = (
                f"{gold_root}/hourly_anomalies/"
                f"country={country_code}/"
            )

            asn_path = (
                f"{gold_root}/asn_degradation/"
                f"country={country_code}/"
            )

            coverage_path = (
                f"{gold_root}/coverage_degradation/"
                f"country={country_code}/"
            )

            incident_path = (
                f"{gold_root}/incident_candidates/"
                f"country={country_code}/"
            )

            silver_path = (
                f"{silver_root}/ping_by_country/"
                f"country={country_code}/"
            )

            try:
                # -----------------------------------------
                # Gold history / anomaly
                # -----------------------------------------

                gold_history = existing_window_rows(
                    spark,
                    gold_path,
                    anomaly_history_window,
                )

                if gold_history is None:
                    raise ValueError(
                        "No Gold history exists "
                        f"country={country_code}"
                    )

                gold_history = gold_history.cache()

                validate_source_scope(
                    gold_history,
                    country_code=country_code,
                    measurement_ids=measurement_ids,
                    source_name="Gold history",
                )

                current_gold = (
                    rows_in_hour(
                        gold_history,
                        window,
                    )
                    .cache()
                )

                validate_hourly_gold(
                    current_gold,
                    measurement_ids,
                    country_code,
                )

                current_gold_rows = (
                    current_gold.count()
                )

                if current_gold_rows == 0:
                    raise ValueError(
                        "Current Gold hour is empty "
                        f"country={country_code}"
                    )

                if (
                    measurement_values(
                        current_gold
                    )
                    != expected_measurements
                ):
                    raise ValueError(
                        "Current Gold measurement set mismatch "
                        f"country={country_code}"
                    )

                anomaly = (
                    build_hourly_anomalies(
                        gold_history,
                        lookback_hours=(
                            args.anomaly_lookback_hours
                        ),
                        min_history_hours=(
                            args.anomaly_min_history_hours
                        ),
                    )
                    .filter(
                        (
                            F.col("hour_start_utc")
                            >= F.lit(window.start)
                        )
                        & (
                            F.col("hour_start_utc")
                            < F.lit(window.stop)
                        )
                    )
                    .cache()
                )

                anomaly_rows = anomaly.count()

                if anomaly_rows != current_gold_rows:
                    raise ValueError(
                        "Anomaly/Gold row conservation failed "
                        f"country={country_code}: "
                        f"gold={current_gold_rows} "
                        f"anomaly={anomaly_rows}"
                    )

                (
                    written_anomaly,
                    anomaly_rows,
                ) = persist_stage(
                    spark=spark,
                    result=anomaly,
                    path=anomaly_path,
                    window=window,
                    writer=write_hourly_anomalies,
                    validator=validate_anomaly_output,
                    measurement_ids=measurement_ids,
                    country_code=country_code,
                    stage_name="anomaly",
                )

                # -----------------------------------------
                # ASN degradation
                # -----------------------------------------

                asn = (
                    build_asn_degradation(
                        written_anomaly,
                        min_eligible_probes=(
                            args.asn_min_eligible_probes
                        ),
                        min_affected_probes=(
                            args.asn_min_affected_probes
                        ),
                        warning_ratio=(
                            args.asn_warning_ratio
                        ),
                        critical_ratio=(
                            args.asn_critical_ratio
                        ),
                    )
                    .cache()
                )

                (
                    written_asn,
                    asn_rows,
                ) = persist_stage(
                    spark=spark,
                    result=asn,
                    path=asn_path,
                    window=window,
                    writer=write_asn_degradation,
                    validator=validate_asn_degradation,
                    measurement_ids=measurement_ids,
                    country_code=country_code,
                    stage_name="ASN degradation",
                )

                # -----------------------------------------
                # Silver history / coverage
                # -----------------------------------------

                silver_history = existing_window_rows(
                    spark,
                    silver_path,
                    coverage_history_window,
                )

                if silver_history is None:
                    raise ValueError(
                        "No Silver history exists "
                        f"country={country_code}"
                    )

                silver_history = (
                    filter_event_window(
                        silver_history,
                        coverage_history_window,
                    )
                    .cache()
                )

                validate_source_scope(
                    silver_history,
                    country_code=country_code,
                    measurement_ids=measurement_ids,
                    source_name="Silver coverage history",
                )

                current_silver_rows = (
                    silver_history
                    .filter(
                        (
                            F.col("event_time_utc")
                            >= F.lit(window.start)
                        )
                        & (
                            F.col("event_time_utc")
                            < F.lit(window.stop)
                        )
                    )
                    .count()
                )

                if current_silver_rows == 0:
                    raise ValueError(
                        "Current Silver coverage hour is empty "
                        f"country={country_code}"
                    )

                coverage = (
                    build_coverage_degradation(
                        silver_history,
                        measurement_ids,
                        country_code=country_code,
                        series_start=(
                            coverage_history_start
                        ),
                        series_stop=window.stop,
                        lookback_hours=(
                            args.coverage_lookback_hours
                        ),
                        min_history_hours=(
                            args.coverage_min_history_hours
                        ),
                        warning_ratio=(
                            args.coverage_warning_ratio
                        ),
                        critical_ratio=(
                            args.coverage_critical_ratio
                        ),
                        min_probe_drop=(
                            args.coverage_min_probe_drop
                        ),
                    )
                    .filter(
                        (
                            F.col("hour_start_utc")
                            >= F.lit(window.start)
                        )
                        & (
                            F.col("hour_start_utc")
                            < F.lit(window.stop)
                        )
                    )
                    .cache()
                )

                expected_grid_rows = len(
                    measurement_ids
                )

                coverage_rows = coverage.count()

                if (
                    coverage_rows
                    != expected_grid_rows
                ):
                    raise ValueError(
                        "Coverage grid row count mismatch "
                        f"country={country_code}: "
                        f"expected={expected_grid_rows} "
                        f"actual={coverage_rows}"
                    )

                (
                    written_coverage,
                    coverage_rows,
                ) = persist_stage(
                    spark=spark,
                    result=coverage,
                    path=coverage_path,
                    window=window,
                    writer=write_coverage_degradation,
                    validator=validate_coverage_output,
                    measurement_ids=measurement_ids,
                    country_code=country_code,
                    stage_name="coverage degradation",
                )

                if (
                    measurement_values(
                        written_coverage
                    )
                    != expected_measurements
                ):
                    raise ValueError(
                        "Coverage measurement set mismatch "
                        f"country={country_code}"
                    )

                # -----------------------------------------
                # Incident candidates
                # -----------------------------------------

                incident = (
                    build_incident_candidates(
                        written_anomaly,
                        written_asn,
                        written_coverage,
                    )
                    .cache()
                )

                incident_rows = (
                    incident.count()
                )

                if (
                    incident_rows
                    != expected_grid_rows
                ):
                    raise ValueError(
                        "Incident grid row count mismatch "
                        f"country={country_code}: "
                        f"expected={expected_grid_rows} "
                        f"actual={incident_rows}"
                    )

                (
                    written_incident,
                    incident_rows,
                ) = persist_stage(
                    spark=spark,
                    result=incident,
                    path=incident_path,
                    window=window,
                    writer=write_incident_candidates,
                    validator=validate_incident_candidates,
                    measurement_ids=measurement_ids,
                    country_code=country_code,
                    stage_name="incident candidates",
                )

                if (
                    measurement_values(
                        written_incident
                    )
                    != expected_measurements
                ):
                    raise ValueError(
                        "Incident measurement set mismatch "
                        f"country={country_code}"
                    )

                candidate_rows = (
                    written_incident
                    .filter(
                        F.col(
                            "incident_candidate"
                        )
                    )
                    .count()
                )

                status_rows = (
                    written_incident
                    .groupBy(
                        "incident_status"
                    )
                    .count()
                    .orderBy(
                        "incident_status"
                    )
                    .collect()
                )

                status_distribution = {
                    str(row["incident_status"]):
                        int(row["count"])
                    for row in status_rows
                }

                completed_countries.append(
                    country_code
                )

                total_anomaly_rows += (
                    anomaly_rows
                )

                total_asn_rows += (
                    asn_rows
                )

                total_coverage_rows += (
                    coverage_rows
                )

                total_incident_rows += (
                    incident_rows
                )

                total_candidate_rows += (
                    candidate_rows
                )

                log.info(
                    "Global incident country complete "
                    "country=%s position=%s/%s "
                    "anomaly_rows=%s asn_rows=%s "
                    "coverage_rows=%s incident_rows=%s "
                    "candidate_rows=%s statuses=%s "
                    "duration_seconds=%.2f",
                    country_code,
                    index,
                    len(country_codes),
                    anomaly_rows,
                    asn_rows,
                    coverage_rows,
                    incident_rows,
                    candidate_rows,
                    status_distribution,
                    (
                        time.monotonic()
                        - country_started
                    ),
                )

            finally:
                for df in (
                    written_incident,
                    incident,
                    written_coverage,
                    coverage,
                    silver_history,
                    written_asn,
                    asn,
                    written_anomaly,
                    anomaly,
                    current_gold,
                    gold_history,
                ):
                    if df is not None:
                        df.unpersist()

        if (
            completed_countries
            != country_codes
        ):
            raise RuntimeError(
                "Global incident completed country set mismatch: "
                f"expected={country_codes} "
                f"actual={completed_countries}"
            )

        expected_incident_rows = (
            len(country_codes)
            * len(measurement_ids)
        )

        if (
            total_incident_rows
            != expected_incident_rows
        ):
            raise RuntimeError(
                "Global incident row count mismatch: "
                f"expected={expected_incident_rows} "
                f"actual={total_incident_rows}"
            )

        manifest_key = (
            write_incident_window_manifest(
                client=s3,
                bucket=settings.minio_bucket,
                window=window,
                country_codes=(
                    completed_countries
                ),
                measurement_ids=list(
                    measurement_ids
                ),
                row_count=(
                    total_incident_rows
                ),
                incident_candidate_count=(
                    total_candidate_rows
                ),
            )
        )

        log.info(
            "Global incident manifest written "
            "window=%s..%s key=%s",
            utc_text(window.start),
            utc_text(window.stop),
            manifest_key,
        )

        log.info(
            "Global anomaly/incident hour complete "
            "window=%s..%s countries=%s "
            "anomaly_rows=%s asn_rows=%s "
            "coverage_rows=%s incident_rows=%s "
            "candidate_rows=%s "
            "duration_seconds=%.2f",
            utc_text(window.start),
            utc_text(window.stop),
            len(completed_countries),
            total_anomaly_rows,
            total_asn_rows,
            total_coverage_rows,
            total_incident_rows,
            total_candidate_rows,
            (
                time.monotonic()
                - started
            ),
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
