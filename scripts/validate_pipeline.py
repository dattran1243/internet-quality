#!/usr/bin/env python
"""Validate persisted country-scoped RIPE Atlas Ping Silver and Gold."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)

from configs.monitoring import (
    DEFAULT_COUNTRY_CODE,
    MEASUREMENTS,
    get_country,
    get_enabled_measurement_ids,
    is_product_measurement,
)
from configs.settings import settings
from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
    utc_text,
)
from processing.silver.incremental import (
    hourly_partition_paths,
    validate_hour_aligned_window,
)
from processing.silver.ripe_atlas_ping import SILVER_COLUMNS
from scripts.run_silver import spark_session


SILVER_EXTRA_COLUMNS = {
    "country_code",
    "probe_scope_key",
    "year",
    "month",
    "day",
    "hour",
}

GOLD_REQUIRED_COLUMNS = {
    "hour_start_utc",
    "measurement_id",
    "probe_id",
    "target",
    "country_code",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate persisted country-scoped RIPE Atlas "
            "Ping Silver and Gold data."
        )
    )

    parser.add_argument(
        "--country-code",
        default=DEFAULT_COUNTRY_CODE,
        help=f"Configured country code. Default: {DEFAULT_COUNTRY_CODE}",
    )

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
        dest="measurement_ids",
        help=(
            "Product measurement ID. Repeat for multiple IDs. "
            "Default: all enabled product measurements."
        ),
    )

    parser.add_argument(
        "--layer",
        choices=("silver", "gold", "all"),
        default="all",
        help="Layer to validate. Default: all",
    )

    return parser.parse_args()


def selected_window(args: argparse.Namespace) -> TimeWindow:
    window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )

    validate_hour_aligned_window(window)

    return window


def selected_measurement_ids(
    values: list[int] | None,
) -> list[int]:
    raw = values or get_enabled_measurement_ids()

    measurement_ids = list(
        dict.fromkeys(int(value) for value in raw)
    )

    require(
        bool(measurement_ids),
        "No product measurements selected",
    )

    invalid = [
        measurement_id
        for measurement_id in measurement_ids
        if not is_product_measurement(measurement_id)
    ]

    require(
        not invalid,
        "Non-product or disabled measurement IDs selected: "
        f"{sorted(invalid)}",
    )

    return measurement_ids


def target_expression(f, measurement_ids: list[int]):
    items = []

    for measurement_id in measurement_ids:
        items.extend(
            [
                f.lit(measurement_id).cast("long"),
                f.lit(
                    MEASUREMENTS[measurement_id].target
                ),
            ]
        )

    return f.element_at(
        f.create_map(*items),
        f.col("measurement_id"),
    )


def path_exists(spark, path: str) -> bool:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    java_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)

    filesystem = java_path.getFileSystem(
        hadoop_conf
    )

    return bool(
        filesystem.exists(java_path)
    )


def read_hourly_window(
    spark,
    base_path: str,
    window: TimeWindow,
):
    paths = hourly_partition_paths(
        base_path,
        window,
    )

    missing = [
        path
        for path in paths
        if not path_exists(spark, path)
    ]

    require(
        not missing,
        "Missing hourly partitions:\n"
        + "\n".join(missing),
    )

    return (
        spark.read
        .option(
            "basePath",
            base_path,
        )
        .parquet(*paths)
    )


def assert_exact_country(
    df,
    country_code: str,
) -> None:
    from pyspark.sql import functions as f

    invalid = (
        df.filter(
            f.col("country_code").isNull()
            | (
                f.upper(
                    f.col("country_code")
                )
                != country_code
            )
        )
        .limit(1)
        .count()
    )

    require(
        invalid == 0,
        f"Rows outside country={country_code}",
    )


def assert_measurement_scope(
    df,
    measurement_ids: list[int],
) -> None:
    from pyspark.sql import functions as f

    outside = (
        df.filter(
            f.col("measurement_id").isNull()
            | ~f.col("measurement_id").isin(
                measurement_ids
            )
        )
        .limit(1)
        .count()
    )

    require(
        outside == 0,
        "Rows outside requested measurement scope",
    )

    actual = sorted(
        int(row.measurement_id)
        for row in (
            df.select(
                "measurement_id"
            )
            .distinct()
            .collect()
        )
        if row.measurement_id is not None
    )

    require(
        actual == sorted(measurement_ids),
        "Measurement scope mismatch: "
        f"expected={sorted(measurement_ids)} "
        f"actual={actual}",
    )


def assert_logical_targets(
    df,
    measurement_ids: list[int],
) -> None:
    from pyspark.sql import functions as f

    expected_target = target_expression(
        f,
        measurement_ids,
    )

    mismatch = (
        df.filter(
            ~f.col("target").eqNullSafe(
                expected_target
            )
        )
        .limit(1)
        .count()
    )

    require(
        mismatch == 0,
        "Logical target mismatch detected",
    )


def validate_silver(
    spark,
    country_code: str,
    window: TimeWindow,
    measurement_ids: list[int],
) -> int:
    from pyspark.sql import functions as f

    base_path = (
        f"s3a://{settings.minio_bucket}/"
        "silver/ripe_atlas/"
        "ping_by_country/"
        f"country={country_code}/"
    )

    df = read_hourly_window(
        spark,
        base_path,
        window,
    ).cache()

    try:
        required_columns = (
            set(SILVER_COLUMNS)
            | SILVER_EXTRA_COLUMNS
        )

        missing_columns = sorted(
            required_columns
            - set(df.columns)
        )

        require(
            not missing_columns,
            "Silver schema missing columns: "
            f"{missing_columns}",
        )

        row_count = df.count()

        require(
            row_count > 0,
            "Silver row count is zero",
        )

        assert_exact_country(
            df,
            country_code,
        )

        assert_measurement_scope(
            df,
            measurement_ids,
        )

        assert_logical_targets(
            df,
            measurement_ids,
        )

        outside_window = (
            df.filter(
                f.col(
                    "event_time_utc"
                ).isNull()
                | (
                    f.col("event_time_utc")
                    < f.lit(window.start)
                )
                | (
                    f.col("event_time_utc")
                    >= f.lit(window.stop)
                )
            )
            .limit(1)
            .count()
        )

        require(
            outside_window == 0,
            "Silver contains rows outside [start, stop)",
        )

        invalid_status = (
            df.filter(
                ~f.col(
                    "validation_status"
                ).isin(
                    "valid",
                    "invalid",
                )
                | f.col(
                    "validation_status"
                ).isNull()
            )
            .limit(1)
            .count()
        )

        require(
            invalid_status == 0,
            "Unexpected Silver validation_status",
        )

        missing_scope_key = (
            df.filter(
                f.col(
                    "probe_scope_key"
                ).isNull()
                | (
                    f.length(
                        f.trim(
                            f.col(
                                "probe_scope_key"
                            )
                        )
                    )
                    == 0
                )
            )
            .limit(1)
            .count()
        )

        require(
            missing_scope_key == 0,
            "Silver probe_scope_key is null or empty",
        )

        negative_rtt = (
            (f.col("rtt_min_ms") < 0)
            | (f.col("rtt_avg_ms") < 0)
            | (f.col("rtt_max_ms") < 0)
        )

        zero_received_negative_rtt = (
            df.filter(
                (f.col("packets_received") == 0)
                & negative_rtt
            )
            .limit(1)
            .count()
        )

        require(
            zero_received_negative_rtt == 0,
            "Negative RTT sentinel was not normalized "
            "for packets_received=0",
        )

        unflagged_negative_rtt = (
            df.filter(
                negative_rtt
                & (
                    f.col("validation_status")
                    != "invalid"
                )
            )
            .limit(1)
            .count()
        )

        require(
            unflagged_negative_rtt == 0,
            "Negative RTT row is not marked invalid",
        )

        invalid_valid_counters = (
            df.filter(
                (f.col("validation_status") == "valid")
                & (
                    (f.col("packets_sent") < 0)
                    | (f.col("packets_received") < 0)
                    | (
                        f.col("packets_received")
                        > f.col("packets_sent")
                    )
                )
            )
            .limit(1)
            .count()
        )

        require(
            invalid_valid_counters == 0,
            "Valid Silver row has invalid packet counters",
        )

        invalid_valid_loss = (
            df.filter(
                (f.col("validation_status") == "valid")
                & f.col("packet_loss_pct").isNotNull()
                & ~f.col(
                    "packet_loss_pct"
                ).between(
                    0.0,
                    100.0,
                )
            )
            .limit(1)
            .count()
        )

        require(
            invalid_valid_loss == 0,
            "Valid Silver row has packet loss "
            "outside [0, 100]",
        )

        partition_mismatch = (
            df.filter(
                (
                    f.col("year").cast("int")
                    != f.year(
                        f.col("event_time_utc")
                    )
                )
                | (
                    f.col("month").cast("int")
                    != f.month(
                        f.col("event_time_utc")
                    )
                )
                | (
                    f.col("day").cast("int")
                    != f.dayofmonth(
                        f.col("event_time_utc")
                    )
                )
                | (
                    f.col("hour").cast("int")
                    != f.hour(
                        f.col("event_time_utc")
                    )
                )
            )
            .limit(1)
            .count()
        )

        require(
            partition_mismatch == 0,
            "Silver partition fields do not match "
            "event_time_utc",
        )

        destination_equals_target = (
            df.filter(
                f.col(
                    "destination_address"
                ).isNotNull()
                & (
                    f.col("destination_address")
                    == f.col("target")
                )
            )
            .limit(1)
            .count()
        )

        require(
            destination_equals_target == 0,
            "destination_address equals logical target; "
            "target/destination semantics may be mixed",
        )

        return row_count

    finally:
        df.unpersist()


def expected_hour_measurements(
    window: TimeWindow,
    measurement_ids: list[int],
) -> set[tuple[str, int]]:
    from datetime import timedelta

    expected: set[tuple[str, int]] = set()

    current = window.start

    while current < window.stop:
        hour_text = current.strftime(
            "%Y-%m-%d %H:00:00"
        )

        for measurement_id in measurement_ids:
            expected.add(
                (
                    hour_text,
                    measurement_id,
                )
            )

        current += timedelta(hours=1)

    return expected


def validate_gold(
    spark,
    country_code: str,
    window: TimeWindow,
    measurement_ids: list[int],
) -> int:
    from pyspark.sql import functions as f

    base_path = (
        f"s3a://{settings.minio_bucket}/"
        "gold/ripe_atlas/"
        "hourly_network_quality/"
        f"country={country_code}/"
    )

    df = read_hourly_window(
        spark,
        base_path,
        window,
    ).cache()

    try:
        missing_columns = sorted(
            GOLD_REQUIRED_COLUMNS
            - set(df.columns)
        )

        require(
            not missing_columns,
            "Gold schema missing columns: "
            f"{missing_columns}",
        )

        row_count = df.count()

        require(
            row_count > 0,
            "Gold row count is zero",
        )

        assert_exact_country(
            df,
            country_code,
        )

        assert_measurement_scope(
            df,
            measurement_ids,
        )

        assert_logical_targets(
            df,
            measurement_ids,
        )

        outside_window = (
            df.filter(
                f.col(
                    "hour_start_utc"
                ).isNull()
                | (
                    f.col("hour_start_utc")
                    < f.lit(window.start)
                )
                | (
                    f.col("hour_start_utc")
                    >= f.lit(window.stop)
                )
            )
            .limit(1)
            .count()
        )

        require(
            outside_window == 0,
            "Gold contains rows outside [start, stop)",
        )

        non_hour_aligned = (
            df.filter(
                f.col("hour_start_utc")
                != f.date_trunc(
                    "hour",
                    f.col("hour_start_utc"),
                )
            )
            .limit(1)
            .count()
        )

        require(
            non_hour_aligned == 0,
            "Gold hour_start_utc is not hour aligned",
        )

        duplicate_probe_grain = (
            df.groupBy(
                "hour_start_utc",
                "measurement_id",
                "probe_id",
            )
            .count()
            .filter(
                f.col("count") > 1
            )
            .limit(1)
            .count()
        )

        require(
            duplicate_probe_grain == 0,
            "Gold duplicate detected at "
            "(hour, measurement, probe) grain",
        )

        actual_hour_measurements = {
            (
                row.hour_text,
                int(row.measurement_id),
            )
            for row in (
                df.select(
                    f.date_format(
                        "hour_start_utc",
                        "yyyy-MM-dd HH:mm:ss",
                    ).alias(
                        "hour_text"
                    ),
                    "measurement_id",
                )
                .distinct()
                .collect()
            )
        }

        expected = expected_hour_measurements(
            window,
            measurement_ids,
        )

        missing_hour_measurements = sorted(
            expected
            - actual_hour_measurements
        )

        require(
            not missing_hour_measurements,
            "Missing Gold hour/measurement combinations: "
            f"{missing_hour_measurements}",
        )

        return row_count

    finally:
        df.unpersist()


def main() -> None:
    logging.basicConfig(
        level=settings.log_level,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    args = arguments()

    country = get_country(
        args.country_code
    )

    window = selected_window(
        args
    )

    measurement_ids = selected_measurement_ids(
        args.measurement_ids
    )

    spark = spark_session()

    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )

    spark.conf.set(
        "spark.sql.sources."
        "partitionColumnTypeInference.enabled",
        "false",
    )

    try:
        print(
            "=== PIPELINE VALIDATION ==="
        )
        print(
            f"country={country.code}"
        )
        print(
            "window="
            f"{utc_text(window.start)}"
            ".."
            f"{utc_text(window.stop)}"
        )
        print(
            f"measurements={measurement_ids}"
        )

        if args.layer in (
            "silver",
            "all",
        ):
            silver_rows = validate_silver(
                spark,
                country.code,
                window,
                measurement_ids,
            )

            print(
                f"silver_rows={silver_rows}"
            )
            print(
                "silver_validation=PASS"
            )

        if args.layer in (
            "gold",
            "all",
        ):
            gold_rows = validate_gold(
                spark,
                country.code,
                window,
                measurement_ids,
            )

            print(
                f"gold_rows={gold_rows}"
            )
            print(
                "gold_probe_grain_duplicates=0"
            )
            print(
                "gold_validation=PASS"
            )

        print(
            "PIPELINE VALIDATION PASSED"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()