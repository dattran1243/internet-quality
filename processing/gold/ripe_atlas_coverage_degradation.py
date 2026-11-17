"""Country-scoped measurement-level probe coverage degradation detection."""

from __future__ import annotations

from datetime import timedelta
from typing import Iterable


PARTITION_COLUMNS = (
    "year",
    "month",
    "day",
    "hour",
)


COVERAGE_KEY_COLUMNS = (
    "hour_start_utc",
    "country_code",
    "measurement_id",
)


def _measurement_ids(
    values: Iterable[int],
) -> tuple[int, ...]:
    ids = tuple(
        sorted(
            set(
                int(value)
                for value in values
            )
        )
    )

    if (
        not ids
        or any(
            value <= 0
            for value in ids
        )
    ):
        raise ValueError(
            "measurement_ids must contain positive IDs"
        )

    return ids


def _country_code(
    value: str,
) -> str:
    if not isinstance(
        value,
        str,
    ):
        raise ValueError(
            "country_code must be a string"
        )

    normalized = (
        value
        .strip()
        .upper()
    )

    if (
        len(normalized) != 2
        or not normalized.isalpha()
    ):
        raise ValueError(
            "country_code must be a two-letter code"
        )

    return normalized


def _validate_series_window(
    start,
    stop,
) -> None:
    if start >= stop:
        raise ValueError(
            "series_start must be before series_stop"
        )

    if (
        start.minute != 0
        or start.second != 0
        or start.microsecond != 0
        or stop.minute != 0
        or stop.second != 0
        or stop.microsecond != 0
    ):
        raise ValueError(
            "Coverage series must use whole-hour boundaries"
        )


def build_coverage_degradation(
    silver_ping_df,
    measurement_ids: Iterable[int],
    *,
    country_code: str,
    series_start,
    series_stop,
    lookback_hours: int = 72,
    min_history_hours: int = 24,
    warning_ratio: float = 0.80,
    critical_ratio: float = 0.50,
    min_probe_drop: int = 2,
):
    """
    Build hourly measurement-level coverage signals for one country.

    Coverage analytical grain:

        hour + country + measurement

    Important:
    - Historical hours with no source data are NOT counted as
      baseline observations.
    - A missing current hour is still represented with
      observed_probe_count=0.
    - Coverage degradation is a measurement/data-availability
      signal; it does not by itself prove an Internet outage.
    """

    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    ids = _measurement_ids(
        measurement_ids
    )

    country = _country_code(
        country_code
    )

    _validate_series_window(
        series_start,
        series_stop,
    )

    if lookback_hours <= 0:
        raise ValueError(
            "lookback_hours must be positive"
        )

    if min_history_hours <= 0:
        raise ValueError(
            "min_history_hours must be positive"
        )

    if min_history_hours > lookback_hours:
        raise ValueError(
            "min_history_hours cannot exceed lookback_hours"
        )

    if not 0 < critical_ratio <= warning_ratio <= 1:
        raise ValueError(
            "Require 0 < critical_ratio <= warning_ratio <= 1"
        )

    if min_probe_drop <= 0:
        raise ValueError(
            "min_probe_drop must be positive"
        )

    spark = silver_ping_df.sparkSession

    scoped = (
        silver_ping_df
        .filter(
            F.col(
                "event_time_utc"
            ).isNotNull()
            & F.col(
                "measurement_id"
            ).isin(
                list(
                    ids
                )
            )
            & F.col(
                "probe_id"
            ).isNotNull()
            & (
                F.col(
                    "probe_id"
                )
                > 0
            )
            & F.col(
                "country_code"
            ).isNotNull()
            & (
                F.upper(
                    F.trim(
                        F.col(
                            "country_code"
                        )
                    )
                )
                == F.lit(
                    country
                )
            )
        )
        .withColumn(
            "country_code",
            F.upper(
                F.trim(
                    F.col(
                        "country_code"
                    )
                )
            ),
        )
        .withColumn(
            "hour_start_utc",
            F.date_trunc(
                "hour",
                F.col(
                    "event_time_utc"
                ),
            ),
        )
    )

    hourly = (
        scoped
        .groupBy(
            "hour_start_utc",
            "country_code",
            "measurement_id",
        )
        .agg(
            F.countDistinct(
                "probe_id"
            ).alias(
                "_observed_probe_count"
            ),

            F.count(
                F.lit(
                    1
                )
            ).alias(
                "_result_count"
            ),

            F.first(
                "target",
                ignorenulls=True,
            ).alias(
                "_hourly_target"
            ),
        )
    )

    target_map = (
        scoped
        .groupBy(
            "country_code",
            "measurement_id",
        )
        .agg(
            F.first(
                "target",
                ignorenulls=True,
            ).alias(
                "_mapped_target"
            )
        )
    )

    # Complete hour x measurement grid for this country.
    #
    # This is intentional: if an entire source hour disappears,
    # coverage must still emit a row with zero observed probes.
    grid_rows = []

    cursor = series_start

    while cursor < series_stop:
        for measurement_id in ids:
            grid_rows.append(
                (
                    cursor,
                    country,
                    measurement_id,
                )
            )

        cursor += timedelta(
            hours=1
        )

    grid_schema = T.StructType(
        [
            T.StructField(
                "hour_start_utc",
                T.TimestampType(),
                False,
            ),

            T.StructField(
                "country_code",
                T.StringType(),
                False,
            ),

            T.StructField(
                "measurement_id",
                T.LongType(),
                False,
            ),
        ]
    )

    grid = spark.createDataFrame(
        grid_rows,
        schema=grid_schema,
    )

    series = (
        grid
        .join(
            target_map,
            on=[
                "country_code",
                "measurement_id",
            ],
            how="left",
        )
        .join(
            hourly,
            on=[
                "hour_start_utc",
                "country_code",
                "measurement_id",
            ],
            how="left",
        )
        .withColumn(
            "source_hour_present",
            F.col(
                "_observed_probe_count"
            ).isNotNull(),
        )
        .withColumn(
            "target",
            F.coalesce(
                F.col(
                    "_hourly_target"
                ),
                F.col(
                    "_mapped_target"
                ),
            ),
        )
        .withColumn(
            "observed_probe_count",
            F.coalesce(
                F.col(
                    "_observed_probe_count"
                ),
                F.lit(
                    0
                ),
            ).cast(
                "long"
            ),
        )
        .withColumn(
            "result_count",
            F.coalesce(
                F.col(
                    "_result_count"
                ),
                F.lit(
                    0
                ),
            ).cast(
                "long"
            ),
        )
        .drop(
            "_observed_probe_count",
            "_result_count",
            "_hourly_target",
            "_mapped_target",
        )
    )

    series = (
        series
        .withColumn(
            "avg_results_per_probe",
            F.when(
                F.col(
                    "observed_probe_count"
                )
                > 0,
                (
                    F.col(
                        "result_count"
                    )
                    / F.col(
                        "observed_probe_count"
                    )
                ),
            ),
        )
        .withColumn(
            "_hour_epoch",
            F.col(
                "hour_start_utc"
            ).cast(
                "long"
            ),
        )
    )

    seconds = (
        lookback_hours
        * 3600
    )

    # Explicit country-aware baseline.
    #
    # The current hour is excluded by rangeBetween(..., -1).
    baseline_window = (
        Window
        .partitionBy(
            "country_code",
            "measurement_id",
        )
        .orderBy(
            F.col(
                "_hour_epoch"
            )
        )
        .rangeBetween(
            -seconds,
            -1,
        )
    )

    valid_probe_count = (
        F.when(
            F.col(
                "source_hour_present"
            ),
            F.col(
                "observed_probe_count"
            ).cast(
                "double"
            ),
        )
    )

    valid_result_count = (
        F.when(
            F.col(
                "source_hour_present"
            ),
            F.col(
                "result_count"
            ).cast(
                "double"
            ),
        )
    )

    result = (
        series
        .withColumn(
            "baseline_history_count",
            F.count(
                F.when(
                    F.col(
                        "source_hour_present"
                    ),
                    F.lit(
                        1
                    ),
                )
            ).over(
                baseline_window
            ),
        )
        .withColumn(
            "baseline_probe_count_mean",
            F.avg(
                valid_probe_count
            ).over(
                baseline_window
            ),
        )
        .withColumn(
            "baseline_probe_count_median",
            F.percentile_approx(
                valid_probe_count,
                0.50,
                10000,
            ).over(
                baseline_window
            ),
        )
        .withColumn(
            "baseline_result_count_median",
            F.percentile_approx(
                valid_result_count,
                0.50,
                10000,
            ).over(
                baseline_window
            ),
        )
    )

    result = (
        result
        .withColumn(
            "probe_coverage_ratio",
            F.when(
                F.col(
                    "baseline_probe_count_median"
                )
                > 0,
                (
                    F.col(
                        "observed_probe_count"
                    )
                    / F.col(
                        "baseline_probe_count_median"
                    )
                ),
            ),
        )
        .withColumn(
            "probe_drop_count",
            F.when(
                F.col(
                    "baseline_probe_count_median"
                ).isNotNull(),
                F.greatest(
                    (
                        F.col(
                            "baseline_probe_count_median"
                        )
                        - F.col(
                            "observed_probe_count"
                        )
                    ),
                    F.lit(
                        0.0
                    ),
                ),
            ),
        )
        .withColumn(
            "probe_drop_pct",
            F.when(
                F.col(
                    "baseline_probe_count_median"
                )
                > 0,
                (
                    F.col(
                        "probe_drop_count"
                    )
                    * F.lit(
                        100.0
                    )
                    / F.col(
                        "baseline_probe_count_median"
                    )
                ),
            ),
        )
        .withColumn(
            "result_coverage_ratio",
            F.when(
                F.col(
                    "baseline_result_count_median"
                )
                > 0,
                (
                    F.col(
                        "result_count"
                    )
                    / F.col(
                        "baseline_result_count_median"
                    )
                ),
            ),
        )
    )

    enough_history = (
        F.col(
            "baseline_history_count"
        )
        >= F.lit(
            min_history_hours
        )
    )

    warning_condition = (
        enough_history
        & (
            F.col(
                "baseline_probe_count_median"
            )
            > 0
        )
        & (
            F.col(
                "probe_coverage_ratio"
            )
            <= F.lit(
                warning_ratio
            )
        )
        & (
            F.col(
                "probe_drop_count"
            )
            >= F.lit(
                float(
                    min_probe_drop
                )
            )
        )
    )

    critical_condition = (
        enough_history
        & (
            F.col(
                "baseline_probe_count_median"
            )
            > 0
        )
        & (
            F.col(
                "probe_coverage_ratio"
            )
            <= F.lit(
                critical_ratio
            )
        )
        & (
            F.col(
                "probe_drop_count"
            )
            >= F.lit(
                float(
                    min_probe_drop
                )
            )
        )
    )

    result = (
        result
        .withColumn(
            "coverage_anomaly",
            warning_condition,
        )
        .withColumn(
            "coverage_status",
            F.when(
                ~enough_history,
                F.lit(
                    "insufficient_history"
                ),
            )
            .when(
                critical_condition,
                F.lit(
                    "critical"
                ),
            )
            .when(
                warning_condition,
                F.lit(
                    "warning"
                ),
            )
            .otherwise(
                F.lit(
                    "normal"
                ),
            ),
        )
        .withColumn(
            "severity_score",
            F.when(
                F.col(
                    "coverage_status"
                )
                == "critical",
                F.lit(
                    2
                ),
            )
            .when(
                F.col(
                    "coverage_status"
                )
                == "warning",
                F.lit(
                    1
                ),
            )
            .when(
                F.col(
                    "coverage_status"
                )
                == "normal",
                F.lit(
                    0
                ),
            )
            .otherwise(
                F.lit(
                    None
                ).cast(
                    "integer"
                ),
            ),
        )
        .withColumn(
            "signal_reason",
            F.when(
                F.col(
                    "coverage_status"
                )
                == "insufficient_history",
                F.lit(
                    "not enough historical "
                    "coverage observations"
                ),
            )
            .when(
                ~F.col(
                    "source_hour_present"
                ),
                F.lit(
                    "measurement hour missing "
                    "from country-scoped Silver Ping"
                ),
            )
            .when(
                F.col(
                    "coverage_status"
                )
                == "critical",
                F.lit(
                    "probe coverage far below "
                    "rolling measurement baseline"
                ),
            )
            .when(
                F.col(
                    "coverage_status"
                )
                == "warning",
                F.lit(
                    "probe coverage below "
                    "rolling measurement baseline"
                ),
            )
            .otherwise(
                F.lit(
                    "coverage within baseline"
                ),
            ),
        )
        .withColumn(
            "year",
            F.date_format(
                "hour_start_utc",
                "yyyy",
            ),
        )
        .withColumn(
            "month",
            F.date_format(
                "hour_start_utc",
                "MM",
            ),
        )
        .withColumn(
            "day",
            F.date_format(
                "hour_start_utc",
                "dd",
            ),
        )
        .withColumn(
            "hour",
            F.date_format(
                "hour_start_utc",
                "HH",
            ),
        )
        .drop(
            "_hour_epoch"
        )
    )

    return result


def validate_coverage_output(
    df,
    measurement_ids: Iterable[int],
    country_code: str | None = None,
) -> None:
    from pyspark.sql import functions as F

    ids = _measurement_ids(
        measurement_ids
    )

    country = (
        _country_code(
            country_code
        )
        if country_code
        is not None
        else None
    )

    if (
        df
        .limit(
            1
        )
        .count()
        == 0
    ):
        raise ValueError(
            "Coverage output is empty"
        )

    unexpected = (
        df
        .filter(
            ~F.col(
                "measurement_id"
            ).isin(
                list(
                    ids
                )
            )
        )
        .limit(
            1
        )
        .count()
    )

    if unexpected:
        raise ValueError(
            "Coverage output contains "
            "unexpected measurement IDs"
        )

    if country is not None:
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
                        country
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
                "Coverage output contains rows outside "
                f"country={country}"
            )

    invalid = (
        df
        .filter(
            F.col(
                "hour_start_utc"
            ).isNull()
            | (
                F.col(
                    "observed_probe_count"
                )
                < 0
            )
            | (
                F.col(
                    "result_count"
                )
                < 0
            )
            | (
                F.col(
                    "baseline_history_count"
                )
                < 0
            )
            | (
                F.col(
                    "probe_drop_count"
                ).isNotNull()
                & (
                    F.col(
                        "probe_drop_count"
                    )
                    < 0
                )
            )
            | (
                F.col(
                    "probe_coverage_ratio"
                ).isNotNull()
                & (
                    F.col(
                        "probe_coverage_ratio"
                    )
                    < 0
                )
            )
            | (
                ~F.col(
                    "coverage_status"
                ).isin(
                    "insufficient_history",
                    "normal",
                    "warning",
                    "critical",
                )
            )
        )
        .limit(
            1
        )
        .count()
    )

    if invalid:
        raise ValueError(
            "Coverage output contains invalid metrics"
        )

    inconsistent_missing_hour = (
        df
        .filter(
            (
                ~F.col(
                    "source_hour_present"
                )
            )
            & (
                (
                    F.col(
                        "observed_probe_count"
                    )
                    != 0
                )
                | (
                    F.col(
                        "result_count"
                    )
                    != 0
                )
            )
        )
        .limit(
            1
        )
        .count()
    )

    if inconsistent_missing_hour:
        raise ValueError(
            "Missing source hour must have zero "
            "probe and result counts"
        )

    duplicate = (
        df
        .groupBy(
            *COVERAGE_KEY_COLUMNS
        )
        .count()
        .filter(
            F.col(
                "count"
            )
            > 1
        )
        .limit(
            1
        )
        .count()
    )

    if duplicate:
        raise ValueError(
            "Coverage output contains duplicate keys"
        )


def write_coverage_degradation(
    df,
    target: str,
) -> None:
    spark = df.sparkSession

    spark.conf.set(
        "spark.sql.sources."
        "partitionColumnTypeInference.enabled",
        "false",
    )

    spark.conf.set(
        "spark.sql.sources."
        "partitionOverwriteMode",
        "dynamic",
    )

    (
        df.write
        .mode(
            "overwrite"
        )
        .partitionBy(
            *PARTITION_COLUMNS
        )
        .parquet(
            target
        )
    )