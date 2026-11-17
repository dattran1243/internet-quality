"""Country-aware rolling baseline and anomaly detection for RIPE Atlas."""

from __future__ import annotations

from typing import Iterable


PARTITION_COLUMNS = (
    "year",
    "month",
    "day",
    "hour",
)


def normalize_measurement_ids(
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
    country = (
        str(value)
        .strip()
        .upper()
    )

    if (
        len(country) != 2
        or not country.isalpha()
    ):
        raise ValueError(
            "country_code must be a two-letter code"
        )

    return country


def build_hourly_anomalies(
    gold_df,
    *,
    lookback_hours: int = 168,
    min_history_hours: int = 24,
):
    """
    Add rolling country/probe/measurement baseline and anomaly status.

    Baseline uses PRIOR hours only.
    Current hour never contributes to its own baseline.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

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

    df = (
        gold_df
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

    # Explicit country isolation in the baseline grain.
    baseline_window = (
        Window
        .partitionBy(
            "country_code",
            "probe_id",
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

    result = (
        df

        # How much usable history exists?
        .withColumn(
            "baseline_history_count",
            F.count(
                "avg_rtt_ms"
            ).over(
                baseline_window
            ),
        )

        .withColumn(
            "baseline_loss_history_count",
            F.count(
                "avg_packet_loss_pct"
            ).over(
                baseline_window
            ),
        )

        # RTT baseline.
        .withColumn(
            "baseline_rtt_mean_ms",
            F.avg(
                "avg_rtt_ms"
            ).over(
                baseline_window
            ),
        )

        .withColumn(
            "baseline_rtt_median_ms",
            F.percentile_approx(
                "avg_rtt_ms",
                0.50,
                10000,
            ).over(
                baseline_window
            ),
        )

        .withColumn(
            "baseline_rtt_stddev_ms",
            F.stddev_pop(
                "avg_rtt_ms"
            ).over(
                baseline_window
            ),
        )

        # Packet-loss baseline.
        .withColumn(
            "baseline_loss_mean_pct",
            F.avg(
                "avg_packet_loss_pct"
            ).over(
                baseline_window
            ),
        )

        .withColumn(
            "baseline_loss_median_pct",
            F.percentile_approx(
                "avg_packet_loss_pct",
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
            "latency_delta_ms",
            (
                F.col(
                    "avg_rtt_ms"
                )
                - F.col(
                    "baseline_rtt_median_ms"
                )
            ),
        )

        .withColumn(
            "latency_ratio",
            F.when(
                (
                    F.col(
                        "baseline_rtt_median_ms"
                    ).isNotNull()
                )
                & (
                    F.col(
                        "baseline_rtt_median_ms"
                    )
                    > 0
                )
                & F.col(
                    "avg_rtt_ms"
                ).isNotNull(),
                (
                    F.col(
                        "avg_rtt_ms"
                    )
                    / F.col(
                        "baseline_rtt_median_ms"
                    )
                ),
            ),
        )

        .withColumn(
            "latency_z_score",
            F.when(
                (
                    F.col(
                        "baseline_rtt_stddev_ms"
                    ).isNotNull()
                )
                & (
                    F.col(
                        "baseline_rtt_stddev_ms"
                    )
                    > 0
                )
                & F.col(
                    "avg_rtt_ms"
                ).isNotNull(),
                (
                    (
                        F.col(
                            "avg_rtt_ms"
                        )
                        - F.col(
                            "baseline_rtt_mean_ms"
                        )
                    )
                    / F.col(
                        "baseline_rtt_stddev_ms"
                    )
                ),
            ),
        )

        .withColumn(
            "loss_delta_pct_points",
            (
                F.col(
                    "avg_packet_loss_pct"
                )
                - F.col(
                    "baseline_loss_mean_pct"
                )
            ),
        )
    )

    enough_rtt_history = (
        F.col(
            "baseline_history_count"
        )
        >= F.lit(
            min_history_hours
        )
    )

    enough_any_history = (
        enough_rtt_history
        | (
            F.col(
                "baseline_loss_history_count"
            )
            >= F.lit(
                min_history_hours
            )
        )
    )

    latency_warning = (
        enough_rtt_history
        & F.col(
            "avg_rtt_ms"
        ).isNotNull()
        & (
            (
                F.col(
                    "latency_ratio"
                )
                >= 1.50
            )
            | (
                F.col(
                    "latency_z_score"
                )
                >= 3.0
            )
        )
        & (
            F.col(
                "latency_delta_ms"
            )
            >= 10.0
        )
    )

    latency_critical = (
        enough_rtt_history
        & F.col(
            "avg_rtt_ms"
        ).isNotNull()
        & (
            (
                F.col(
                    "latency_ratio"
                )
                >= 2.00
            )
            | (
                F.col(
                    "latency_z_score"
                )
                >= 5.0
            )
        )
        & (
            F.col(
                "latency_delta_ms"
            )
            >= 20.0
        )
    )

    enough_loss_history = (
        F.col(
            "baseline_loss_history_count"
        )
        >= F.lit(
            min_history_hours
        )
    )

    loss_warning = (
        enough_loss_history
        & F.col(
            "avg_packet_loss_pct"
        ).isNotNull()
        & (
            F.col(
                "avg_packet_loss_pct"
            )
            >= 5.0
        )
        & (
            F.col(
                "loss_delta_pct_points"
            )
            >= 5.0
        )
    )

    loss_critical = (
        enough_loss_history
        & F.col(
            "avg_packet_loss_pct"
        ).isNotNull()
        & (
            F.col(
                "avg_packet_loss_pct"
            )
            >= 20.0
        )
        & (
            F.col(
                "loss_delta_pct_points"
            )
            >= 20.0
        )
    )

    result = (
        result

        .withColumn(
            "latency_anomaly",
            latency_warning,
        )

        .withColumn(
            "packet_loss_anomaly",
            loss_warning,
        )

        .withColumn(
            "status",
            F.when(
                ~enough_any_history,
                F.lit(
                    "insufficient_history"
                ),
            )
            .when(
                latency_critical
                | loss_critical,
                F.lit(
                    "critical"
                ),
            )
            .when(
                latency_warning
                | loss_warning,
                F.lit(
                    "warning"
                ),
            )
            .otherwise(
                F.lit(
                    "normal"
                )
            ),
        )

        .withColumn(
            "severity_score",
            F.when(
                F.col(
                    "status"
                )
                == "critical",
                F.lit(
                    2
                ),
            )
            .when(
                F.col(
                    "status"
                )
                == "warning",
                F.lit(
                    1
                ),
            )
            .when(
                F.col(
                    "status"
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
            "anomaly_reasons",
            F.concat_ws(
                "; ",
                F.when(
                    latency_warning,
                    F.lit(
                        "latency elevated versus "
                        "probe-measurement baseline"
                    ),
                ),
                F.when(
                    loss_warning,
                    F.lit(
                        "packet loss elevated versus "
                        "probe-measurement baseline"
                    ),
                ),
            ),
        )

        .drop(
            "_hour_epoch"
        )
    )

    return result


def validate_anomaly_output(
    df,
    measurement_ids: Iterable[int],
    country_code: str | None = None,
) -> None:
    from pyspark.sql import functions as F

    ids = normalize_measurement_ids(
        measurement_ids
    )

    expected_country = (
        _country_code(
            country_code
        )
        if country_code is not None
        else None
    )

    if (
        df.limit(1).count()
        == 0
    ):
        raise ValueError(
            "Anomaly output is empty"
        )

    unexpected = (
        df
        .filter(
            ~F.col(
                "measurement_id"
            ).isin(
                list(ids)
            )
        )
        .limit(1)
        .count()
    )

    if unexpected:
        raise ValueError(
            "Anomaly output contains "
            "unexpected measurement IDs"
        )

    invalid_country = (
        df
        .filter(
            F.col(
                "country_code"
            ).isNull()
            | (
                F.length(
                    F.trim(
                        F.col(
                            "country_code"
                        )
                    )
                )
                != 2
            )
        )
        .limit(1)
        .count()
    )

    if invalid_country:
        raise ValueError(
            "Anomaly output contains "
            "invalid country_code"
        )

    if expected_country is not None:
        wrong_country = (
            df
            .filter(
                F.upper(
                    F.trim(
                        F.col(
                            "country_code"
                        )
                    )
                )
                != expected_country
            )
            .limit(1)
            .count()
        )

        if wrong_country:
            raise ValueError(
                "Anomaly output contains rows outside "
                f"country={expected_country}"
            )

    invalid_status = (
        df
        .filter(
            ~F.col(
                "status"
            ).isin(
                [
                    "insufficient_history",
                    "normal",
                    "warning",
                    "critical",
                ]
            )
        )
        .limit(1)
        .count()
    )

    if invalid_status:
        raise ValueError(
            "Anomaly output contains invalid status"
        )

    bad_history = (
        df
        .filter(
            (
                F.col(
                    "baseline_history_count"
                )
                < 0
            )
            | (
                F.col(
                    "baseline_loss_history_count"
                )
                < 0
            )
        )
        .limit(1)
        .count()
    )

    if bad_history:
        raise ValueError(
            "Invalid baseline history counts"
        )


def write_hourly_anomalies(
    df,
    target: str,
) -> None:
    """
    Incrementally replace only time partitions present in df.

    The caller must provide a country-isolated target root such as:

        .../hourly_anomalies/country=VN/
        .../hourly_anomalies/country=DE/
    """
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