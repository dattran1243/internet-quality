"""Hourly Gold aggregation for country-scoped RIPE Atlas Ping."""

from __future__ import annotations

from typing import Iterable


PARTITION_COLUMNS = (
    "year",
    "month",
    "day",
    "hour",
)


# Country is part of the analytical grain.
#
# This prevents rows from different country scopes from being
# considered the same analytical entity if they are ever combined.
GOLD_KEY_COLUMNS = (
    "hour_start_utc",
    "country_code",
    "measurement_id",
    "probe_id",
    "target",
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


def validate_gold_input(
    df,
    measurement_ids: Iterable[int],
    country_code: str | None = None,
) -> None:
    """
    Validate country-scoped enriched Silver before aggregation.

    country_code remains optional for backward compatibility with
    existing callers while runners migrate to explicit country scope.
    """
    from pyspark.sql import functions as F

    ids = _measurement_ids(
        measurement_ids
    )

    expected_country = (
        _country_code(country_code)
        if country_code is not None
        else None
    )

    if (
        df.limit(1).count()
        == 0
    ):
        raise ValueError(
            "Gold input is empty"
        )

    unexpected_measurement = (
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

    if unexpected_measurement:
        raise ValueError(
            "Gold input contains measurement IDs "
            "outside the selected product scope"
        )

    invalid_identity = (
        df
        .filter(
            F.col(
                "event_time_utc"
            ).isNull()
            | F.col(
                "measurement_id"
            ).isNull()
            | (
                F.col(
                    "measurement_id"
                )
                <= 0
            )
            | F.col(
                "probe_id"
            ).isNull()
            | (
                F.col(
                    "probe_id"
                )
                <= 0
            )
            | F.col(
                "target"
            ).isNull()
            | F.col(
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

    if invalid_identity:
        raise ValueError(
            "Gold input contains invalid "
            "identity/time/country fields"
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
                "Gold input contains rows outside "
                f"country={expected_country}"
            )


def build_hourly_network_quality(
    silver_df,
):
    """
    Aggregate enriched Silver to one row per:

        hour
        + country
        + measurement
        + probe
        + target
    """
    from pyspark.sql import functions as F

    # Invalid Silver rows must not influence analytical metrics.
    valid = (
        silver_df
        .filter(
            F.col(
                "validation_status"
            )
            == "valid"
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
    )

    bucketed = (
        valid
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
        bucketed
        .groupBy(
            *GOLD_KEY_COLUMNS
        )
        .agg(
            # Number of RIPE results in this hour.
            F.count(
                F.lit(1)
            ).alias(
                "sample_count"
            ),

            # RTT can legitimately be NULL for 100% loss.
            F.count(
                "rtt_avg_ms"
            ).alias(
                "rtt_sample_count"
            ),

            F.count(
                "packet_loss_pct"
            ).alias(
                "loss_sample_count"
            ),

            F.avg(
                "rtt_avg_ms"
            ).alias(
                "avg_rtt_ms"
            ),

            F.percentile_approx(
                "rtt_avg_ms",
                0.50,
                10000,
            ).alias(
                "median_rtt_ms"
            ),

            F.percentile_approx(
                "rtt_avg_ms",
                0.95,
                10000,
            ).alias(
                "p95_rtt_ms"
            ),

            F.min(
                "rtt_avg_ms"
            ).alias(
                "min_rtt_ms"
            ),

            F.max(
                "rtt_avg_ms"
            ).alias(
                "max_rtt_ms"
            ),

            F.stddev_pop(
                "rtt_avg_ms"
            ).alias(
                "rtt_stddev_ms"
            ),

            F.avg(
                "packet_loss_pct"
            ).alias(
                "avg_packet_loss_pct"
            ),

            F.max(
                "packet_loss_pct"
            ).alias(
                "max_packet_loss_pct"
            ),

            F.sum(
                "packets_sent"
            ).alias(
                "total_packets_sent"
            ),

            F.sum(
                "packets_received"
            ).alias(
                "total_packets_received"
            ),

            # Probe metadata.
            F.first(
                "asn_v4",
                ignorenulls=True,
            ).alias(
                "asn_v4"
            ),

            F.first(
                "asn_v6",
                ignorenulls=True,
            ).alias(
                "asn_v6"
            ),

            F.first(
                "latitude",
                ignorenulls=True,
            ).alias(
                "latitude"
            ),

            F.first(
                "longitude",
                ignorenulls=True,
            ).alias(
                "longitude"
            ),

            F.first(
                "is_anchor",
                ignorenulls=True,
            ).alias(
                "is_anchor"
            ),

            # Useful lineage.
            F.max(
                "ingested_at_utc"
            ).alias(
                "source_max_ingested_at_utc"
            ),

            F.max(
                "probe_snapshot_at_utc"
            ).alias(
                "probe_snapshot_at_utc"
            ),
        )
    )

    hourly = (
        hourly
        .withColumn(
            "weighted_packet_loss_pct",
            F.when(
                F.col(
                    "total_packets_sent"
                )
                > 0,
                (
                    (
                        F.col(
                            "total_packets_sent"
                        )
                        - F.col(
                            "total_packets_received"
                        )
                    )
                    * F.lit(
                        100.0
                    )
                    / F.col(
                        "total_packets_sent"
                    )
                ),
            )
            .otherwise(
                F.lit(
                    None
                ).cast(
                    "double"
                )
            ),
        )
    )

    # Canonical zero-padded partitions.
    return (
        hourly
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
    )


def validate_hourly_gold(
    df,
    measurement_ids: Iterable[int],
    country_code: str | None = None,
) -> None:
    """Validate country-aware Gold analytical invariants."""
    from pyspark.sql import functions as F

    ids = _measurement_ids(
        measurement_ids
    )

    expected_country = (
        _country_code(country_code)
        if country_code is not None
        else None
    )

    if (
        df.limit(1).count()
        == 0
    ):
        raise ValueError(
            "Hourly Gold output is empty"
        )

    unexpected_measurement = (
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

    if unexpected_measurement:
        raise ValueError(
            "Hourly Gold contains "
            "unexpected measurement IDs"
        )

    bad_country = (
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

    if bad_country:
        raise ValueError(
            "Hourly Gold contains "
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
                "Hourly Gold contains rows outside "
                f"country={expected_country}"
            )

    bad_metrics = (
        df
        .filter(
            F.col(
                "hour_start_utc"
            ).isNull()
            | F.col(
                "probe_id"
            ).isNull()
            | F.col(
                "measurement_id"
            ).isNull()
            | (
                F.col(
                    "sample_count"
                )
                <= 0
            )
            | (
                F.col(
                    "rtt_sample_count"
                )
                < 0
            )
            | (
                F.col(
                    "rtt_sample_count"
                )
                > F.col(
                    "sample_count"
                )
            )
            | (
                F.col(
                    "loss_sample_count"
                )
                < 0
            )
            | (
                F.col(
                    "loss_sample_count"
                )
                > F.col(
                    "sample_count"
                )
            )
            | (
                F.col(
                    "avg_rtt_ms"
                ).isNotNull()
                & (
                    F.col(
                        "avg_rtt_ms"
                    )
                    < 0
                )
            )
            | (
                F.col(
                    "p95_rtt_ms"
                ).isNotNull()
                & (
                    F.col(
                        "p95_rtt_ms"
                    )
                    < 0
                )
            )
            | (
                F.col(
                    "avg_packet_loss_pct"
                ).isNotNull()
                & (
                    (
                        F.col(
                            "avg_packet_loss_pct"
                        )
                        < 0
                    )
                    | (
                        F.col(
                            "avg_packet_loss_pct"
                        )
                        > 100
                    )
                )
            )
            | (
                F.col(
                    "max_packet_loss_pct"
                ).isNotNull()
                & (
                    (
                        F.col(
                            "max_packet_loss_pct"
                        )
                        < 0
                    )
                    | (
                        F.col(
                            "max_packet_loss_pct"
                        )
                        > 100
                    )
                )
            )
        )
        .limit(1)
        .count()
    )

    if bad_metrics:
        raise ValueError(
            "Hourly Gold contains "
            "invalid analytical metrics"
        )

    duplicate_key = (
        df
        .groupBy(
            *GOLD_KEY_COLUMNS
        )
        .count()
        .filter(
            F.col(
                "count"
            )
            > 1
        )
        .limit(1)
        .count()
    )

    if duplicate_key:
        raise ValueError(
            "Hourly Gold contains "
            "duplicate analytical keys"
        )


def write_hourly_gold(
    df,
    target: str,
) -> None:
    """
    Incrementally replace only partitions present in df.

    Safety across countries is provided by using a separate target
    root for each country, for example:

        .../hourly_network_quality/country=VN/
        .../hourly_network_quality/country=DE/
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
