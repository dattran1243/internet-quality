"""Aggregate country-scoped probe anomalies into ASN degradation signals."""

from __future__ import annotations

from typing import Iterable


PARTITION_COLUMNS = (
    "year",
    "month",
    "day",
    "hour",
)

ASN_KEY_COLUMNS = (
    "hour_start_utc",
    "country_code",
    "measurement_id",
    "target",
    "asn_v4",
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


def build_asn_degradation(
    anomaly_df,
    *,
    min_eligible_probes: int = 5,
    min_affected_probes: int = 2,
    warning_ratio: float = 0.30,
    critical_ratio: float = 0.50,
):
    """
    Build one ASN-level signal per:

        hour + country + measurement + target + ASN

    Important:
    - insufficient_history probes do NOT enter anomaly denominator.
    - ASN with too few eligible probes is not classified as degradation.
    - This is a degradation signal, not proof that the ASN caused it.
    """

    from pyspark.sql import functions as F

    if min_eligible_probes <= 0:
        raise ValueError(
            "min_eligible_probes must be positive"
        )

    if min_affected_probes <= 0:
        raise ValueError(
            "min_affected_probes must be positive"
        )

    if not 0 < warning_ratio <= 1:
        raise ValueError(
            "warning_ratio must be in (0, 1]"
        )

    if not 0 < critical_ratio <= 1:
        raise ValueError(
            "critical_ratio must be in (0, 1]"
        )

    if warning_ratio > critical_ratio:
        raise ValueError(
            "warning_ratio cannot exceed critical_ratio"
        )

    # For MVP, rows without IPv4 ASN cannot be assigned
    # to an ASN-level signal.
    scoped = (
        anomaly_df
        .filter(
            F.col(
                "asn_v4"
            ).isNotNull()
            & (
                F.col(
                    "asn_v4"
                )
                > 0
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
    )

    aggregated = (
        scoped
        .groupBy(
            *ASN_KEY_COLUMNS
        )
        .agg(
            F.countDistinct(
                "probe_id"
            ).alias(
                "observed_probe_count"
            ),

            F.countDistinct(
                F.when(
                    F.col(
                        "status"
                    )
                    != "insufficient_history",
                    F.col(
                        "probe_id"
                    ),
                )
            ).alias(
                "eligible_probe_count"
            ),

            F.countDistinct(
                F.when(
                    F.col(
                        "status"
                    )
                    == "insufficient_history",
                    F.col(
                        "probe_id"
                    ),
                )
            ).alias(
                "insufficient_history_probe_count"
            ),

            F.countDistinct(
                F.when(
                    F.col(
                        "status"
                    ).isin(
                        "warning",
                        "critical",
                    ),
                    F.col(
                        "probe_id"
                    ),
                )
            ).alias(
                "affected_probe_count"
            ),

            F.countDistinct(
                F.when(
                    F.col(
                        "status"
                    )
                    == "critical",
                    F.col(
                        "probe_id"
                    ),
                )
            ).alias(
                "critical_probe_count"
            ),

            F.avg(
                F.when(
                    F.col(
                        "status"
                    )
                    != "insufficient_history",
                    F.col(
                        "latency_ratio"
                    ),
                )
            ).alias(
                "avg_latency_ratio"
            ),

            F.max(
                "latency_ratio"
            ).alias(
                "max_latency_ratio"
            ),

            F.avg(
                F.when(
                    F.col(
                        "status"
                    )
                    != "insufficient_history",
                    F.col(
                        "latency_delta_ms"
                    ),
                )
            ).alias(
                "avg_latency_delta_ms"
            ),

            F.max(
                "latency_delta_ms"
            ).alias(
                "max_latency_delta_ms"
            ),

            F.avg(
                F.when(
                    F.col(
                        "status"
                    )
                    != "insufficient_history",
                    F.col(
                        "avg_packet_loss_pct"
                    ),
                )
            ).alias(
                "avg_packet_loss_pct"
            ),

            F.max(
                "avg_packet_loss_pct"
            ).alias(
                "max_packet_loss_pct"
            ),

            F.avg(
                F.when(
                    F.col(
                        "status"
                    )
                    != "insufficient_history",
                    F.col(
                        "loss_delta_pct_points"
                    ),
                )
            ).alias(
                "avg_loss_delta_pct_points"
            ),
        )
    )

    aggregated = (
        aggregated
        .withColumn(
            "eligible_ratio",
            F.when(
                F.col(
                    "observed_probe_count"
                )
                > 0,
                (
                    F.col(
                        "eligible_probe_count"
                    )
                    / F.col(
                        "observed_probe_count"
                    )
                ),
            ),
        )
        .withColumn(
            "affected_ratio",
            F.when(
                F.col(
                    "eligible_probe_count"
                )
                > 0,
                (
                    F.col(
                        "affected_probe_count"
                    )
                    / F.col(
                        "eligible_probe_count"
                    )
                ),
            ),
        )
        .withColumn(
            "critical_ratio",
            F.when(
                F.col(
                    "eligible_probe_count"
                )
                > 0,
                (
                    F.col(
                        "critical_probe_count"
                    )
                    / F.col(
                        "eligible_probe_count"
                    )
                ),
            ),
        )
    )

    enough_coverage = (
        F.col(
            "eligible_probe_count"
        )
        >= F.lit(
            min_eligible_probes
        )
    )

    warning_condition = (
        enough_coverage
        & (
            F.col(
                "affected_probe_count"
            )
            >= F.lit(
                min_affected_probes
            )
        )
        & (
            F.col(
                "affected_ratio"
            )
            >= F.lit(
                warning_ratio
            )
        )
    )

    # Preserve the original critical semantics:
    # - enough eligible probes
    # - enough CRITICAL probes
    # - overall affected ratio reaches critical threshold
    critical_condition = (
        enough_coverage
        & (
            F.col(
                "critical_probe_count"
            )
            >= F.lit(
                min_affected_probes
            )
        )
        & (
            F.col(
                "affected_ratio"
            )
            >= F.lit(
                critical_ratio
            )
        )
    )

    result = (
        aggregated
        .withColumn(
            "asn_signal_status",
            F.when(
                ~enough_coverage,
                F.lit(
                    "insufficient_coverage"
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
                    "asn_signal_status"
                )
                == "critical",
                F.lit(
                    2
                ),
            )
            .when(
                F.col(
                    "asn_signal_status"
                )
                == "warning",
                F.lit(
                    1
                ),
            )
            .when(
                F.col(
                    "asn_signal_status"
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
                    "asn_signal_status"
                )
                == "critical",
                F.lit(
                    "multiple probes in ASN show "
                    "critical degradation"
                ),
            )
            .when(
                F.col(
                    "asn_signal_status"
                )
                == "warning",
                F.lit(
                    "multiple probes in ASN show "
                    "degradation"
                ),
            )
            .when(
                F.col(
                    "asn_signal_status"
                )
                == "insufficient_coverage",
                F.lit(
                    "too few eligible probes "
                    "for ASN-level classification"
                ),
            )
            .otherwise(
                F.lit(
                    "no ASN-level degradation"
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
    )

    return result


def validate_asn_degradation(
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
            "ASN degradation output is empty"
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
            "ASN degradation contains "
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
                "ASN degradation contains rows outside "
                f"country={country}"
            )

    invalid = (
        df
        .filter(
            F.col(
                "asn_v4"
            ).isNull()
            | (
                F.col(
                    "asn_v4"
                )
                <= 0
            )
            | (
                F.col(
                    "observed_probe_count"
                )
                <= 0
            )
            | (
                F.col(
                    "eligible_probe_count"
                )
                < 0
            )
            | (
                F.col(
                    "eligible_probe_count"
                )
                > F.col(
                    "observed_probe_count"
                )
            )
            | (
                F.col(
                    "insufficient_history_probe_count"
                )
                < 0
            )
            | (
                F.col(
                    "affected_probe_count"
                )
                < 0
            )
            | (
                F.col(
                    "affected_probe_count"
                )
                > F.col(
                    "eligible_probe_count"
                )
            )
            | (
                F.col(
                    "critical_probe_count"
                )
                < 0
            )
            | (
                F.col(
                    "critical_probe_count"
                )
                > F.col(
                    "affected_probe_count"
                )
            )
            | (
                F.col(
                    "eligible_ratio"
                ).isNotNull()
                & ~F.col(
                    "eligible_ratio"
                ).between(
                    0.0,
                    1.0,
                )
            )
            | (
                F.col(
                    "affected_ratio"
                ).isNotNull()
                & ~F.col(
                    "affected_ratio"
                ).between(
                    0.0,
                    1.0,
                )
            )
            | (
                F.col(
                    "critical_ratio"
                ).isNotNull()
                & ~F.col(
                    "critical_ratio"
                ).between(
                    0.0,
                    1.0,
                )
            )
            | (
                ~F.col(
                    "asn_signal_status"
                ).isin(
                    "insufficient_coverage",
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
            "Invalid ASN degradation metrics"
        )

    duplicates = (
        df
        .groupBy(
            *ASN_KEY_COLUMNS
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

    if duplicates:
        raise ValueError(
            "Duplicate ASN degradation analytical keys"
        )


def write_asn_degradation(
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