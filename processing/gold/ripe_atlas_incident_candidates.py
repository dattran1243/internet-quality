"""Combine country-scoped probe, ASN, and coverage signals into incident candidates."""

from __future__ import annotations

from typing import Iterable


PARTITION_COLUMNS = (
    "year",
    "month",
    "day",
    "hour",
)


INCIDENT_KEY = (
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


def build_incident_candidates(
    probe_anomaly_df,
    asn_degradation_df,
    coverage_df,
):
    """
    Build one incident signal per:

        hour + country + measurement

    Important:
    - Probe-only anomalies remain observe.
    - ASN or coverage corroboration may raise warning/critical.
    - These are incident candidates/signals, not root-cause claims.
    """

    from pyspark.sql import functions as F

    # ---------------------------------------------------------
    # 1. Probe-level summary
    # ---------------------------------------------------------
    probe_summary = (
        probe_anomaly_df
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
        .groupBy(
            "hour_start_utc",
            "country_code",
            "measurement_id",
        )
        .agg(
            F.first(
                "target",
                ignorenulls=True,
            ).alias(
                "probe_target"
            ),

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
                    == "warning",
                    F.col(
                        "probe_id"
                    ),
                )
            ).alias(
                "warning_probe_count"
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
                    ).isin(
                        "warning",
                        "critical",
                    ),
                    F.col(
                        "asn_v4"
                    ),
                )
            ).alias(
                "probe_affected_asn_count"
            ),

            F.max(
                "latency_ratio"
            ).alias(
                "max_probe_latency_ratio"
            ),

            F.max(
                "latency_delta_ms"
            ).alias(
                "max_probe_latency_delta_ms"
            ),

            F.max(
                "avg_packet_loss_pct"
            ).alias(
                "max_probe_packet_loss_pct"
            ),
        )
    )

    # ---------------------------------------------------------
    # 2. ASN-level summary
    # ---------------------------------------------------------
    asn_summary = (
        asn_degradation_df
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
        .groupBy(
            "hour_start_utc",
            "country_code",
            "measurement_id",
        )
        .agg(
            F.first(
                "target",
                ignorenulls=True,
            ).alias(
                "asn_target"
            ),

            F.countDistinct(
                "asn_v4"
            ).alias(
                "observed_asn_count"
            ),

            F.countDistinct(
                F.when(
                    F.col(
                        "asn_signal_status"
                    )
                    != "insufficient_coverage",
                    F.col(
                        "asn_v4"
                    ),
                )
            ).alias(
                "eligible_asn_count"
            ),

            F.countDistinct(
                F.when(
                    F.col(
                        "asn_signal_status"
                    )
                    == "warning",
                    F.col(
                        "asn_v4"
                    ),
                )
            ).alias(
                "warning_asn_count"
            ),

            F.countDistinct(
                F.when(
                    F.col(
                        "asn_signal_status"
                    )
                    == "critical",
                    F.col(
                        "asn_v4"
                    ),
                )
            ).alias(
                "critical_asn_count"
            ),

            F.countDistinct(
                F.when(
                    F.col(
                        "asn_signal_status"
                    ).isin(
                        "warning",
                        "critical",
                    ),
                    F.col(
                        "asn_v4"
                    ),
                )
            ).alias(
                "affected_asn_count"
            ),

            F.max(
                "affected_ratio"
            ).alias(
                "max_asn_affected_ratio"
            ),

            F.array_sort(
                F.collect_set(
                    F.when(
                        F.col(
                            "asn_signal_status"
                        ).isin(
                            "warning",
                            "critical",
                        ),
                        F.col(
                            "asn_v4"
                        ),
                    )
                )
            ).alias(
                "affected_asns"
            ),
        )
    )

    # ---------------------------------------------------------
    # 3. Coverage summary
    # ---------------------------------------------------------
    coverage = (
        coverage_df
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
        .select(
            "hour_start_utc",
            "country_code",
            "measurement_id",

            F.col(
                "target"
            ).alias(
                "coverage_target"
            ),

            "source_hour_present",

            F.col(
                "observed_probe_count"
            ).alias(
                "coverage_observed_probe_count"
            ),

            "baseline_probe_count_median",
            "probe_coverage_ratio",
            "probe_drop_count",
            "probe_drop_pct",
            "coverage_status",
        )
    )

    # ---------------------------------------------------------
    # 4. Build complete key set.
    #
    # full_outer is intentional:
    # coverage can exist when the measurement has zero probe rows.
    # ---------------------------------------------------------
    combined = (
        coverage
        .join(
            probe_summary,
            on=list(
                INCIDENT_KEY
            ),
            how="full_outer",
        )
        .join(
            asn_summary,
            on=list(
                INCIDENT_KEY
            ),
            how="full_outer",
        )
    )

    combined = (
        combined
        .withColumn(
            "target",
            F.coalesce(
                F.col(
                    "coverage_target"
                ),
                F.col(
                    "probe_target"
                ),
                F.col(
                    "asn_target"
                ),
            ),
        )

        .withColumn(
            "observed_probe_count",
            F.coalesce(
                F.col(
                    "observed_probe_count"
                ),
                F.col(
                    "coverage_observed_probe_count"
                ),
                F.lit(
                    0
                ),
            ).cast(
                "long"
            ),
        )

        .withColumn(
            "eligible_probe_count",
            F.coalesce(
                F.col(
                    "eligible_probe_count"
                ),
                F.lit(
                    0
                ),
            ).cast(
                "long"
            ),
        )

        .withColumn(
            "warning_probe_count",
            F.coalesce(
                F.col(
                    "warning_probe_count"
                ),
                F.lit(
                    0
                ),
            ).cast(
                "long"
            ),
        )

        .withColumn(
            "critical_probe_count",
            F.coalesce(
                F.col(
                    "critical_probe_count"
                ),
                F.lit(
                    0
                ),
            ).cast(
                "long"
            ),
        )

        .withColumn(
            "affected_probe_count",
            F.coalesce(
                F.col(
                    "affected_probe_count"
                ),
                F.lit(
                    0
                ),
            ).cast(
                "long"
            ),
        )

        .withColumn(
            "observed_asn_count",
            F.coalesce(
                F.col(
                    "observed_asn_count"
                ),
                F.lit(
                    0
                ),
            ).cast(
                "long"
            ),
        )

        .withColumn(
            "eligible_asn_count",
            F.coalesce(
                F.col(
                    "eligible_asn_count"
                ),
                F.lit(
                    0
                ),
            ).cast(
                "long"
            ),
        )

        .withColumn(
            "warning_asn_count",
            F.coalesce(
                F.col(
                    "warning_asn_count"
                ),
                F.lit(
                    0
                ),
            ).cast(
                "long"
            ),
        )

        .withColumn(
            "critical_asn_count",
            F.coalesce(
                F.col(
                    "critical_asn_count"
                ),
                F.lit(
                    0
                ),
            ).cast(
                "long"
            ),
        )

        .withColumn(
            "affected_asn_count",
            F.coalesce(
                F.col(
                    "affected_asn_count"
                ),
                F.lit(
                    0
                ),
            ).cast(
                "long"
            ),
        )
    )

    coverage_critical = (
        F.col(
            "coverage_status"
        )
        == "critical"
    )

    coverage_warning = (
        F.col(
            "coverage_status"
        )
        == "warning"
    )

    asn_critical = (
        F.col(
            "critical_asn_count"
        )
        > 0
    )

    asn_warning = (
        F.col(
            "affected_asn_count"
        )
        > 0
    )

    probe_only = (
        F.col(
            "affected_probe_count"
        )
        > 0
    )

    no_usable_network_history = (
        (
            F.col(
                "coverage_status"
            )
            == "insufficient_history"
        )
        & (
            F.col(
                "eligible_probe_count"
            )
            == 0
        )
        & (
            F.col(
                "eligible_asn_count"
            )
            == 0
        )
    )

    # ---------------------------------------------------------
    # 5. Incident classification
    #
    # Preserve existing semantics:
    #
    # critical ASN/coverage -> critical
    # warning ASN/coverage  -> warning
    # probe-only anomaly    -> observe
    # no usable history     -> insufficient_data
    # otherwise             -> normal
    # ---------------------------------------------------------
    result = (
        combined
        .withColumn(
            "incident_status",
            F.when(
                coverage_critical
                | asn_critical,
                F.lit(
                    "critical"
                ),
            )
            .when(
                coverage_warning
                | asn_warning,
                F.lit(
                    "warning"
                ),
            )
            .when(
                probe_only,
                F.lit(
                    "observe"
                ),
            )
            .when(
                no_usable_network_history,
                F.lit(
                    "insufficient_data"
                ),
            )
            .otherwise(
                F.lit(
                    "normal"
                ),
            ),
        )

        .withColumn(
            "incident_candidate",
            F.col(
                "incident_status"
            ).isin(
                "warning",
                "critical",
            ),
        )

        .withColumn(
            "severity_score",
            F.when(
                F.col(
                    "incident_status"
                )
                == "critical",
                F.lit(
                    3
                ),
            )
            .when(
                F.col(
                    "incident_status"
                )
                == "warning",
                F.lit(
                    2
                ),
            )
            .when(
                F.col(
                    "incident_status"
                )
                == "observe",
                F.lit(
                    1
                ),
            )
            .when(
                F.col(
                    "incident_status"
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
            "signal_types",
            F.concat_ws(
                ";",

                F.when(
                    F.col(
                        "affected_probe_count"
                    )
                    > 0,
                    F.lit(
                        "probe_anomaly"
                    ),
                ),

                F.when(
                    F.col(
                        "affected_asn_count"
                    )
                    > 0,
                    F.lit(
                        "asn_degradation"
                    ),
                ),

                F.when(
                    F.col(
                        "coverage_status"
                    ).isin(
                        "warning",
                        "critical",
                    ),
                    F.lit(
                        "coverage_degradation"
                    ),
                ),
            ),
        )

        .withColumn(
            "incident_summary",
            F.when(
                F.col(
                    "incident_status"
                )
                == "critical",
                F.lit(
                    "strong network degradation signal"
                ),
            )
            .when(
                F.col(
                    "incident_status"
                )
                == "warning",
                F.lit(
                    "network degradation signal"
                ),
            )
            .when(
                F.col(
                    "incident_status"
                )
                == "observe",
                F.lit(
                    "probe-level anomaly without "
                    "network-level corroboration"
                ),
            )
            .when(
                F.col(
                    "incident_status"
                )
                == "insufficient_data",
                F.lit(
                    "not enough history for "
                    "network-level classification"
                ),
            )
            .otherwise(
                F.lit(
                    "no degradation signal"
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
            "coverage_target",
            "probe_target",
            "asn_target",
            "coverage_observed_probe_count",
        )
    )

    return result


def validate_incident_candidates(
    df,
    measurement_ids: Iterable[int] | None = None,
    country_code: str | None = None,
) -> None:
    from pyspark.sql import functions as F

    ids = (
        _measurement_ids(
            measurement_ids
        )
        if measurement_ids
        is not None
        else None
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
            "Incident candidate output is empty"
        )

    invalid = (
        df
        .filter(
            F.col(
                "hour_start_utc"
            ).isNull()
            | F.col(
                "country_code"
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
            | (
                F.col(
                    "observed_probe_count"
                )
                < 0
            )
            | (
                F.col(
                    "eligible_probe_count"
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
                    "observed_asn_count"
                )
                < 0
            )
            | (
                F.col(
                    "eligible_asn_count"
                )
                < 0
            )
            | (
                F.col(
                    "affected_asn_count"
                )
                < 0
            )
            | (
                F.col(
                    "affected_asn_count"
                )
                > F.col(
                    "eligible_asn_count"
                )
            )
            | (
                ~F.col(
                    "incident_status"
                ).isin(
                    "insufficient_data",
                    "normal",
                    "observe",
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
            "Invalid incident candidate metrics"
        )

    if ids is not None:
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
                "Incident candidates contain "
                "unexpected measurement IDs"
            )

    if country is not None:
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
                != F.lit(
                    country
                )
            )
            .limit(
                1
            )
            .count()
        )

        if wrong_country:
            raise ValueError(
                "Incident candidates contain rows "
                f"outside country={country}"
            )

    inconsistent_candidate = (
        df
        .filter(
            (
                F.col(
                    "incident_candidate"
                )
                != F.col(
                    "incident_status"
                ).isin(
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

    if inconsistent_candidate:
        raise ValueError(
            "incident_candidate does not match "
            "incident_status semantics"
        )

    duplicates = (
        df
        .groupBy(
            *INCIDENT_KEY
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
            "Duplicate incident candidate keys"
        )


def write_incident_candidates(
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