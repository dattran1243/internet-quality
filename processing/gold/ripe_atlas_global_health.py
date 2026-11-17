"""Global Internet health aggregation from country health Gold layer."""

from __future__ import annotations

from pyspark.sql import functions as F


def build_global_health_hourly(country_health_df):
    """
    Aggregate country_health_hourly into global health metrics.

    Input grain:

        hour_start_utc
        country_code
        target

    Output grain:

        hour_start_utc

    Output:
        global_health_hourly
    """

    global_health = (
        country_health_df
        .groupBy(
            "hour_start_utc",
        )
        .agg(
            F.countDistinct(
                "country_code"
            ).alias(
                "countries_monitored"
            ),

            F.countDistinct(
                "target"
            ).alias(
                "targets_monitored"
            ),

            F.avg(
                "health_score"
            ).alias(
                "global_score"
            ),

            F.sum(
                "probe_count"
            ).alias(
                "active_probes"
            ),

            F.sum(
                F.when(
                    F.col("status") == "GOOD",
                    1,
                )
                .otherwise(0)
            ).alias(
                "good_countries"
            ),

            F.sum(
                F.when(
                    F.col("status") == "WARNING",
                    1,
                )
                .otherwise(0)
            ).alias(
                "warning_countries"
            ),

            F.sum(
                F.when(
                    F.col("status") == "BAD",
                    1,
                )
                .otherwise(0)
            ).alias(
                "bad_countries"
            ),
        )
    )


    result = (
        global_health

        .withColumn(
            "global_status",
            F.when(
                F.col("global_score") >= 85,
                "GOOD",
            )
            .when(
                F.col("global_score") >= 70,
                "WARNING",
            )
            .otherwise(
                "BAD",
            )
        )

        .withColumn(
            "generated_by",
            F.lit(
                "global_health_v1"
            )
        )
    )


    return result