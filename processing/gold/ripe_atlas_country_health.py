"""Country level Internet health aggregation from Gold hourly quality."""

from __future__ import annotations

from pyspark.sql import functions as F


def build_country_health_hourly(gold_df):
    """
    Aggregate hourly_network_quality into country health metrics.

    Grain:

        hour_start_utc
        country_code
        target

    Input:
        hourly_network_quality

    Output:
        country_health_hourly
    """

    aggregated = (
        gold_df
        .groupBy(
            "hour_start_utc",
            "country_code",
            "target",
        )
        .agg(
            F.countDistinct(
                "probe_id"
            ).alias(
                "probe_count"
            ),

            F.countDistinct(
                "measurement_id"
            ).alias(
                "measurement_count"
            ),

            F.avg(
                "avg_rtt_ms"
            ).alias(
                "avg_rtt_ms"
            ),

            F.percentile_approx(
                "avg_rtt_ms",
                0.95,
                10000,
            ).alias(
                "p95_rtt_ms"
            ),

            F.avg(
                "avg_packet_loss_pct"
            ).alias(
                "packet_loss_pct"
            ),

            F.max(
                "source_max_ingested_at_utc"
            ).alias(
                "source_max_ingested_at_utc"
            ),
        )
    )

    # Normalize metrics
    result = (
        aggregated

        # Simple health score:
        # latency and packet loss reduce score.
        .withColumn(
            "latency_score",
            F.when(
                F.col("avg_rtt_ms") <= 50,
                F.lit(100)
            )
            .when(
                F.col("avg_rtt_ms") <= 100,
                F.lit(80)
            )
            .when(
                F.col("avg_rtt_ms") <= 200,
                F.lit(60)
            )
            .otherwise(
                F.lit(40)
            )
        )

        .withColumn(
            "loss_score",
            F.when(
                F.col("packet_loss_pct") <= 1,
                F.lit(100)
            )
            .when(
                F.col("packet_loss_pct") <= 5,
                F.lit(70)
            )
            .otherwise(
                F.lit(40)
            )
        )

        .withColumn(
            "health_score",
            (
                F.col("latency_score")
                * F.lit(0.7)
                +
                F.col("loss_score")
                * F.lit(0.3)
            )
        )

        .withColumn(
            "status",
            F.when(
                F.col("health_score") >= 85,
                "GOOD"
            )
            .when(
                F.col("health_score") >= 70,
                "WARNING"
            )
            .otherwise(
                "BAD"
            )
        )
    )

    return result