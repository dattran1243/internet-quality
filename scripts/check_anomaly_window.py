#!/usr/bin/env python
"""Inspect one country-scoped anomaly window without modifying data."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)

from pyspark.sql import functions as F

from configs.monitoring import (
    get_enabled_measurement_ids,
)
from configs.settings import settings
from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
)
from processing.silver.incremental import (
    existing_window_rows,
)
from scripts.run_silver import spark_session


COUNTRY_CODE = "VN"

START = "2026-09-21T00:00:00Z"
STOP = "2026-09-21T01:00:00Z"

MIN_HISTORY_HOURS = 24


def main() -> None:
    spark = spark_session()

    spark.conf.set(
        "spark.sql.sources."
        "partitionColumnTypeInference.enabled",
        "false",
    )

    window = TimeWindow(
        parse_utc(START),
        parse_utc(STOP),
    )

    measurement_ids = tuple(
        sorted(
            int(value)
            for value
            in get_enabled_measurement_ids()
        )
    )

    root = (
        f"s3a://{settings.minio_bucket}"
    )

    target = (
        f"{root}/gold/ripe_atlas/"
        "hourly_anomalies/"
        f"country={COUNTRY_CODE}/"
    )

    try:
        df = existing_window_rows(
            spark,
            target,
            window,
        )

        if df is None:
            raise ValueError(
                "No anomaly data found for requested window"
            )

        df = (
            df
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

        total_rows = df.count()

        if total_rows == 0:
            raise ValueError(
                "Anomaly partition exists but contains no rows"
            )

        print()
        print("=" * 80)
        print("ANOMALY WINDOW CHECK")
        print("=" * 80)

        print(
            f"country={COUNTRY_CODE}"
        )
        print(
            f"window={START} -> {STOP}"
        )
        print(
            f"total_rows={total_rows}"
        )
        print(
            f"configured_measurements={list(measurement_ids)}"
        )
        print(
            f"path={target}"
        )

        actual_measurements = [
            int(row["measurement_id"])
            for row in (
                df
                .select("measurement_id")
                .distinct()
                .orderBy("measurement_id")
                .collect()
            )
        ]

        print(
            f"actual_measurements={actual_measurements}"
        )

        print()
        print("=" * 80)
        print("STATUS DISTRIBUTION")
        print("=" * 80)

        (
            df
            .groupBy(
                "country_code",
                "status",
            )
            .count()
            .orderBy(
                "country_code",
                "status",
            )
            .show(
                100,
                truncate=False,
            )
        )

        print()
        print("=" * 80)
        print("BASELINE HISTORY COUNT DISTRIBUTION")
        print("=" * 80)

        (
            df
            .groupBy(
                "baseline_history_count"
            )
            .count()
            .orderBy(
                "baseline_history_count"
            )
            .show(
                100,
                truncate=False,
            )
        )

        print()
        print("=" * 80)
        print("LOSS BASELINE HISTORY COUNT DISTRIBUTION")
        print("=" * 80)

        (
            df
            .groupBy(
                "baseline_loss_history_count"
            )
            .count()
            .orderBy(
                "baseline_loss_history_count"
            )
            .show(
                100,
                truncate=False,
            )
        )

        stats = (
            df
            .agg(
                F.min(
                    "baseline_history_count"
                ).alias(
                    "min_rtt_history"
                ),
                F.max(
                    "baseline_history_count"
                ).alias(
                    "max_rtt_history"
                ),
                F.min(
                    "baseline_loss_history_count"
                ).alias(
                    "min_loss_history"
                ),
                F.max(
                    "baseline_loss_history_count"
                ).alias(
                    "max_loss_history"
                ),
            )
            .first()
        )

        sufficient_rows = (
            df
            .filter(
                F.col(
                    "baseline_history_count"
                )
                >= MIN_HISTORY_HOURS
            )
            .count()
        )

        insufficient_rows = (
            df
            .filter(
                F.col(
                    "baseline_history_count"
                )
                < MIN_HISTORY_HOURS
            )
            .count()
        )

        insufficient_status_rows = (
            df
            .filter(
                F.col("status")
                == "insufficient_history"
            )
            .count()
        )

        active_scored_rows = (
            df
            .filter(
                (
                    F.col(
                        "baseline_history_count"
                    )
                    >= MIN_HISTORY_HOURS
                )
                & (
                    F.col("status")
                    != "insufficient_history"
                )
            )
            .count()
        )

        print()
        print("=" * 80)
        print("BASELINE SUMMARY")
        print("=" * 80)

        print(
            "min_rtt_history="
            f"{stats['min_rtt_history']}"
        )
        print(
            "max_rtt_history="
            f"{stats['max_rtt_history']}"
        )
        print(
            "min_loss_history="
            f"{stats['min_loss_history']}"
        )
        print(
            "max_loss_history="
            f"{stats['max_loss_history']}"
        )

        print(
            f"sufficient_rows={sufficient_rows}"
        )
        print(
            f"insufficient_rows={insufficient_rows}"
        )
        print(
            "insufficient_status_rows="
            f"{insufficient_status_rows}"
        )
        print(
            f"active_scored_rows={active_scored_rows}"
        )

        print()
        print("=" * 80)
        print("ROWS WITH FULL 24-HOUR RTT BASELINE")
        print("=" * 80)

        (
            df
            .filter(
                F.col(
                    "baseline_history_count"
                )
                >= MIN_HISTORY_HOURS
            )
            .select(
                "hour_start_utc",
                "country_code",
                "measurement_id",
                "probe_id",
                "target",
                "baseline_history_count",
                "baseline_loss_history_count",
                "avg_rtt_ms",
                "baseline_rtt_median_ms",
                "latency_ratio",
                "latency_z_score",
                "avg_packet_loss_pct",
                "baseline_loss_mean_pct",
                "loss_delta_pct_points",
                "latency_anomaly",
                "packet_loss_anomaly",
                "status",
                "severity_score",
                "anomaly_reasons",
            )
            .orderBy(
                F.col(
                    "severity_score"
                ).desc_nulls_last(),
                F.col(
                    "latency_ratio"
                ).desc_nulls_last(),
            )
            .show(
                100,
                truncate=False,
            )
        )

        print()
        print("=" * 80)
        print("WARNING / CRITICAL")
        print("=" * 80)

        (
            df
            .filter(
                F.col("status").isin(
                    "warning",
                    "critical",
                )
            )
            .select(
                "hour_start_utc",
                "measurement_id",
                "probe_id",
                "target",
                "baseline_history_count",
                "avg_rtt_ms",
                "baseline_rtt_median_ms",
                "latency_ratio",
                "latency_z_score",
                "avg_packet_loss_pct",
                "baseline_loss_mean_pct",
                "loss_delta_pct_points",
                "status",
                "anomaly_reasons",
            )
            .orderBy(
                F.col(
                    "severity_score"
                ).desc_nulls_last(),
                F.col(
                    "latency_ratio"
                ).desc_nulls_last(),
            )
            .show(
                100,
                truncate=False,
            )
        )

        wrong_country = (
            df
            .filter(
                F.upper(
                    F.trim(
                        F.col("country_code")
                    )
                )
                != COUNTRY_CODE
            )
            .count()
        )

        unexpected_measurements = (
            df
            .filter(
                ~F.col(
                    "measurement_id"
                ).isin(
                    list(
                        measurement_ids
                    )
                )
            )
            .count()
        )

        print()
        print("=" * 80)
        print("ACCEPTANCE")
        print("=" * 80)

        print(
            f"wrong_country_rows={wrong_country}"
        )
        print(
            "unexpected_measurement_rows="
            f"{unexpected_measurements}"
        )

        if wrong_country != 0:
            raise ValueError(
                "FAIL: rows outside country VN"
            )

        if unexpected_measurements != 0:
            raise ValueError(
                "FAIL: unexpected measurement IDs"
            )

        if active_scored_rows == 0:
            raise ValueError(
                "FAIL: no rows have enough RTT history "
                "to perform anomaly scoring"
            )

        print(
            "PASS: anomaly detector has rows with "
            "sufficient historical baseline."
        )

        if insufficient_status_rows > 0:
            print(
                "NOTE: some rows are still "
                "insufficient_history; this can be valid "
                "when a probe lacks enough usable RTT hours."
            )

        print("=" * 80)

        df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()