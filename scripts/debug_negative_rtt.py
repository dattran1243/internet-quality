#!/usr/bin/env python
"""Inspect negative Silver RTT rows and their matching Bronze records."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from configs.settings import settings
from scripts.run_silver import spark_session


def main() -> None:
    """Print negative RTT patterns and matched raw packet measurement fields."""
    spark = spark_session()
    silver_path = f"s3a://{settings.minio_bucket}/silver/ripe_atlas/ping/"
    bronze_path = f"s3a://{settings.minio_bucket}/bronze/ripe_atlas/ping/"
    negative_condition = (
        "rtt_min_ms < 0 OR rtt_avg_ms < 0 OR rtt_max_ms < 0"
    )
    try:
        from pyspark.sql import functions as f

        silver = spark.read.parquet(silver_path)
        negative = silver.filter(negative_condition).cache()
        print(f"total_negative_rtt_rows={negative.count()}")
        negative.groupBy(
            "packets_sent", "packets_received", "rtt_min_ms",
            "rtt_avg_ms", "rtt_max_ms",
        ).count().orderBy(f.desc("count")).show(100, truncate=False)
        negative.select(
            "event_time_utc", "measurement_id", "probe_id", "target",
            "destination_address", "rtt_min_ms", "rtt_avg_ms",
            "rtt_max_ms", "packets_sent", "packets_received",
            "packet_loss_pct", "validation_status", "year", "month",
            "day", "hour",
        ).show(20, truncate=False)

        if negative.isEmpty():
            return

        bronze = spark.read.option("multiLine", "true").json(bronze_path)
        raw = (
            bronze.select(f.explode_outer("records").alias("r"))
            .select(
                f.col("r.msm_id").cast("long").alias("raw_measurement_id"),
                f.col("r.prb_id").cast("long").alias("raw_probe_id"),
                f.to_timestamp(f.from_unixtime(f.col("r.timestamp"))).alias(
                    "raw_event_time_utc"
                ),
                f.col("r.sent").alias("raw_sent"),
                f.col("r.rcvd").alias("raw_rcvd"),
                f.col("r.min").alias("raw_min"),
                f.col("r.avg").alias("raw_avg"),
                f.col("r.max").alias("raw_max"),
                f.col("r.result").alias("raw_result"),
            )
        )
        sample_keys = negative.select(
            "measurement_id", "probe_id", "event_time_utc"
        ).limit(20)
        (
            sample_keys.join(
                raw,
                (sample_keys.measurement_id == raw.raw_measurement_id)
                & (sample_keys.probe_id == raw.raw_probe_id)
                & (sample_keys.event_time_utc == raw.raw_event_time_utc),
                "left",
            )
            .select(
                "event_time_utc", "measurement_id", "probe_id",
                "raw_sent", "raw_rcvd", "raw_min", "raw_avg",
                "raw_max", "raw_result",
            )
            .show(20, truncate=False)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
