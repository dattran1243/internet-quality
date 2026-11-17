#!/usr/bin/env python
"""Incrementally transform one RIPE Atlas Bronze window into Silver Ping."""

import argparse
import logging
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from configs.settings import settings
from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
    utc_text,
)
from processing.silver.incremental import (
    bronze_window_glob,
    filter_exact_bronze_window,
    replace_measurements_in_window,
    validate_hour_aligned_window,
)
from processing.silver.ripe_atlas_ping import transform_bronze


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

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

    return parser.parse_args()


def selected_window(args: argparse.Namespace) -> TimeWindow:
    window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )

    validate_hour_aligned_window(window)

    return window


def spark_session():
    from pyspark.sql import SparkSession

    settings.require_storage_credentials()

    return (
        SparkSession.builder
        .appName("ripe-atlas-ping-silver")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .master(settings.spark_master)

        # Add Hadoop AWS connector for s3a:// MinIO access
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )

        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )

        .config(
            "spark.sql.sources.partitionColumnTypeInference.enabled",
            "false",
        )

        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic",
        )

        # MinIO / S3A configuration
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            settings.minio_endpoint,
        )

        .config(
            "spark.hadoop.fs.s3a.access.key",
            settings.minio_access_key,
        )

        .config(
            "spark.hadoop.fs.s3a.secret.key",
            settings.minio_secret_key,
        )

        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )

        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )

        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

        .config(
           "spark.hadoop.fs.s3a.aws.credentials.provider",
           "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

        # Avoid Hadoop NativeIO disk-buffer issue on Windows.
        .config(
          "spark.hadoop.fs.s3a.fast.upload",
          "true",
        )

        .config(
         "spark.hadoop.fs.s3a.fast.upload.buffer",
         "bytebuffer",
        )

        .getOrCreate()
    )


def main() -> None:
    started = time.monotonic()

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    log = logging.getLogger(__name__)

    args = arguments()

    window = selected_window(args)

    spark = spark_session()

    source = bronze_window_glob(
        settings.minio_bucket,
        window,
    )

    target = (
        f"s3a://{settings.minio_bucket}/"
        "silver/ripe_atlas/ping/"
    )

    try:
        log.info(
            "Incremental Silver window=%s..%s",
            utc_text(window.start),
            utc_text(window.stop),
        )

        log.info(
            "Spark Bronze input glob=%s",
            source,
        )

        bronze_candidates = (
            spark.read
            .option("multiLine", "true")
            .json(source)
        )

        bronze = (
            filter_exact_bronze_window(
                bronze_candidates,
                window,
            )
            .cache()
        )

        envelope_count = bronze.count()

        if envelope_count == 0:
            raise ValueError(
                "No Bronze envelopes found for the requested exact window"
            )

        measurement_ids = [
            int(row.measurement_id)
            for row in (
                bronze
                .selectExpr(
                    "cast(metadata.measurement_id as long) "
                    "as measurement_id"
                )
                .distinct()
                .collect()
            )
            if row.measurement_id is not None
        ]

        if not measurement_ids:
            raise ValueError(
                "Bronze window contains no measurement IDs"
            )

        before = (
            bronze
            .selectExpr("explode_outer(records)")
            .count()
        )

        silver = transform_bronze(
            bronze
        ).cache()

        valid = (
            silver
            .filter("validation_status = 'valid'")
            .count()
        )

        invalid = (
            silver
            .filter("validation_status = 'invalid'")
            .count()
        )

        if invalid:
            log.warning(
                "Invalid records retained in Silver: %s",
                invalid,
            )

        log.info(
            "Bronze envelopes=%s measurements=%s "
            "records=%s valid=%s invalid=%s",
            envelope_count,
            sorted(measurement_ids),
            before,
            valid,
            invalid,
        )

        log.info(
            "Spark output path=%s",
            target,
        )

        replace_measurements_in_window(
            silver,
            target,
            window,
            measurement_ids,
        )

        log.info(
            "Incremental Silver complete "
            "window=%s..%s duration_seconds=%.2f",
            utc_text(window.start),
            utc_text(window.stop),
            time.monotonic() - started,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()