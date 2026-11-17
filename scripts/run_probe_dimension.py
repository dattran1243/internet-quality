#!/usr/bin/env python
"""Build the current RIPE Atlas active-probe Silver dimension."""

import argparse
import logging
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from configs.settings import settings
from ingestion.ripe_atlas.client import RipeAtlasClient
from processing.silver.ripe_atlas_probe import (
    build_probe_dimension,
    probe_dimension_summary,
    validate_probe_dimension,
)
from scripts.run_silver import spark_session


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--country-code", default="VN")
    parser.add_argument("--status-name", default="Connected")
    parser.add_argument("--output-path")
    return parser.parse_args()


def main() -> None:
    """Fetch, normalize, validate, and replace the current snapshot."""
    started = time.monotonic()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger(__name__)
    args = arguments()
    settings.require_storage_credentials()
    output_path = args.output_path or (
        f"s3a://{settings.minio_bucket}/silver/ripe_atlas/probes/"
    )
    client = RipeAtlasClient(
        settings.ripe_atlas_base_url, settings.request_timeout,
        settings.max_retries, settings.backoff_seconds,
    )
    probes = client.get_connected_probes(
        args.country_code, args.status_name
    )
    spark = spark_session()
    try:
        dimension = build_probe_dimension(spark, probes).cache()
        validate_probe_dimension(
            dimension, args.country_code, args.status_name
        )
        summary = probe_dimension_summary(dimension)
        distributions = [
            row.asDict()
            for row in dimension.groupBy(
                "country_code", "status_name"
            ).count().collect()
        ]
        log.info(
            "Probe dimension summary country=%s status=%s probes=%s "
            "distinct_probe_ids=%s distinct_asn_v4=%s missing_asn_v4=%s "
            "missing_asn_v6=%s missing_coordinates=%s anchors=%s "
            "distribution=%s",
            args.country_code.upper(), args.status_name, summary["probes"],
            summary["distinct_probe_ids"], summary["distinct_asn_v4"],
            summary["missing_asn_v4"], summary["missing_asn_v6"],
            summary["missing_coordinates"], summary["anchors"],
            distributions,
        )
        log.info("Writing current probe snapshot path=%s mode=overwrite",
                 output_path)
        dimension.coalesce(1).write.mode("overwrite").parquet(output_path)
        written = spark.read.parquet(output_path)
        validate_probe_dimension(
            written, args.country_code, args.status_name
        )
        log.info(
            "Probe dimension complete rows=%s path=%s duration_seconds=%.2f",
            written.count(), output_path, time.monotonic() - started,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
