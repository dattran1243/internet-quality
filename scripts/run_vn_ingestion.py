#!/usr/bin/env python
"""Ingest multiple ping measurements from connected probes in one country."""

import argparse
import logging
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from configs.settings import DEFAULT_VN_PING_MEASUREMENT_IDS, settings
from ingestion.ripe_atlas.client import RipeAtlasClient
from ingestion.ripe_atlas.ingest import ingest_filtered_measurements
from scripts.run_ingestion import selected_window
from storage.minio_client import create_s3_client


def arguments() -> argparse.Namespace:
    """Parse repeatable measurement IDs and a bounded UTC window."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--measurement-id", type=int, action="append", dest="measurement_ids",
        help="Repeat to override the default VN ping measurement list",
    )
    parser.add_argument("--country-code", default="VN")
    parser.add_argument("--hours", type=float, default=1.0)
    parser.add_argument("--start")
    parser.add_argument("--stop")
    args = parser.parse_args()
    if args.measurement_ids is None:
        args.measurement_ids = list(DEFAULT_VN_PING_MEASUREMENT_IDS)
    return args


def main() -> None:
    """Discover connected probes once and ingest every requested measurement."""
    started = time.monotonic()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = arguments()
    settings.require_storage_credentials()
    window = selected_window(args)
    client = RipeAtlasClient(
        settings.ripe_atlas_base_url, settings.request_timeout,
        settings.max_retries, settings.backoff_seconds,
    )
    s3 = create_s3_client(
        settings.minio_endpoint, settings.minio_access_key,
        settings.minio_secret_key,
    )
    summaries = ingest_filtered_measurements(
        client, s3, settings.minio_bucket, args.measurement_ids, window,
        country_code=args.country_code,
    )
    logging.getLogger(__name__).info(
        "VN ingestion complete measurements=%s duration_seconds=%.2f",
        len(summaries), time.monotonic() - started,
    )


if __name__ == "__main__":
    main()
