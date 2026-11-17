#!/usr/bin/env python
"""CLI for one bounded RIPE Atlas ingestion window."""

import argparse
from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from configs.settings import settings
from ingestion.ripe_atlas.client import RipeAtlasClient
from ingestion.ripe_atlas.ingest import ingest_window
from ingestion.ripe_atlas.models import TimeWindow, parse_utc
from storage.minio_client import create_s3_client


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--measurement-id", type=int,
                        default=settings.default_measurement_id)
    parser.add_argument("--hours", type=float, default=1.0)
    parser.add_argument("--start")
    parser.add_argument("--stop")
    return parser.parse_args()


def selected_window(args: argparse.Namespace) -> TimeWindow:
    if bool(args.start) != bool(args.stop):
        raise ValueError("--start and --stop must be supplied together")
    if args.start:
        return TimeWindow(parse_utc(args.start), parse_utc(args.stop))
    if args.hours <= 0:
        raise ValueError("--hours must be positive")
    stop = datetime.now(timezone.utc).replace(microsecond=0)
    return TimeWindow(stop - timedelta(hours=args.hours), stop)


def main() -> None:
    started = time.monotonic()
    logging.basicConfig(level=settings.log_level,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = arguments()
    settings.require_storage_credentials()
    window = selected_window(args)
    client = RipeAtlasClient(settings.ripe_atlas_base_url,
                             settings.request_timeout, settings.max_retries,
                             settings.backoff_seconds)
    s3 = create_s3_client(settings.minio_endpoint, settings.minio_access_key,
                          settings.minio_secret_key)
    key, count = ingest_window(client, s3, settings.minio_bucket,
                               args.measurement_id, window)
    logging.getLogger(__name__).info(
        "Ingestion complete object=%s records=%s duration_seconds=%.2f",
        key, count, time.monotonic() - started)


if __name__ == "__main__":
    main()
