#!/usr/bin/env python
"""Detect missing RIPE Atlas Silver five-minute completion manifests."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)

from configs.settings import settings
from ingestion.ripe_atlas.models import (
    parse_utc,
    utc_text,
)
from processing.silver.window_manifest import (
    silver_window_manifest_exists,
)
from scripts.run_silver_backfill import build_windows
from storage.minio_client import create_s3_client


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Detect missing Silver five-minute completion manifests "
            "over a deterministic UTC range."
        )
    )

    parser.add_argument(
        "--start",
        required=True,
        help="UTC inclusive start on a five-minute boundary",
    )

    parser.add_argument(
        "--stop",
        required=True,
        help="UTC exclusive stop on a five-minute boundary",
    )

    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=settings.log_level,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    log = logging.getLogger(__name__)

    args = arguments()

    start = parse_utc(
        args.start
    )

    stop = parse_utc(
        args.stop
    )

    windows = build_windows(
        start,
        stop,
    )

    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    missing = []

    for index, window in enumerate(
        windows,
        start=1,
    ):
        exists = silver_window_manifest_exists(
            s3,
            settings.minio_bucket,
            window,
        )

        log.info(
            "Silver manifest check "
            "position=%s/%s window=%s..%s status=%s",
            index,
            len(windows),
            utc_text(window.start),
            utc_text(window.stop),
            "COMPLETE" if exists else "MISSING",
        )

        if not exists:
            missing.append(
                window
            )

    if not missing:
        log.info(
            "Silver gap detection complete "
            "range=%s..%s windows=%s missing=0",
            utc_text(start),
            utc_text(stop),
            len(windows),
        )
        return

    log.warning(
        "Silver gap detection complete "
        "range=%s..%s windows=%s missing=%s",
        utc_text(start),
        utc_text(stop),
        len(windows),
        len(missing),
    )

    for window in missing:
        log.warning(
            "Silver gap detected "
            "window=%s..%s",
            utc_text(window.start),
            utc_text(window.stop),
        )


if __name__ == "__main__":
    main()
