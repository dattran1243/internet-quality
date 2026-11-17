#!/usr/bin/env python
"""Build global Internet health from country health Gold layer."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
import time


sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)


from pyspark.sql import functions as F


from configs.settings import settings
from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
    utc_text,
)

from processing.gold.ripe_atlas_global_health import (
    build_global_health_hourly,
)

from processing.silver.incremental import (
    validate_hour_aligned_window,
)

from scripts.run_silver import spark_session



def arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start",
        required=True,
        help="UTC ISO-8601 start",
    )

    parser.add_argument(
        "--stop",
        required=True,
        help="UTC ISO-8601 stop",
    )

    return parser.parse_args()



def main() -> None:

    started = time.monotonic()


    logging.basicConfig(
        level=settings.log_level,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )


    log = logging.getLogger(__name__)


    args = arguments()


    window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )


    validate_hour_aligned_window(
        window
    )


    spark = spark_session()


    root = (
        f"s3a://{settings.minio_bucket}"
    )


    source = (
        f"{root}/gold/ripe_atlas/"
        "country_health_hourly/"
    )


    target = (
        f"{root}/gold/ripe_atlas/"
        "global_health_hourly/"
    )


    try:

        log.info(
            "Global health start "
            "window=%s..%s",
            utc_text(window.start),
            utc_text(window.stop),
        )


        log.info(
            "Reading country health source=%s",
            source,
        )


        country_health = (
            spark.read
            .parquet(source)
        )


        country_health = (
            country_health
            .filter(
                (F.col("hour_start_utc") >= window.start)
                &
                (F.col("hour_start_utc") < window.stop)
            )
            .cache()
        )


        input_rows = (
            country_health.count()
        )


        if input_rows == 0:
            raise ValueError(
                "Country health window empty"
            )


        log.info(
            "Country health rows=%s",
            input_rows,
        )


        global_health = (
            build_global_health_hourly(
                country_health
            )
            .cache()
        )


        output_rows = (
            global_health.count()
        )


        if output_rows == 0:
            raise ValueError(
                "Global health output empty"
            )


        log.info(
            "Global health generated rows=%s",
            output_rows,
        )


        (
            global_health
            .write
            .mode("overwrite")
            .parquet(
                target
            )
        )


        log.info(
            "Global health complete "
            "output=%s duration_seconds=%.2f",
            target,
            time.monotonic() - started,
        )


        global_health.unpersist()
        country_health.unpersist()


    finally:

        spark.stop()



if __name__ == "__main__":
    main()