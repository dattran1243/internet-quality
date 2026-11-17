#!/usr/bin/env python
"""Build country-level Internet health from Gold hourly quality."""

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

from processing.gold.ripe_atlas_country_health import (
    build_country_health_hourly,
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
        "hourly_network_quality/"
    )


    target = (
        f"{root}/gold/ripe_atlas/"
        "country_health_hourly/"
    )


    try:

        log.info(
            "Country health start "
            "window=%s..%s",
            utc_text(window.start),
            utc_text(window.stop),
        )


        log.info(
            "Reading Gold source=%s",
            source,
        )


        gold = (
            spark.read
            .parquet(source)
        )


        gold = (
            gold
            .filter(
                (F.col("hour_start_utc") >= window.start)
                &
                (F.col("hour_start_utc") < window.stop)
            )
            .cache()
        )


        gold_rows = gold.count()


        if gold_rows == 0:
            raise ValueError(
                "Hourly Gold window empty"
            )


        log.info(
            "Hourly Gold rows=%s",
            gold_rows,
        )


        country_health = (
            build_country_health_hourly(
                gold
            )
            .cache()
        )


        output_rows = country_health.count()


        if output_rows == 0:
            raise ValueError(
                "Country health output empty"
            )


        log.info(
            "Country health generated rows=%s",
            output_rows,
        )


        (
            country_health
            .write
            .mode("overwrite")
            .partitionBy(
                "country_code"
            )
            .parquet(
                target
            )
        )


        log.info(
            "Country health complete "
            "output=%s duration_seconds=%.2f",
            target,
            time.monotonic() - started,
        )


        country_health.unpersist()
        gold.unpersist()


    finally:

        spark.stop()



if __name__ == "__main__":
    main()