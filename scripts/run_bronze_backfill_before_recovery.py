#!/usr/bin/env python
"""Bounded historical RIPE Atlas -> Kafka -> Bronze backfill."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import timedelta
import json
import logging
from pathlib import Path
import re
import subprocess
import sys
import time


sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)


from confluent_kafka import (
    Consumer,
    TopicPartition,
)

from configs.monitoring import (
    get_enabled_countries,
    get_enabled_measurement_ids,
)

from configs.settings import settings

from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
    utc_text,
)

from messaging.kafka_config import (
    BRONZE_RIPE_ATLAS_CONSUMER_GROUP,
    KAFKA_BOOTSTRAP_SERVERS,
    RIPE_ATLAS_PING_TOPIC,
)

from storage.minio_client import (
    create_s3_client,
)


FIVE_MINUTES = timedelta(
    minutes=5
)

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

PUBLISH_SCRIPT = (
    PROJECT_ROOT
    / "scripts"
    / "publish_ripe_global_kafka.py"
)

CONSUMER_SCRIPT = (
    PROJECT_ROOT
    / "scripts"
    / "consume_ripe_kafka_to_bronze.py"
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill RIPE Atlas historical Bronze "
            "through Kafka using deterministic "
            "five-minute windows."
        )
    )

    parser.add_argument(
        "--start",
        required=True,
        help=(
            "UTC inclusive start aligned "
            "to five minutes."
        ),
    )

    parser.add_argument(
        "--stop",
        required=True,
        help=(
            "UTC exclusive stop aligned "
            "to five minutes."
        ),
    )

    parser.add_argument(
        "--max-windows",
        type=int,
        default=None,
        help=(
            "Process at most this many "
            "missing windows."
        ),
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=3,
        help=(
            "Maximum countries published "
            "concurrently. Default: 3."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Inspect windows without "
            "publishing or consuming."
        ),
    )

    return parser.parse_args()


def build_windows(
    start,
    stop,
) -> list[TimeWindow]:

    if stop <= start:
        raise ValueError(
            "stop must be greater than start"
        )

    for name, value in (
        ("start", start),
        ("stop", stop),
    ):
        if (
            value.second != 0
            or value.microsecond != 0
            or value.minute % 5 != 0
        ):
            raise ValueError(
                f"{name} must align to "
                "a five-minute boundary"
            )

    windows: list[
        TimeWindow
    ] = []

    current = start

    while current < stop:
        windows.append(
            TimeWindow(
                current,
                current + FIVE_MINUTES,
            )
        )

        current += FIVE_MINUTES

    return windows


def first_not_none(
    *values,
):
    for value in values:
        if value is not None:
            return value

    return None


def verify_bronze_window(
    s3,
    bucket: str,
    window: TimeWindow,
    countries: list[str],
    measurements: set[int],
) -> tuple[bool, dict]:

    expected_start = utc_text(
        window.start
    )

    expected_stop = utc_text(
        window.stop
    )

    errors: list[str] = []

    complete_combinations = 0
    total_objects = 0

    for country in countries:

        prefix = (
            "bronze/"
            "source=ripe_atlas/"
            "dataset=ping/"
            f"country={country}/"
            f"year={window.start:%Y}/"
            f"month={window.start:%m}/"
            f"day={window.start:%d}/"
            f"hour={window.start:%H}/"
            f"minute={window.start:%M}/"
        )

        objects: list[str] = []

        paginator = (
            s3.get_paginator(
                "list_objects_v2"
            )
        )

        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
        ):
            objects.extend(
                obj["Key"]
                for obj
                in page.get(
                    "Contents",
                    [],
                )
                if obj["Key"].endswith(
                    ".json"
                )
            )

        total_objects += len(
            objects
        )

        by_measurement = defaultdict(
            list
        )

        for key in objects:

            match = re.search(
                r"/measurement_id=(\d+)/",
                key,
            )

            if not match:
                errors.append(
                    f"{country}: "
                    "cannot parse measurement "
                    f"from {key}"
                )

                continue

            measurement_id = int(
                match.group(1)
            )

            if (
                measurement_id
                not in measurements
            ):
                errors.append(
                    f"{country}: unexpected "
                    f"measurement="
                    f"{measurement_id}"
                )

                continue

            response = s3.get_object(
                Bucket=bucket,
                Key=key,
            )

            payload = json.loads(
                response[
                    "Body"
                ].read()
            )

            metadata = payload.get(
                "metadata",
                {},
            )

            payload_section = (
                payload.get(
                    "payload",
                    {},
                )
            )

            meta_country = (
                first_not_none(
                    metadata.get(
                        "country_code"
                    ),
                    payload.get(
                        "country_code"
                    ),
                )
            )

            meta_measurement = (
                first_not_none(
                    metadata.get(
                        "measurement_id"
                    ),
                    payload.get(
                        "measurement_id"
                    ),
                )
            )

            meta_start = (
                first_not_none(
                    metadata.get(
                        "window_start_utc"
                    ),
                    metadata.get(
                        "requested_start_utc"
                    ),
                    payload.get(
                        "window_start_utc"
                    ),
                )
            )

            meta_stop = (
                first_not_none(
                    metadata.get(
                        "window_stop_utc"
                    ),
                    metadata.get(
                        "requested_stop_utc"
                    ),
                    payload.get(
                        "window_stop_utc"
                    ),
                )
            )

            chunk_index = (
                first_not_none(
                    metadata.get(
                        "chunk_index"
                    ),
                    payload.get(
                        "chunk_index"
                    ),
                )
            )

            chunk_count = (
                first_not_none(
                    metadata.get(
                        "chunk_count"
                    ),
                    payload.get(
                        "chunk_count"
                    ),
                )
            )

            records = (
                first_not_none(
                    payload.get(
                        "records"
                    ),
                    payload_section.get(
                        "records"
                    ),
                )
            )

            if meta_country != country:
                errors.append(
                    f"{country}/"
                    f"{measurement_id}: "
                    f"country="
                    f"{meta_country}"
                )

            try:
                parsed_measurement = int(
                    meta_measurement
                )

            except (
                TypeError,
                ValueError,
            ):
                parsed_measurement = None

            if (
                parsed_measurement
                != measurement_id
            ):
                errors.append(
                    f"{country}/"
                    f"{measurement_id}: "
                    f"measurement="
                    f"{meta_measurement}"
                )

            if (
                meta_start
                != expected_start
            ):
                errors.append(
                    f"{country}/"
                    f"{measurement_id}: "
                    f"start={meta_start}"
                )

            if (
                meta_stop
                != expected_stop
            ):
                errors.append(
                    f"{country}/"
                    f"{measurement_id}: "
                    f"stop={meta_stop}"
                )

            if (
                chunk_index is None
                or chunk_count is None
            ):
                errors.append(
                    f"{country}/"
                    f"{measurement_id}: "
                    "missing chunk metadata"
                )

                continue

            if records is None:
                errors.append(
                    f"{country}/"
                    f"{measurement_id}: "
                    "records missing"
                )

            by_measurement[
                measurement_id
            ].append(
                (
                    int(
                        chunk_index
                    ),
                    int(
                        chunk_count
                    ),
                )
            )

        for measurement_id in sorted(
            measurements
        ):

            chunks = (
                by_measurement.get(
                    measurement_id,
                    [],
                )
            )

            if not chunks:
                errors.append(
                    f"{country}/"
                    f"{measurement_id}: "
                    "MISSING"
                )

                continue

            declared_counts = {
                chunk_count
                for _, chunk_count
                in chunks
            }

            if (
                len(
                    declared_counts
                )
                != 1
            ):
                errors.append(
                    f"{country}/"
                    f"{measurement_id}: "
                    "inconsistent "
                    "chunk_count"
                )

                continue

            expected_chunk_count = (
                next(
                    iter(
                        declared_counts
                    )
                )
            )

            actual_indexes = sorted(
                chunk_index
                for chunk_index, _
                in chunks
            )

            expected_indexes = list(
                range(
                    expected_chunk_count
                )
            )

            if (
                actual_indexes
                != expected_indexes
            ):
                errors.append(
                    f"{country}/"
                    f"{measurement_id}: "
                    f"chunks="
                    f"{actual_indexes} "
                    f"expected="
                    f"{expected_indexes}"
                )

                continue

            if (
                len(chunks)
                != expected_chunk_count
            ):
                errors.append(
                    f"{country}/"
                    f"{measurement_id}: "
                    f"objects="
                    f"{len(chunks)} "
                    f"chunk_count="
                    f"{expected_chunk_count}"
                )

                continue

            complete_combinations += 1

    expected_combinations = (
        len(countries)
        * len(measurements)
    )

    complete = (
        complete_combinations
        == expected_combinations
        and not errors
    )

    return (
        complete,
        {
            "expected_combinations":
                expected_combinations,

            "complete_combinations":
                complete_combinations,

            "total_objects":
                total_objects,

            "errors":
                errors,
        },
    )


def kafka_lag(
) -> tuple[
    int,
    dict[int, int],
]:

    consumer = Consumer(
        {
            "bootstrap.servers":
                KAFKA_BOOTSTRAP_SERVERS,

            "group.id":
                BRONZE_RIPE_ATLAS_CONSUMER_GROUP,

            "enable.auto.commit":
                False,
        }
    )

    try:
        metadata = (
            consumer.list_topics(
                RIPE_ATLAS_PING_TOPIC,
                timeout=15,
            )
        )

        topic_metadata = (
            metadata
            .topics[
                RIPE_ATLAS_PING_TOPIC
            ]
        )

        if topic_metadata.error:
            raise RuntimeError(
                "Kafka topic metadata "
                f"error: "
                f"{topic_metadata.error}"
            )

        partitions = sorted(
            topic_metadata
            .partitions
            .keys()
        )

        topic_partitions = [
            TopicPartition(
                RIPE_ATLAS_PING_TOPIC,
                partition,
            )
            for partition
            in partitions
        ]

        committed = (
            consumer.committed(
                topic_partitions,
                timeout=15,
            )
        )

        committed_by_partition = {
            item.partition:
                item.offset
            for item
            in committed
        }

        lags: dict[
            int,
            int,
        ] = {}

        for partition in partitions:

            tp = TopicPartition(
                RIPE_ATLAS_PING_TOPIC,
                partition,
            )

            low, high = (
                consumer
                .get_watermark_offsets(
                    tp,
                    timeout=15,
                    cached=False,
                )
            )

            current = (
                committed_by_partition[
                    partition
                ]
            )

            if current < 0:
                current = low

            lags[
                partition
            ] = max(
                0,
                high - current,
            )

        return (
            sum(
                lags.values()
            ),
            lags,
        )

    finally:
        consumer.close()


def run_command(
    command: list[str],
) -> None:

    print(
        "Executing:",
        " ".join(command),
        flush=True,
    )

    subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=True,
    )


def main() -> None:

    started = time.monotonic()

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    log = logging.getLogger(
        __name__
    )

    args = arguments()

    if args.max_workers <= 0:
        raise ValueError(
            "max_workers must be positive"
        )

    if (
        args.max_windows is not None
        and args.max_windows <= 0
    ):
        raise ValueError(
            "max_windows must be positive"
        )

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

    countries = sorted(
        country.code
        for country
        in get_enabled_countries()
    )

    measurements = set(
        get_enabled_measurement_ids()
    )

    if not countries:
        raise ValueError(
            "No enabled countries"
        )

    if not measurements:
        raise ValueError(
            "No enabled measurements"
        )

    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    log.info(
        "Bronze backfill start "
        "range=%s..%s "
        "windows=%s "
        "countries=%s "
        "measurements=%s "
        "dry_run=%s "
        "max_windows=%s "
        "max_workers=%s",
        utc_text(start),
        utc_text(stop),
        len(windows),
        len(countries),
        len(measurements),
        args.dry_run,
        args.max_windows,
        args.max_workers,
    )

    processed = 0
    skipped = 0

    for position, window in enumerate(
        windows,
        start=1,
    ):

        complete, summary = (
            verify_bronze_window(
                s3,
                settings.minio_bucket,
                window,
                countries,
                measurements,
            )
        )

        if complete:
            skipped += 1

            log.info(
                "Bronze backfill window "
                "position=%s/%s "
                "window=%s..%s "
                "status=COMPLETE "
                "action=SKIP "
                "objects=%s",
                position,
                len(windows),
                utc_text(
                    window.start
                ),
                utc_text(
                    window.stop
                ),
                summary[
                    "total_objects"
                ],
            )

            continue

        log.info(
            "Bronze backfill window "
            "position=%s/%s "
            "window=%s..%s "
            "status=MISSING "
            "complete_combinations="
            "%s/%s errors=%s",
            position,
            len(windows),
            utc_text(
                window.start
            ),
            utc_text(
                window.stop
            ),
            summary[
                "complete_combinations"
            ],
            summary[
                "expected_combinations"
            ],
            len(
                summary[
                    "errors"
                ]
            ),
        )

        if args.dry_run:
            continue

        if (
            args.max_windows
            is not None
            and processed
            >= args.max_windows
        ):
            log.info(
                "Bronze backfill "
                "max_windows reached "
                "processed=%s",
                processed,
            )

            break

        (
            before_total,
            before_parts,
        ) = kafka_lag()

        if before_total != 0:
            raise RuntimeError(
                "Kafka must have zero lag "
                "before historical publish; "
                f"lag={before_total} "
                f"partitions="
                f"{before_parts}"
            )

        publish_command = [
            sys.executable,
            str(
                PUBLISH_SCRIPT
            ),
            "--start",
            utc_text(
                window.start
            ),
            "--stop",
            utc_text(
                window.stop
            ),
            "--max-workers",
            str(
                args.max_workers
            ),
        ]

        try:
            run_command(
                publish_command
            )

        except subprocess.CalledProcessError:

            (
                lag_total,
                lag_parts,
            ) = kafka_lag()

            log.error(
                "Historical publisher "
                "failed "
                "window=%s..%s "
                "remaining_kafka_lag=%s "
                "partitions=%s",
                utc_text(
                    window.start
                ),
                utc_text(
                    window.stop
                ),
                lag_total,
                lag_parts,
            )

            raise

        (
            lag_total,
            lag_parts,
        ) = kafka_lag()

        if lag_total <= 0:
            raise RuntimeError(
                "Publisher completed but "
                "Kafka lag did not increase"
            )

        log.info(
            "Historical Kafka batch ready "
            "window=%s..%s "
            "messages=%s "
            "partitions=%s",
            utc_text(
                window.start
            ),
            utc_text(
                window.stop
            ),
            lag_total,
            lag_parts,
        )

        consume_command = [
            sys.executable,
            str(
                CONSUMER_SCRIPT
            ),
            "--max-messages",
            str(
                lag_total
            ),
        ]

        run_command(
            consume_command
        )

        (
            after_total,
            after_parts,
        ) = kafka_lag()

        if after_total != 0:
            raise RuntimeError(
                "Kafka lag remains after "
                "historical consume; "
                f"lag={after_total} "
                f"partitions="
                f"{after_parts}"
            )

        complete, summary = (
            verify_bronze_window(
                s3,
                settings.minio_bucket,
                window,
                countries,
                measurements,
            )
        )

        if not complete:

            for error in (
                summary[
                    "errors"
                ][:20]
            ):
                log.error(
                    "Bronze verify "
                    "error %s",
                    error,
                )

            raise RuntimeError(
                "Historical Bronze "
                "post-write verification "
                "failed "
                f"window="
                f"{utc_text(window.start)}"
                f".."
                f"{utc_text(window.stop)} "
                f"complete_combinations="
                f"{summary['complete_combinations']}"
                f"/"
                f"{summary['expected_combinations']}"
            )

        processed += 1

        log.info(
            "Bronze backfill window "
            "COMPLETE "
            "window=%s..%s "
            "objects=%s "
            "processed=%s",
            utc_text(
                window.start
            ),
            utc_text(
                window.stop
            ),
            summary[
                "total_objects"
            ],
            processed,
        )

    log.info(
        "Bronze backfill complete "
        "range=%s..%s "
        "processed=%s "
        "skipped=%s "
        "duration_seconds=%.2f",
        utc_text(start),
        utc_text(stop),
        processed,
        skipped,
        time.monotonic()
        - started,
    )


if __name__ == "__main__":
    main()