#!/usr/bin/env python
"""Publish one 5-minute country-scoped RIPE Atlas window to Kafka."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sys
import time
from typing import Any


sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)


from configs.monitoring import (
    DEFAULT_COUNTRY_CODE,
    get_country,
    get_enabled_measurement_ids,
)

from configs.settings import settings

from ingestion.ripe_atlas.client import (
    RipeAtlasClient,
)

from ingestion.ripe_atlas.ingest import (
    coverage_metrics,
)

from ingestion.ripe_atlas.ingest_country import (
    get_or_create_probe_scope,
)

from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
    utc_text,
)

from messaging.contracts import (
    SCHEMA_VERSION,
    RipeAtlasPingEvent,
    build_event_id,
    build_window_id,
)

from messaging.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    RIPE_ATLAS_PING_TOPIC,
)

from messaging.producer import (
    JsonKafkaProducer,
)

from storage.minio_client import (
    create_s3_client,
)


# Keep enough headroom below Kafka's normal single-message limit.
MAX_RECORDS_JSON_BYTES = 700_000


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--country-code",
        default=DEFAULT_COUNTRY_CODE,
    )

    parser.add_argument(
        "--measurement-id",
        type=int,
        action="append",
        dest="measurement_ids",
    )

    parser.add_argument(
        "--start",
        required=True,
    )

    parser.add_argument(
        "--stop",
        required=True,
    )

    args = parser.parse_args()

    if args.measurement_ids is None:
        args.measurement_ids = (
            get_enabled_measurement_ids()
        )

    return args


def selected_window(
    args: argparse.Namespace,
) -> TimeWindow:
    window = TimeWindow(
        parse_utc(args.start),
        parse_utc(args.stop),
    )

    duration_seconds = (
        window.stop - window.start
    ).total_seconds()

    if duration_seconds != 300:
        raise ValueError(
            "Kafka RIPE ingestion requires "
            "an exact 5-minute window"
        )

    if (
        window.start.second != 0
        or window.start.microsecond != 0
        or window.start.minute % 5 != 0
    ):
        raise ValueError(
            "Window start must be aligned "
            "to a 5-minute UTC boundary"
        )

    return window


def normalize_measurement_ids(
    values: list[int],
) -> list[int]:
    ids = list(
        dict.fromkeys(
            int(value)
            for value in values
        )
    )

    if (
        not ids
        or any(
            measurement_id <= 0
            for measurement_id in ids
        )
    ):
        raise ValueError(
            "measurement_ids must contain positive IDs"
        )

    return ids


def chunk_records(
    records: list[dict[str, Any]],
    max_bytes: int = MAX_RECORDS_JSON_BYTES,
) -> list[list[dict[str, Any]]]:
    """Split raw RIPE records into bounded JSON chunks."""

    chunks: list[
        list[dict[str, Any]]
    ] = []

    current: list[
        dict[str, Any]
    ] = []

    current_bytes = 2

    for record in records:
        encoded = json.dumps(
            record,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        record_bytes = len(encoded)

        if record_bytes > max_bytes:
            raise ValueError(
                "Single RIPE Atlas record exceeds "
                f"Kafka chunk limit: {record_bytes}"
            )

        separator_bytes = (
            1 if current else 0
        )

        if (
            current
            and (
                current_bytes
                + separator_bytes
                + record_bytes
                > max_bytes
            )
        ):
            chunks.append(
                current
            )

            current = []
            current_bytes = 2
            separator_bytes = 0

        current.append(
            record
        )

        current_bytes += (
            separator_bytes
            + record_bytes
        )

    if current:
        chunks.append(
            current
        )

    # Preserve an empty RIPE window as a real ingestion event.
    if not chunks:
        chunks.append([])

    return chunks


def main() -> None:
    started = time.monotonic()

    logging.basicConfig(
        level=settings.log_level,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    log = logging.getLogger(
        __name__
    )

    args = arguments()

    country = get_country(
        args.country_code
    )

    if not country.enabled:
        raise ValueError(
            f"Country {country.code} is disabled"
        )

    measurement_ids = (
        normalize_measurement_ids(
            args.measurement_ids
        )
    )

    window = selected_window(
        args
    )

    # Probe-scope freezing currently uses MinIO.
    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    client = RipeAtlasClient(
        settings.ripe_atlas_base_url,
        settings.request_timeout,
        settings.max_retries,
        settings.backoff_seconds,
    )

    producer = JsonKafkaProducer(
        KAFKA_BOOTSTRAP_SERVERS
    )

    log.info(
        "RIPE Kafka publish start "
        "country=%s "
        "measurements=%s "
        "window=%s..%s "
        "topic=%s",
        country.code,
        measurement_ids,
        utc_text(window.start),
        utc_text(window.stop),
        RIPE_ATLAS_PING_TOPIC,
    )

    country_probe_ids_cache = None

    total_records = 0
    total_events = 0

    for measurement_id in measurement_ids:
        (
            probe_ids,
            probe_scope_key,
            probe_scope_reused,
            country_probe_ids_cache,
        ) = get_or_create_probe_scope(
            client=client,
            s3_client=s3,
            bucket=settings.minio_bucket,
            country_code=country.code,
            measurement_id=measurement_id,
            window=window,
            status_name="Connected",
            country_probe_ids=(
                country_probe_ids_cache
            ),
        )

        records = client.fetch_results(
            measurement_id,
            window,
            probe_ids=probe_ids,
        )

        metrics = coverage_metrics(
            probe_ids,
            records,
        )

        chunks = chunk_records(
            records
        )

        window_start = utc_text(
            window.start
        )

        window_stop = utc_text(
            window.stop
        )

        fetched_at = utc_text(
            datetime.now(
                timezone.utc
            )
        )

        window_id = build_window_id(
            source="ripe_atlas",
            dataset="ping",
            country_code=country.code,
            measurement_id=str(
                measurement_id
            ),
            window_start_utc=window_start,
            window_stop_utc=window_stop,
        )

        chunk_count = len(
            chunks
        )

        for chunk_index, chunk in enumerate(
            chunks
        ):
            event_id = build_event_id(
                window_id=window_id,
                chunk_index=chunk_index,
            )

            event = RipeAtlasPingEvent(
                schema_version=SCHEMA_VERSION,

                event_id=event_id,
                window_id=window_id,

                source="ripe_atlas",
                dataset="ping",

                country_code=country.code,
                measurement_id=str(
                    measurement_id
                ),

                window_start_utc=window_start,
                window_stop_utc=window_stop,
                fetched_at_utc=fetched_at,

                chunk_index=chunk_index,
                chunk_count=chunk_count,

                payload={
                    "records": chunk,
                    "record_count": len(
                        chunk
                    ),
                    "probe_scope_key":
                        probe_scope_key,
                    "probe_scope_reused":
                        probe_scope_reused,
                    "probe_status_filter":
                        "Connected",
                    "coverage": metrics,
                    "source_url":
                        client.results_url(
                            measurement_id
                        ),
                },
            )

            producer.publish(
                topic=RIPE_ATLAS_PING_TOPIC,
                key=event.kafka_key,
                payload=event.to_dict(),
            )

            total_events += 1

            log.info(
                "Kafka event published "
                "country=%s "
                "measurement=%s "
                "chunk=%s/%s "
                "records=%s "
                "event_id=%s",
                country.code,
                measurement_id,
                chunk_index + 1,
                chunk_count,
                len(chunk),
                event_id,
            )

        total_records += len(
            records
        )

        log.info(
            "RIPE measurement published "
            "country=%s "
            "measurement=%s "
            "records=%s "
            "chunks=%s "
            "coverage_pct=%.2f",
            country.code,
            measurement_id,
            len(records),
            chunk_count,
            metrics["coverage_pct"],
        )

    log.info(
        "RIPE Kafka publish complete "
        "country=%s "
        "measurements=%s "
        "records=%s "
        "events=%s "
        "duration_seconds=%.2f",
        country.code,
        len(measurement_ids),
        total_records,
        total_events,
        time.monotonic() - started,
    )


if __name__ == "__main__":
    main()