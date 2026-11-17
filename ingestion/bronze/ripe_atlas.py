"""RIPE Atlas Kafka event to global Bronze storage."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ingestion.ripe_atlas.models import parse_utc
from messaging.contracts import (
    SCHEMA_VERSION,
    build_event_id,
    build_window_id,
)


def validate_event(
    event: dict[str, Any],
    kafka_key: str | None,
) -> None:
    required = {
        "schema_version",
        "event_id",
        "window_id",
        "source",
        "dataset",
        "country_code",
        "measurement_id",
        "window_start_utc",
        "window_stop_utc",
        "fetched_at_utc",
        "chunk_index",
        "chunk_count",
        "payload",
    }

    missing = required - set(event)

    if missing:
        raise ValueError(
            f"Missing required fields: {sorted(missing)}"
        )

    if event["schema_version"] != SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported schema_version={event['schema_version']}"
        )

    if event["source"] != "ripe_atlas":
        raise ValueError("source must be ripe_atlas")

    if event["dataset"] != "ping":
        raise ValueError("dataset must be ping")

    country = str(event["country_code"]).upper()

    if len(country) != 2:
        raise ValueError("country_code must be ISO-2")

    measurement_id = str(event["measurement_id"])

    if not measurement_id.isdigit():
        raise ValueError(
            "RIPE Atlas measurement_id must be numeric"
        )

    chunk_index = event["chunk_index"]
    chunk_count = event["chunk_count"]

    if (
        not isinstance(chunk_index, int)
        or not isinstance(chunk_count, int)
        or chunk_index < 0
        or chunk_count <= 0
        or chunk_index >= chunk_count
    ):
        raise ValueError("Invalid chunk metadata")

    payload = event["payload"]

    if not isinstance(payload, dict):
        raise ValueError("payload must be an object")

    records = payload.get("records")

    if not isinstance(records, list):
        raise ValueError("payload.records must be a list")

    if payload.get("record_count") != len(records):
        raise ValueError(
            "payload.record_count does not match records"
        )

    start = parse_utc(event["window_start_utc"])
    stop = parse_utc(event["window_stop_utc"])

    if (stop - start).total_seconds() != 300:
        raise ValueError(
            "RIPE Kafka event must represent 5 minutes"
        )

    expected_window_id = build_window_id(
        source="ripe_atlas",
        dataset="ping",
        country_code=country,
        measurement_id=measurement_id,
        window_start_utc=event["window_start_utc"],
        window_stop_utc=event["window_stop_utc"],
    )

    if event["window_id"] != expected_window_id:
        raise ValueError("window_id mismatch")

    expected_event_id = build_event_id(
        window_id=expected_window_id,
        chunk_index=chunk_index,
    )

    if event["event_id"] != expected_event_id:
        raise ValueError("event_id mismatch")

    expected_key = (
        f"ripe_atlas|ping|{country}|{measurement_id}"
    )

    if kafka_key != expected_key:
        raise ValueError(
            f"Kafka key mismatch: {kafka_key!r}"
        )


def bronze_object_key(
    event: dict[str, Any],
) -> str:
    start = parse_utc(
        event["window_start_utc"]
    )

    country = str(
        event["country_code"]
    ).upper()

    measurement_id = str(
        event["measurement_id"]
    )

    event_id = str(
        event["event_id"]
    )

    return (
        "bronze/"
        "source=ripe_atlas/"
        "dataset=ping/"
        f"country={country}/"
        f"year={start:%Y}/"
        f"month={start:%m}/"
        f"day={start:%d}/"
        f"hour={start:%H}/"
        f"minute={start:%M}/"
        f"measurement_id={measurement_id}/"
        f"{event_id}.json"
    )


def build_bronze_envelope(
    event: dict[str, Any],
    *,
    kafka_topic: str,
    kafka_partition: int,
    kafka_offset: int,
    kafka_key: str | None,
) -> dict[str, Any]:
    payload = event["payload"]

    written_at = (
        datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )

    return {
        "metadata": {
            "schema_version": event["schema_version"],

            "source": event["source"],
            "dataset": event["dataset"],

            "event_id": event["event_id"],
            "window_id": event["window_id"],

            "country_code": (
                str(event["country_code"]).upper()
            ),

            "measurement_id": int(
                event["measurement_id"]
            ),

            "requested_start_utc":
                event["window_start_utc"],

            "requested_stop_utc":
                event["window_stop_utc"],

            "fetched_at_utc":
                event["fetched_at_utc"],

            "ingested_at_utc":
                written_at,

            "chunk_index":
                event["chunk_index"],

            "chunk_count":
                event["chunk_count"],

            "record_count":
                len(payload["records"]),

            "probe_scope_key":
                payload.get("probe_scope_key"),

            "probe_scope_reused":
                payload.get("probe_scope_reused"),

            "probe_status_filter":
                payload.get("probe_status_filter"),

            "coverage":
                payload.get("coverage"),

            "source_url":
                payload.get("source_url"),

            "kafka": {
                "topic": kafka_topic,
                "partition": kafka_partition,
                "offset": kafka_offset,
                "key": kafka_key,
            },
        },

        # Keep RIPE Atlas raw records unchanged.
        "records": payload["records"],
    }