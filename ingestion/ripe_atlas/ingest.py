"""RIPE Atlas to Bronze orchestration."""

from datetime import datetime, timezone
import logging
from typing import Any

from ingestion.ripe_atlas.client import RipeAtlasClient
from ingestion.ripe_atlas.models import TimeWindow, utc_text
from storage.minio_client import ensure_bucket, put_json

LOGGER = logging.getLogger(__name__)


def bronze_object_key(measurement_id: int, window: TimeWindow) -> str:
    """Build the deterministic key used for idempotent window replacement."""
    start = window.start
    start_token = start.strftime("%Y%m%dT%H%M%SZ")
    stop_token = window.stop.strftime("%Y%m%dT%H%M%SZ")
    return (
        "bronze/ripe_atlas/ping/"
        f"measurement_id={measurement_id}/year={start:%Y}/month={start:%m}/"
        f"day={start:%d}/hour={start:%H}/"
        f"{measurement_id}_{start_token}_{stop_token}.json"
    )


def build_envelope(measurement_id: int, window: TimeWindow,
                   records: list[dict[str, Any]], api_url: str,
                   ingested_at: datetime | None = None,
                   extra_metadata: dict[str, Any] | None = None
                   ) -> dict[str, Any]:
    """Keep API records unchanged and attach separate lineage metadata."""
    now = ingested_at or datetime.now(timezone.utc)
    metadata = {"source": "ripe_atlas", "measurement_id": measurement_id,
                "requested_start_utc": utc_text(window.start),
                "requested_stop_utc": utc_text(window.stop),
                "ingested_at_utc": utc_text(now),
                "record_count": len(records), "api_url": api_url,
                "api_version": "v2"}
    if extra_metadata:
        metadata.update(extra_metadata)
    return {"metadata": metadata, "records": records}


def coverage_metrics(probe_ids: list[int],
                     records: list[dict[str, Any]]) -> dict[str, Any]:
    """Calculate probe coverage using distinct valid probe IDs."""
    requested = sorted(set(probe_ids))
    returned = {
        record["prb_id"]
        for record in records
        if isinstance(record.get("prb_id"), int)
    }
    missing = set(requested) - returned
    coverage = (
        len(set(requested) & returned) / len(requested) * 100.0
        if requested else 0.0
    )
    return {
        "requested_probe_count": len(requested),
        "returned_unique_probe_count": len(returned),
        "missing_probe_count": len(missing),
        "coverage_pct": coverage,
    }


def store_bronze_records(
        client: RipeAtlasClient, s3_client: Any, bucket: str,
        measurement_id: int, window: TimeWindow,
        records: list[dict[str, Any]],
        extra_metadata: dict[str, Any] | None = None) -> str:
    """Store raw records and lineage at the deterministic Bronze key."""
    envelope = build_envelope(
        measurement_id, window, records, client.results_url(measurement_id),
        extra_metadata=extra_metadata,
    )
    key = bronze_object_key(measurement_id, window)
    ensure_bucket(s3_client, bucket)
    put_json(s3_client, bucket, key, envelope)
    LOGGER.info("Bronze object path=s3://%s/%s record_count=%s",
                bucket, key, len(records))
    return key


def ingest_window(client: RipeAtlasClient, s3_client: Any, bucket: str,
                  measurement_id: int, window: TimeWindow) -> tuple[str, int]:
    """Fetch one bounded window and atomically replace its Bronze object."""
    records = client.fetch_results(measurement_id, window)
    key = store_bronze_records(
        client, s3_client, bucket, measurement_id, window, records
    )
    return key, len(records)


def ingest_filtered_measurements(
        client: RipeAtlasClient, s3_client: Any, bucket: str,
        measurement_ids: list[int], window: TimeWindow,
        country_code: str = "VN") -> list[dict[str, Any]]:
    """Discover connected probes once and ingest filtered measurements."""
    probe_ids = sorted(set(client.get_connected_probe_ids(country_code)))
    if not probe_ids:
        raise ValueError(
            f"No connected RIPE Atlas probes found for {country_code.upper()}"
        )
    unique_measurement_ids = list(dict.fromkeys(measurement_ids))
    if not unique_measurement_ids or any(
            measurement_id <= 0 for measurement_id in unique_measurement_ids):
        raise ValueError("measurement_ids must contain positive IDs")
    summaries: list[dict[str, Any]] = []
    total = len(unique_measurement_ids)
    for index, measurement_id in enumerate(unique_measurement_ids, start=1):
        LOGGER.info("[%s/%s] Fetching measurement %s window=%s..%s",
                    index, total, measurement_id, utc_text(window.start),
                    utc_text(window.stop))
        records = client.fetch_results(
            measurement_id, window, probe_ids=probe_ids
        )
        metrics = coverage_metrics(probe_ids, records)
        metadata = {
            "country_code": country_code.upper(),
            "probe_status_filter": "Connected",
            "requested_probe_ids": probe_ids,
            **metrics,
        }
        key = store_bronze_records(
            client, s3_client, bucket, measurement_id, window, records,
            extra_metadata=metadata,
        )
        LOGGER.info(
            "Measurement %s records=%s probes=%s/%s coverage=%.2f%% stored=s3://%s/%s",
            measurement_id, len(records),
            metrics["returned_unique_probe_count"],
            metrics["requested_probe_count"], metrics["coverage_pct"],
            bucket, key,
        )
        if metrics["missing_probe_count"]:
            LOGGER.warning(
                "Measurement %s missing results from %s requested probes",
                measurement_id, metrics["missing_probe_count"],
            )
        summaries.append({
            "measurement_id": measurement_id,
            "object_key": key,
            "record_count": len(records),
            **metrics,
        })
    return summaries
