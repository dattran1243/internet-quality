"""Country-scoped RIPE Atlas to Bronze orchestration.

This module intentionally lives beside the legacy ingest.py.

Legacy Bronze:
    bronze/ripe_atlas/ping/measurement_id=...

Country-scoped Bronze:
    bronze/ripe_atlas/ping_by_country/
        country=VN/
        measurement_id=...

Probe scopes are frozen per country + measurement + requested window so
rerunning the same window does not silently change the requested probe
inventory.

For newly-created scopes, eligibility is defined as:

    country Connected probes
    INTERSECT
    measurement current participant probes

The measurement participant list is a snapshot taken when the scope is
created. It must not be interpreted as a historical reconstruction of
measurement participation.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from typing import Any

from botocore.exceptions import ClientError

from ingestion.ripe_atlas.client import RipeAtlasClient
from ingestion.ripe_atlas.ingest import (
    build_envelope,
    coverage_metrics,
)
from ingestion.ripe_atlas.models import (
    TimeWindow,
    utc_text,
)
from storage.minio_client import (
    ensure_bucket,
    put_json,
)


LOGGER = logging.getLogger(__name__)


PROBE_SCOPE_SCHEMA_VERSION = 2


def normalize_country_code(
    country_code: str,
) -> str:
    """Normalize and validate a two-letter country code."""

    country = (
        country_code
        .strip()
        .upper()
    )

    if (
        len(country) != 2
        or not country.isalpha()
    ):
        raise ValueError(
            "country_code must be a two-letter ISO code"
        )

    return country


def normalize_status_name(
    status_name: str,
) -> str:
    """Normalize a non-empty RIPE Atlas probe status."""

    status = status_name.strip()

    if not status:
        raise ValueError(
            "status_name must not be empty"
        )

    return status


def country_bronze_object_key(
    measurement_id: int,
    window: TimeWindow,
    country_code: str,
) -> str:
    """Build deterministic Bronze key scoped by country."""

    if measurement_id <= 0:
        raise ValueError(
            "measurement_id must be positive"
        )

    country = normalize_country_code(
        country_code
    )

    start = window.start

    start_token = (
        start.strftime(
            "%Y%m%dT%H%M%SZ"
        )
    )

    stop_token = (
        window.stop.strftime(
            "%Y%m%dT%H%M%SZ"
        )
    )

    return (
        "bronze/ripe_atlas/"
        "ping_by_country/"
        f"country={country}/"
        f"measurement_id={measurement_id}/"
        f"year={start:%Y}/"
        f"month={start:%m}/"
        f"day={start:%d}/"
        f"hour={start:%H}/"
        f"{measurement_id}_"
        f"{start_token}_"
        f"{stop_token}.json"
    )


def probe_scope_object_key(
    country_code: str,
    measurement_id: int,
    window: TimeWindow,
) -> str:
    """
    Build deterministic key for one frozen
    country + measurement + window probe scope.
    """

    country = normalize_country_code(
        country_code
    )

    if measurement_id <= 0:
        raise ValueError(
            "measurement_id must be positive"
        )

    start = window.start

    start_token = (
        start.strftime(
            "%Y%m%dT%H%M%SZ"
        )
    )

    stop_token = (
        window.stop.strftime(
            "%Y%m%dT%H%M%SZ"
        )
    )

    return (
        "bronze/ripe_atlas/"
        "probe_scope/"
        f"country={country}/"
        f"measurement_id={measurement_id}/"
        f"year={start:%Y}/"
        f"month={start:%m}/"
        f"day={start:%d}/"
        f"hour={start:%H}/"
        f"{country}_"
        f"{measurement_id}_"
        f"{start_token}_"
        f"{stop_token}.json"
    )


def _read_json_object(
    s3_client: Any,
    bucket: str,
    key: str,
) -> dict[str, Any] | None:
    """Read a JSON object, returning None only when the key does not exist."""

    try:
        response = s3_client.get_object(
            Bucket=bucket,
            Key=key,
        )

    except ClientError as exc:
        code = str(
            exc.response
            .get("Error", {})
            .get("Code", "")
        )

        if code in {
            "404",
            "NoSuchKey",
            "NoSuchObject",
            "NotFound",
        }:
            return None

        raise

    body = response["Body"].read()

    try:
        payload = json.loads(
            body.decode("utf-8")
        )

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise ValueError(
            f"Invalid JSON object: s3://{bucket}/{key}"
        ) from exc

    if not isinstance(
        payload,
        dict,
    ):
        raise ValueError(
            "Expected JSON object at "
            f"s3://{bucket}/{key}"
        )

    return payload


def _normalize_probe_ids(
    probe_ids: list[int],
) -> list[int]:
    """Return deterministic unique positive RIPE Atlas probe IDs."""

    normalized = sorted(
        set(
            int(probe_id)
            for probe_id in probe_ids
        )
    )

    if (
        not normalized
        or any(
            probe_id <= 0
            for probe_id in normalized
        )
    ):
        raise ValueError(
            "probe_ids must contain positive IDs"
        )

    return normalized


def _validate_probe_scope_manifest(
    manifest: dict[str, Any],
    country_code: str,
    measurement_id: int,
    status_name: str,
    window: TimeWindow,
) -> list[int]:
    """
    Validate a persisted measurement-aware probe-scope
    manifest and return its probe IDs.
    """

    country = normalize_country_code(
        country_code
    )

    status = normalize_status_name(
        status_name
    )

    if measurement_id <= 0:
        raise ValueError(
            "measurement_id must be positive"
        )

    errors: list[str] = []

    if manifest.get(
        "schema_version"
    ) != PROBE_SCOPE_SCHEMA_VERSION:
        errors.append(
            "unexpected schema_version"
        )

    if manifest.get(
        "source"
    ) != "ripe_atlas":
        errors.append(
            "source must be ripe_atlas"
        )

    if manifest.get(
        "scope_type"
    ) != "country_measurement":
        errors.append(
            "scope_type must be country_measurement"
        )

    if str(
        manifest.get(
            "country_code",
            "",
        )
    ).upper() != country:
        errors.append(
            f"country_code must be {country}"
        )

    if manifest.get(
        "measurement_id"
    ) != measurement_id:
        errors.append(
            "measurement_id does not match"
        )

    if manifest.get(
        "status_name"
    ) != status:
        errors.append(
            f"status_name must be {status}"
        )

    if manifest.get(
        "requested_start_utc"
    ) != utc_text(
        window.start
    ):
        errors.append(
            "requested_start_utc does not match"
        )

    if manifest.get(
        "requested_stop_utc"
    ) != utc_text(
        window.stop
    ):
        errors.append(
            "requested_stop_utc does not match"
        )

    raw_probe_ids = manifest.get(
        "requested_probe_ids"
    )

    if not isinstance(
        raw_probe_ids,
        list,
    ):
        errors.append(
            "requested_probe_ids must be a list"
        )

        probe_ids: list[int] = []

    else:
        try:
            probe_ids = (
                _normalize_probe_ids(
                    raw_probe_ids
                )
            )

        except (
            TypeError,
            ValueError,
        ):
            errors.append(
                "requested_probe_ids are invalid"
            )

            probe_ids = []

    if (
        isinstance(
            raw_probe_ids,
            list,
        )
        and probe_ids
        and raw_probe_ids != probe_ids
    ):
        errors.append(
            "requested_probe_ids must be sorted and unique"
        )

    probe_count = manifest.get(
        "probe_count"
    )

    if (
        probe_ids
        and probe_count
        != len(
            probe_ids
        )
    ):
        errors.append(
            "probe_count does not match requested_probe_ids"
        )

    discovered_at = manifest.get(
        "discovered_at_utc"
    )

    if (
        not isinstance(
            discovered_at,
            str,
        )
        or not discovered_at
    ):
        errors.append(
            "discovered_at_utc must be present"
        )

    snapshot_at = manifest.get(
        "participant_snapshot_at_utc"
    )

    if (
        not isinstance(
            snapshot_at,
            str,
        )
        or not snapshot_at
    ):
        errors.append(
            "participant_snapshot_at_utc must be present"
        )

    country_count = manifest.get(
        "country_connected_probe_count"
    )

    if (
        not isinstance(
            country_count,
            int,
        )
        or isinstance(
            country_count,
            bool,
        )
        or country_count
        < len(
            probe_ids
        )
    ):
        errors.append(
            "country_connected_probe_count is invalid"
        )

    participant_count = manifest.get(
        "measurement_participant_count"
    )

    if (
        not isinstance(
            participant_count,
            int,
        )
        or isinstance(
            participant_count,
            bool,
        )
        or participant_count
        < len(
            probe_ids
        )
    ):
        errors.append(
            "measurement_participant_count is invalid"
        )

    if errors:
        raise ValueError(
            "Invalid frozen probe scope: "
            + "; ".join(
                errors
            )
        )

    return probe_ids


def get_or_create_probe_scope(
    client: RipeAtlasClient,
    s3_client: Any,
    bucket: str,
    country_code: str,
    measurement_id: int,
    window: TimeWindow,
    status_name: str = "Connected",
    country_probe_ids: list[int] | None = None,
) -> tuple[
    list[int],
    str,
    bool,
    list[int] | None,
]:
    """
    Return a frozen country + measurement + window
    eligible probe scope.

    Eligibility for a newly-created scope is:

        country Connected probes
        INTERSECT
        measurement current participants

    The measurement participant list is a snapshot taken
    when this manifest is created. It is not a historical
    participant reconstruction.

    Returns:
        eligible probe IDs
        manifest object key
        True if existing manifest was reused
        cached country Connected probe IDs, when available
    """

    country = normalize_country_code(
        country_code
    )

    status = normalize_status_name(
        status_name
    )

    if measurement_id <= 0:
        raise ValueError(
            "measurement_id must be positive"
        )

    key = probe_scope_object_key(
        country,
        measurement_id,
        window,
    )

    ensure_bucket(
        s3_client,
        bucket,
    )

    existing = _read_json_object(
        s3_client,
        bucket,
        key,
    )

    if existing is not None:
        probe_ids = (
            _validate_probe_scope_manifest(
                existing,
                country,
                measurement_id,
                status,
                window,
            )
        )

        LOGGER.info(
            "Reusing frozen probe scope "
            "country=%s measurement=%s "
            "status=%s probes=%s "
            "path=s3://%s/%s",
            country,
            measurement_id,
            status,
            len(probe_ids),
            bucket,
            key,
        )

        return (
            probe_ids,
            key,
            True,
            country_probe_ids,
        )

    LOGGER.info(
        "No frozen probe scope found; "
        "discovering measurement-aware scope "
        "country=%s measurement=%s status=%s",
        country,
        measurement_id,
        status,
    )

    if country_probe_ids is None:
        country_probe_ids = (
            _normalize_probe_ids(
                client.get_connected_probe_ids(
                    country,
                    status,
                )
            )
        )

    else:
        country_probe_ids = (
            _normalize_probe_ids(
                country_probe_ids
            )
        )

    measurement_probe_ids = (
        client.get_measurement_current_probe_ids(
            measurement_id
        )
    )

    if not measurement_probe_ids:
        raise ValueError(
            "Measurement has no current participant probes: "
            f"{measurement_id}"
        )

    eligible_probe_ids = sorted(
        set(
            country_probe_ids
        )
        & set(
            measurement_probe_ids
        )
    )

    if not eligible_probe_ids:
        raise ValueError(
            "No eligible probes for "
            f"country={country} "
            f"measurement={measurement_id}"
        )

    discovered_at = datetime.now(
        timezone.utc
    )

    manifest = {
        "schema_version": (
            PROBE_SCOPE_SCHEMA_VERSION
        ),

        "source":
            "ripe_atlas",

        "scope_type":
            "country_measurement",

        "country_code":
            country,

        "measurement_id":
            measurement_id,

        "status_name":
            status,

        "requested_start_utc": (
            utc_text(
                window.start
            )
        ),

        "requested_stop_utc": (
            utc_text(
                window.stop
            )
        ),

        "discovered_at_utc": (
            utc_text(
                discovered_at
            )
        ),

        "participant_snapshot_at_utc": (
            utc_text(
                discovered_at
            )
        ),

        "country_connected_probe_count": (
            len(
                country_probe_ids
            )
        ),

        "measurement_participant_count": (
            len(
                measurement_probe_ids
            )
        ),

        "probe_count": (
            len(
                eligible_probe_ids
            )
        ),

        "requested_probe_ids": (
            eligible_probe_ids
        ),
    }

    put_json(
        s3_client,
        bucket,
        key,
        manifest,
    )

    LOGGER.info(
        "Created frozen probe scope "
        "country=%s measurement=%s "
        "status=%s eligible=%s "
        "country_connected=%s "
        "measurement_participants=%s "
        "path=s3://%s/%s",
        country,
        measurement_id,
        status,
        len(
            eligible_probe_ids
        ),
        len(
            country_probe_ids
        ),
        len(
            measurement_probe_ids
        ),
        bucket,
        key,
    )

    return (
        eligible_probe_ids,
        key,
        False,
        country_probe_ids,
    )


def store_country_bronze_records(
    client: RipeAtlasClient,
    s3_client: Any,
    bucket: str,
    measurement_id: int,
    window: TimeWindow,
    country_code: str,
    records: list[dict[str, Any]],
    extra_metadata: dict[str, Any] | None = None,
) -> str:
    """Store raw country-filtered records at a deterministic key."""

    country = normalize_country_code(
        country_code
    )

    metadata = dict(
        extra_metadata
        or {}
    )

    existing_country = metadata.get(
        "country_code"
    )

    if (
        existing_country is not None
        and str(
            existing_country
        ).upper() != country
    ):
        raise ValueError(
            "extra_metadata country_code "
            "does not match requested country"
        )

    metadata["country_code"] = (
        country
    )

    envelope = build_envelope(
        measurement_id,
        window,
        records,
        client.results_url(
            measurement_id
        ),
        extra_metadata=metadata,
    )

    key = country_bronze_object_key(
        measurement_id,
        window,
        country,
    )

    ensure_bucket(
        s3_client,
        bucket,
    )

    put_json(
        s3_client,
        bucket,
        key,
        envelope,
    )

    LOGGER.info(
        "Country Bronze object "
        "country=%s "
        "path=s3://%s/%s "
        "record_count=%s",
        country,
        bucket,
        key,
        len(records),
    )

    return key


def ingest_country_measurements(
    client: RipeAtlasClient,
    s3_client: Any,
    bucket: str,
    measurement_ids: list[int],
    window: TimeWindow,
    country_code: str,
    status_name: str = "Connected",
) -> list[dict[str, Any]]:
    """
    Ingest selected measurements using frozen
    country + measurement + window probe scopes.
    """

    country = normalize_country_code(
        country_code
    )

    status = normalize_status_name(
        status_name
    )

    unique_measurement_ids = list(
        dict.fromkeys(
            int(value)
            for value in measurement_ids
        )
    )

    if (
        not unique_measurement_ids
        or any(
            measurement_id <= 0
            for measurement_id
            in unique_measurement_ids
        )
    ):
        raise ValueError(
            "measurement_ids must contain positive IDs"
        )

    summaries: list[
        dict[str, Any]
    ] = []

    total = len(
        unique_measurement_ids
    )

    # Lazily populated the first time a new probe scope
    # needs to be created. All new measurement scopes in
    # this ingestion run may reuse the same country
    # Connected-probe snapshot.
    country_probe_ids_cache: (
        list[int] | None
    ) = None

    for index, measurement_id in enumerate(
        unique_measurement_ids,
        start=1,
    ):
        (
            probe_ids,
            probe_scope_key,
            reused_probe_scope,
            country_probe_ids_cache,
        ) = get_or_create_probe_scope(
            client=client,
            s3_client=s3_client,
            bucket=bucket,
            country_code=country,
            measurement_id=measurement_id,
            window=window,
            status_name=status,
            country_probe_ids=(
                country_probe_ids_cache
            ),
        )

        LOGGER.info(
            "Measurement probe scope "
            "country=%s measurement=%s "
            "eligible_probes=%s reused=%s "
            "scope_key=%s",
            country,
            measurement_id,
            len(
                probe_ids
            ),
            reused_probe_scope,
            probe_scope_key,
        )

        LOGGER.info(
            "[%s/%s] Fetching "
            "country=%s measurement=%s "
            "window=%s..%s",
            index,
            total,
            country,
            measurement_id,
            utc_text(
                window.start
            ),
            utc_text(
                window.stop
            ),
        )

        records = (
            client.fetch_results(
                measurement_id,
                window,
                probe_ids=probe_ids,
            )
        )

        metrics = coverage_metrics(
            probe_ids,
            records,
        )

        metadata = {
            "country_code":
                country,

            "probe_status_filter":
                status,

            "requested_probe_ids": (
                probe_ids
            ),

            "probe_scope_key": (
                probe_scope_key
            ),

            "probe_scope_reused": (
                reused_probe_scope
            ),

            **metrics,
        }

        key = (
            store_country_bronze_records(
                client,
                s3_client,
                bucket,
                measurement_id,
                window,
                country,
                records,
                extra_metadata=metadata,
            )
        )

        LOGGER.info(
            "Country measurement complete "
            "country=%s "
            "measurement=%s "
            "records=%s "
            "probes=%s/%s "
            "coverage=%.2f%% "
            "stored=s3://%s/%s",
            country,
            measurement_id,
            len(
                records
            ),
            metrics[
                "returned_unique_probe_count"
            ],
            metrics[
                "requested_probe_count"
            ],
            metrics[
                "coverage_pct"
            ],
            bucket,
            key,
        )

        if metrics[
            "missing_probe_count"
        ]:
            LOGGER.warning(
                "Measurement %s country=%s "
                "missing results from %s "
                "requested probes",
                measurement_id,
                country,
                metrics[
                    "missing_probe_count"
                ],
            )

        summaries.append({
            "country_code":
                country,

            "measurement_id": (
                measurement_id
            ),

            "object_key":
                key,

            "probe_scope_key": (
                probe_scope_key
            ),

            "probe_scope_reused": (
                reused_probe_scope
            ),

            "record_count": (
                len(
                    records
                )
            ),

            **metrics,
        })

    return summaries