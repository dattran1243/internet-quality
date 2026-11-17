"""Incident endpoints."""

from datetime import datetime
from datetime import timezone
from typing import Literal

from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query

from api.clickhouse import (
    ClickHouseError,
    clickhouse,
)
from api.models.incident import (
    IncidentRecord,
)
from configs.monitoring import (
    DEFAULT_COUNTRY_CODE,
    get_country,
)


router = APIRouter(
    prefix="/incidents",
    tags=["incidents"],
)


IncidentStatus = Literal[
    "insufficient_data",
    "normal",
    "observe",
    "warning",
    "critical",
]


def normalize_utc(
    value: datetime,
    field_name: str,
) -> datetime:
    """Require timezone-aware timestamps and normalize to UTC."""

    if value.tzinfo is None:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{field_name} must include "
                "a timezone, preferably Z/UTC"
            ),
        )

    return value.astimezone(
        timezone.utc
    )


def clickhouse_datetime(
    value: datetime,
) -> str:
    """Format UTC datetime for ClickHouse DateTime64."""

    return value.strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def normalize_country(
    value: str,
) -> str:
    """
    Validate country scope against the monitoring registry.

    Only enabled countries are queryable.
    """

    try:
        country = get_country(
            value
        )

    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Unsupported country_code: {value}"
            ),
        ) from exc

    if not country.enabled:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Country {country.code} "
                "is currently disabled"
            ),
        )

    return country.code


@router.get(
    "",
    response_model=list[IncidentRecord],
)
def list_incidents(
    start: datetime = Query(
        ...,
        description=(
            "Inclusive UTC start. "
            "Example: 2026-09-20T00:00:00Z"
        ),
    ),
    stop: datetime = Query(
        ...,
        description=(
            "Exclusive UTC stop. "
            "Example: 2026-09-21T01:00:00Z"
        ),
    ),
    country_code: str = Query(
        DEFAULT_COUNTRY_CODE,
        min_length=2,
        max_length=2,
        description=(
            "Two-letter country code. "
            f"Default: {DEFAULT_COUNTRY_CODE}"
        ),
    ),
    measurement_id: int | None = Query(
        default=None,
        gt=0,
        description=(
            "Optional RIPE Atlas measurement ID"
        ),
    ),
    status: IncidentStatus | None = Query(
        default=None,
        description=(
            "Optional incident status filter"
        ),
    ),
    only_candidates: bool = Query(
        default=True,
        description=(
            "When true, return only warning/critical "
            "incident candidates."
        ),
    ),
    limit: int = Query(
        default=200,
        ge=1,
        le=1000,
        description=(
            "Maximum number of rows returned"
        ),
    ),
):
    """
    Return incident observations from ClickHouse
    for one country scope.

    Window semantics:
        [start, stop)

    By default only actual incident candidates are returned.
    Set only_candidates=false to inspect normal/observe/
    insufficient_data rows as well.
    """

    country = normalize_country(
        country_code
    )

    start_utc = normalize_utc(
        start,
        "start",
    )

    stop_utc = normalize_utc(
        stop,
        "stop",
    )

    if start_utc >= stop_utc:
        raise HTTPException(
            status_code=422,
            detail=(
                "start must be before stop"
            ),
        )

    start_text = clickhouse_datetime(
        start_utc
    )

    stop_text = clickhouse_datetime(
        stop_utc
    )

    conditions = [
        (
            f"country_code = '{country}'"
        ),
        (
            "hour_start_utc >= "
            "toDateTime64("
            f"'{start_text}', 3, 'UTC'"
            ")"
        ),
        (
            "hour_start_utc < "
            "toDateTime64("
            f"'{stop_text}', 3, 'UTC'"
            ")"
        ),
    ]

    if measurement_id is not None:
        conditions.append(
            f"measurement_id = {int(measurement_id)}"
        )

    if status is not None:
        conditions.append(
            f"incident_status = '{status}'"
        )

    if only_candidates:
        conditions.append(
            "incident_candidate = true"
        )

    where_sql = "\n    AND ".join(
        conditions
    )

    sql = f"""
SELECT
    hour_start_utc,

    measurement_id,
    target,

    observed_probe_count,
    eligible_probe_count,

    warning_probe_count,
    critical_probe_count,
    affected_probe_count,

    observed_asn_count,
    eligible_asn_count,

    warning_asn_count,
    critical_asn_count,
    affected_asn_count,

    max_probe_latency_ratio,
    max_probe_latency_delta_ms,
    max_probe_packet_loss_pct,

    max_asn_affected_ratio,

    coverage_status,
    probe_coverage_ratio,

    incident_status,
    incident_candidate,

    severity_score,

    signal_types,
    incident_summary

FROM incident_candidates

WHERE
    {where_sql}

ORDER BY
    severity_score DESC,
    hour_start_utc DESC,
    measurement_id

LIMIT {int(limit)}
"""

    try:
        rows = clickhouse.query_json(
            sql
        )

    except ClickHouseError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "message": (
                    "Could not query incident data"
                ),
            },
        ) from exc

    return rows