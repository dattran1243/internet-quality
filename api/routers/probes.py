"""Probe map endpoints."""

from datetime import datetime
from datetime import timezone

from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query

from api.clickhouse import (
    ClickHouseError,
    clickhouse,
)
from api.models.probe import (
    ProbeMapPoint,
)
from configs.monitoring import (
    DEFAULT_COUNTRY_CODE,
    get_country,
)


router = APIRouter(
    prefix="/probes",
    tags=["probes"],
)


def normalize_utc(
    value: datetime,
    field_name: str,
) -> datetime:
    """Require timezone-aware datetime and normalize to UTC."""

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
    "/map",
    response_model=list[ProbeMapPoint],
)
def probe_map(
    measurement_id: int = Query(
        ...,
        gt=0,
        description=(
            "RIPE Atlas measurement ID"
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
    at: datetime | None = Query(
        default=None,
        description=(
            "Optional UTC hour. "
            "If omitted, the latest available hour "
            "for the country and measurement is returned."
        ),
    ),
):
    """
    Return probe locations and quality metrics for map rendering.

    RIPE Atlas coordinates should be treated as approximate,
    not exact device locations.

    If 'at' is omitted:
        return latest available hour for the selected
        country + measurement.

    If 'at' is supplied:
        return the hour containing that timestamp.
    """

    country = normalize_country(
        country_code
    )

    # ---------------------------------------------------------
    # Resolve requested hour
    # ---------------------------------------------------------

    if at is None:
        hour_condition = f"""
q.hour_start_utc = (
    SELECT max(hour_start_utc)
    FROM hourly_network_quality
    WHERE
        country_code = '{country}'
        AND measurement_id = {int(measurement_id)}
)
"""

    else:
        at_utc = normalize_utc(
            at,
            "at",
        )

        hour_start = at_utc.replace(
            minute=0,
            second=0,
            microsecond=0,
        )

        hour_text = clickhouse_datetime(
            hour_start
        )

        hour_condition = f"""
q.hour_start_utc = toDateTime64(
    '{hour_text}',
    3,
    'UTC'
)
"""

    # ---------------------------------------------------------
    # Query
    # ---------------------------------------------------------

    sql = f"""
SELECT
    q.hour_start_utc,

    q.measurement_id,
    q.probe_id,
    q.target,

    q.country_code,

    q.asn_v4,
    q.asn_v6,

    q.latitude,
    q.longitude,

    q.is_anchor,

    q.sample_count,

    q.avg_rtt_ms,
    q.p95_rtt_ms,

    q.weighted_packet_loss_pct
        AS packet_loss_pct,

    i.incident_status
        AS measurement_incident_status

FROM hourly_network_quality AS q

LEFT JOIN incident_candidates AS i
    ON
        q.hour_start_utc = i.hour_start_utc
        AND q.country_code = i.country_code
        AND q.measurement_id = i.measurement_id

WHERE
    q.country_code = '{country}'

    AND q.measurement_id = {int(measurement_id)}

    AND {hour_condition}

    AND q.latitude IS NOT NULL
    AND q.longitude IS NOT NULL

ORDER BY
    q.probe_id
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
                    "Could not query probe map data"
                ),
            },
        ) from exc

    return rows