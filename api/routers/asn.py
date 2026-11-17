"""ASN summary endpoints."""

from datetime import datetime
from datetime import timezone

from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query

from api.clickhouse import (
    ClickHouseError,
    clickhouse,
)
from api.models.asn import (
    ASNQualitySummary,
)
from configs.monitoring import (
    DEFAULT_COUNTRY_CODE,
    get_country,
)


router = APIRouter(
    prefix="/asn",
    tags=["asn"],
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
    "/summary",
    response_model=list[ASNQualitySummary],
)
def asn_summary(
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
):
    """
    Return hourly IPv4 ASN network quality summaries
    for one country scope.

    Window semantics:
        [start, stop)

    This endpoint summarizes quality by ASN.
    It does not expose the ASN degradation Gold dataset yet.

    measurement_incident_status is measurement-level
    incident context, not proof that the ASN caused
    the observed degradation.
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

    sql = f"""
SELECT
    q.hour_start_utc,
    q.measurement_id,
    q.target,
    q.asn_v4,

    q.probe_count,
    q.sample_count,

    q.avg_rtt_ms,
    q.avg_probe_p95_rtt_ms,

    if(
        q.packet_sent_sum > 0,
        (
            (
                q.packet_sent_sum
                - q.packet_received_sum
            )
            * 100.0
            / q.packet_sent_sum
        ),
        NULL
    ) AS packet_loss_pct,

    q.packet_sent_sum
        AS total_packets_sent,

    q.packet_received_sum
        AS total_packets_received,

    i.incident_status
        AS measurement_incident_status

FROM
(
    SELECT
        hour_start_utc,
        country_code,
        measurement_id,
        asn_v4,

        any(target)
            AS target,

        uniqExact(probe_id)
            AS probe_count,

        sum(sample_count)
            AS sample_count,

        avg(avg_rtt_ms)
            AS avg_rtt_ms,

        avg(p95_rtt_ms)
            AS avg_probe_p95_rtt_ms,

        sum(total_packets_sent)
            AS packet_sent_sum,

        sum(total_packets_received)
            AS packet_received_sum

    FROM hourly_network_quality

    WHERE
        country_code = '{country}'

        AND measurement_id = {
            int(measurement_id)
        }

        AND hour_start_utc >= toDateTime64(
            '{start_text}',
            3,
            'UTC'
        )

        AND hour_start_utc < toDateTime64(
            '{stop_text}',
            3,
            'UTC'
        )

        AND asn_v4 IS NOT NULL
        AND asn_v4 > 0

    GROUP BY
        hour_start_utc,
        country_code,
        measurement_id,
        asn_v4
) AS q

LEFT JOIN incident_candidates AS i
    ON
        q.hour_start_utc = i.hour_start_utc
        AND q.country_code = i.country_code
        AND q.measurement_id = i.measurement_id

ORDER BY
    q.hour_start_utc,
    q.asn_v4
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
                    "Could not query ASN summary data"
                ),
            },
        ) from exc

    return rows