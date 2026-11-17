"""Network quality endpoints."""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query

from api.clickhouse import (
    ClickHouseError,
    clickhouse,
)
from api.models.quality import HourlyQualityPoint
from configs.monitoring import (
    DEFAULT_COUNTRY_CODE,
    get_country,
)


router = APIRouter(
    prefix="/quality",
    tags=["quality"],
)


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

    Only enabled countries are queryable by the product API.
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
    "/hourly",
    response_model=list[
        HourlyQualityPoint
    ],
)
def hourly_quality(
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
    Return hourly quality time series for one
    measurement inside one country scope.

    Window semantics:
        [start, stop)
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

    start_text = (
        clickhouse_datetime(
            start_utc
        )
    )

    stop_text = (
        clickhouse_datetime(
            stop_utc
        )
    )

    sql = f"""
SELECT
    hour_start_utc,
    measurement_id,
    target,
    probe_count,
    aggregated_sample_count AS sample_count,
    aggregated_avg_rtt_ms AS avg_rtt_ms,
    aggregated_probe_p95_rtt_ms AS avg_probe_p95_rtt_ms,

    if(
        aggregated_total_packets_sent > 0,
        (
            (
                aggregated_total_packets_sent
                - aggregated_total_packets_received
            )
            * 100.0
            / aggregated_total_packets_sent
        ),
        NULL
    ) AS packet_loss_pct,

    aggregated_total_packets_sent AS total_packets_sent,
    aggregated_total_packets_received AS total_packets_received

FROM
(
    SELECT
        hour_start_utc,
        measurement_id,

        any(target) AS target,

        uniqExact(probe_id)
            AS probe_count,

        sum(sample_count)
            AS aggregated_sample_count,

        avg(avg_rtt_ms)
            AS aggregated_avg_rtt_ms,

        avg(p95_rtt_ms)
            AS aggregated_probe_p95_rtt_ms,

        sum(total_packets_sent)
            AS aggregated_total_packets_sent,

        sum(total_packets_received)
            AS aggregated_total_packets_received

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

    GROUP BY
        hour_start_utc,
        measurement_id
)

ORDER BY hour_start_utc
"""

    try:
        rows = (
            clickhouse.query_json(
                sql
            )
        )

    except ClickHouseError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "message": (
                    "Could not query hourly "
                    "network quality"
                ),
            },
        ) from exc

    return rows