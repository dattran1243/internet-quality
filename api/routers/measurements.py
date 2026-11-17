"""Measurement endpoints."""

from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query

from api.clickhouse import (
    ClickHouseError,
    clickhouse,
)
from api.models.measurement import (
    MeasurementSummary,
)
from configs.monitoring import (
    DEFAULT_COUNTRY_CODE,
    get_country,
)


router = APIRouter(
    prefix="/measurements",
    tags=["measurements"],
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
    response_model=list[MeasurementSummary],
)
def list_measurements(
    country_code: str = Query(
        DEFAULT_COUNTRY_CODE,
        min_length=2,
        max_length=2,
        description=(
            "Two-letter country scope. "
            f"Default: {DEFAULT_COUNTRY_CODE}"
        ),
    ),
):
    """
    Return measurements available in ClickHouse
    for one country scope.

    Measurement identity remains independent of country.
    country_code only selects the probe/data scope.
    """

    country = normalize_country(
        country_code
    )

    sql = f"""
SELECT
    measurement_id,
    any(target) AS target,

    min(hour_start_utc) AS first_seen_utc,
    max(hour_start_utc) AS last_seen_utc,

    uniqExact(probe_id) AS probe_count,
    count() AS hourly_probe_rows

FROM hourly_network_quality

WHERE
    country_code = '{country}'

GROUP BY
    measurement_id

ORDER BY
    measurement_id
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
                    "Could not query measurement data"
                ),
            },
        ) from exc

    return rows