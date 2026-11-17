"""Health endpoints."""

from fastapi import APIRouter
from fastapi import HTTPException

from api.clickhouse import (
    ClickHouseError,
    clickhouse,
)


router = APIRouter(
    tags=["health"],
)


@router.get(
    "/health",
)
def health():
    """
    Check API and ClickHouse connectivity.
    """

    try:
        clickhouse_ok = (
            clickhouse.ping()
        )

    except ClickHouseError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "degraded",
                "api": "ok",
                "clickhouse": "unavailable",
            },
        ) from exc

    if not clickhouse_ok:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "degraded",
                "api": "ok",
                "clickhouse": "unexpected_response",
            },
        )

    return {
        "status": "ok",
        "api": "ok",
        "clickhouse": "ok",
    }