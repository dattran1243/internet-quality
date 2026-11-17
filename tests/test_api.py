"""FastAPI contract tests.

These tests mock ClickHouse so they can run without external services.
"""

import pytest
from fastapi.testclient import TestClient

from api.clickhouse import ClickHouseError, clickhouse
from api.main import app


client = TestClient(app)


QUALITY_PATH = (
    "/quality/hourly"
    "?measurement_id=86206926"
    "&country_code=VN"
    "&start=2026-10-07T07:00:00Z"
    "&stop=2026-10-07T08:00:00Z"
)

INCIDENTS_PATH = (
    "/incidents"
    "?start=2026-10-07T07:00:00Z"
    "&stop=2026-10-07T08:00:00Z"
    "&country_code=VN"
    "&only_candidates=false"
    "&limit=1000"
)

MEASUREMENTS_PATH = (
    "/measurements"
    "?country_code=VN"
)

PROBES_PATH = (
    "/probes/map"
    "?measurement_id=86206926"
    "&country_code=VN"
    "&at=2026-10-07T07:00:00Z"
)

ASN_PATH = (
    "/asn/summary"
    "?measurement_id=86206926"
    "&country_code=VN"
    "&start=2026-10-07T07:00:00Z"
    "&stop=2026-10-07T08:00:00Z"
)


BUSINESS_ENDPOINTS = [
    (
        QUALITY_PATH,
        "Could not query hourly network quality",
    ),
    (
        INCIDENTS_PATH,
        "Could not query incident data",
    ),
    (
        MEASUREMENTS_PATH,
        "Could not query measurement data",
    ),
    (
        PROBES_PATH,
        "Could not query probe map data",
    ),
    (
        ASN_PATH,
        "Could not query ASN summary data",
    ),
]


def raise_clickhouse_error(*args, **kwargs):
    raise ClickHouseError(
        "SECRET_INTERNAL_HOST=clickhouse:8123"
    )


def test_health_ok(monkeypatch):
    monkeypatch.setattr(
        clickhouse,
        "ping",
        lambda: True,
    )

    response = client.get(
        "/health"
    )

    assert response.status_code == 200

    assert response.json() == {
        "status": "ok",
        "api": "ok",
        "clickhouse": "ok",
    }


def test_health_unavailable_is_public_safe(
    monkeypatch,
):
    monkeypatch.setattr(
        clickhouse,
        "ping",
        raise_clickhouse_error,
    )

    response = client.get(
        "/health"
    )

    assert response.status_code == 503

    body = response.json()

    assert body == {
        "detail": {
            "status": "degraded",
            "api": "ok",
            "clickhouse": "unavailable",
        }
    }

    assert (
        "SECRET_INTERNAL_HOST"
        not in response.text
    )


def test_health_unexpected_ping_response(
    monkeypatch,
):
    monkeypatch.setattr(
        clickhouse,
        "ping",
        lambda: False,
    )

    response = client.get(
        "/health"
    )

    assert response.status_code == 503

    assert response.json() == {
        "detail": {
            "status": "degraded",
            "api": "ok",
            "clickhouse": "unexpected_response",
        }
    }


@pytest.mark.parametrize(
    "path",
    [
        QUALITY_PATH,
        INCIDENTS_PATH,
        MEASUREMENTS_PATH,
        PROBES_PATH,
        ASN_PATH,
    ],
)
def test_business_endpoints_return_200(
    monkeypatch,
    path,
):
    monkeypatch.setattr(
        clickhouse,
        "query_json",
        lambda sql: [],
    )

    response = client.get(
        path
    )

    assert response.status_code == 200
    assert response.json() == []


def test_invalid_country_returns_422():
    response = client.get(
        "/measurements"
        "?country_code=ZZ"
    )

    assert response.status_code == 422


def test_missing_timezone_returns_422():
    response = client.get(
        "/quality/hourly"
        "?measurement_id=86206926"
        "&country_code=VN"
        "&start=2026-10-07T07:00:00"
        "&stop=2026-10-07T08:00:00Z"
    )

    assert response.status_code == 422


def test_start_must_be_before_stop():
    response = client.get(
        "/quality/hourly"
        "?measurement_id=86206926"
        "&country_code=VN"
        "&start=2026-10-07T08:00:00Z"
        "&stop=2026-10-07T07:00:00Z"
    )

    assert response.status_code == 422


def test_incident_limit_validation():
    response = client.get(
        "/incidents"
        "?start=2026-10-07T07:00:00Z"
        "&stop=2026-10-07T08:00:00Z"
        "&country_code=VN"
        "&limit=1001"
    )

    assert response.status_code == 422


@pytest.mark.parametrize(
    "path,expected_message",
    BUSINESS_ENDPOINTS,
)
def test_clickhouse_failure_returns_safe_503(
    monkeypatch,
    path,
    expected_message,
):
    monkeypatch.setattr(
        clickhouse,
        "query_json",
        raise_clickhouse_error,
    )

    response = client.get(
        path
    )

    assert response.status_code == 503

    assert response.json() == {
        "detail": {
            "message": expected_message,
        }
    }

    # Infrastructure information must never
    # be exposed to API clients.
    assert (
        "SECRET_INTERNAL_HOST"
        not in response.text
    )

    assert "clickhouse:8123" not in response.text
