"""Minimal ClickHouse HTTP client used by the API."""

import json
import logging
from typing import Any

import requests

from api.config import settings


LOGGER = logging.getLogger(__name__)


class ClickHouseError(RuntimeError):
    """Public-safe ClickHouse failure."""


class ClickHouseClient:
    """Small HTTP client for ClickHouse queries."""

    def __init__(self) -> None:
        self.base_url = (
            f"http://{settings.clickhouse_host}:"
            f"{settings.clickhouse_http_port}/"
        )

        # Reuse TCP connections across API requests.
        self.session = requests.Session()

    def execute(
        self,
        sql: str,
    ) -> str:
        """
        Execute SQL and return raw ClickHouse response.

        Internal infrastructure errors are logged server-side.
        Callers only receive public-safe ClickHouseError messages.
        """

        try:
            response = self.session.post(
                self.base_url,
                params={
                    "database": settings.clickhouse_database,
                },
                auth=(
                    settings.clickhouse_user,
                    settings.clickhouse_password,
                ),
                data=sql.encode("utf-8"),
                timeout=settings.clickhouse_timeout_seconds,
            )

        except requests.RequestException:
            LOGGER.exception(
                "Could not connect to ClickHouse"
            )

            raise ClickHouseError(
                "ClickHouse is currently unavailable"
            )

        if not response.ok:
            LOGGER.error(
                "ClickHouse query failed status=%s response=%r",
                response.status_code,
                response.text[:2000],
            )

            raise ClickHouseError(
                "ClickHouse query failed"
            )

        return response.text

    def query_json(
        self,
        sql: str,
    ) -> list[dict[str, Any]]:
        """
        Execute SELECT SQL and return JSONEachRow records.
        """

        query = sql.strip().rstrip(";")

        query = (
            f"{query}\n"
            "FORMAT JSONEachRow"
        )

        raw = self.execute(
            query
        )

        if not raw.strip():
            return []

        try:
            return [
                json.loads(line)
                for line in raw.splitlines()
                if line.strip()
            ]

        except json.JSONDecodeError:
            LOGGER.exception(
                "ClickHouse returned invalid JSONEachRow"
            )

            raise ClickHouseError(
                "Invalid response from ClickHouse"
            )

    def ping(self) -> bool:
        """Return True when ClickHouse responds successfully."""

        result = self.execute(
            "SELECT 1"
        )

        return result.strip() == "1"


clickhouse = ClickHouseClient()
