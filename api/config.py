"""Application configuration."""

import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(
    PROJECT_ROOT / ".env"
)


class Settings:
    """Runtime settings loaded from environment variables."""

    app_name: str = "Vietnam Internet Quality API"
    app_version: str = "0.1.0"

    clickhouse_host: str = os.getenv(
        "CLICKHOUSE_HOST",
        "localhost",
    )

    clickhouse_http_port: int = int(
        os.getenv(
            "CLICKHOUSE_HTTP_PORT",
            "8123",
        )
    )

    clickhouse_database: str = os.getenv(
        "CLICKHOUSE_DATABASE",
        "telecom",
    )

    clickhouse_user: str = os.getenv(
        "CLICKHOUSE_USER",
        "telecom",
    )

    clickhouse_password: str = os.getenv(
        "CLICKHOUSE_PASSWORD",
        "",
    )

    clickhouse_timeout_seconds: int = int(
        os.getenv(
            "CLICKHOUSE_TIMEOUT_SECONDS",
            "30",
        )
    )


settings = Settings()