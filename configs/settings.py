"""Environment-based application settings."""

from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_VN_PING_MEASUREMENT_IDS = (
    86206926,
    176906978,
    177421171,
    177401244,
    177401210,
)


def _integer(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _number(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric") from exc


@dataclass(frozen=True)
class Settings:
    """Runtime configuration loaded from environment variables."""

    ripe_atlas_base_url: str = os.getenv(
        "RIPE_ATLAS_BASE_URL", "https://atlas.ripe.net/api/v2"
    ).rstrip("/")
    default_measurement_id: int = _integer("RIPE_DEFAULT_MEASUREMENT_ID", 1009)
    request_timeout: int = _integer("RIPE_REQUEST_TIMEOUT", 30)
    max_retries: int = _integer("RIPE_MAX_RETRIES", 3)
    backoff_seconds: float = _number("RIPE_BACKOFF_SECONDS", 1.0)
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "")
    minio_bucket: str = os.getenv("MINIO_BUCKET", "telecom-data")
    spark_master: str = os.getenv("SPARK_MASTER", "local[*]")
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()

    def require_storage_credentials(self) -> None:
        """Fail early when required credentials are absent."""
        if not self.minio_access_key or not self.minio_secret_key:
            raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required")


settings = Settings()
