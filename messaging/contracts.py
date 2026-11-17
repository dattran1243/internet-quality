"""Message contracts used by the Kafka ingestion layer."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any


SCHEMA_VERSION = 1


def build_window_id(
    *,
    source: str,
    dataset: str,
    country_code: str,
    measurement_id: str,
    window_start_utc: str,
    window_stop_utc: str,
) -> str:
    """Build deterministic ID for one logical ingestion window."""

    raw_key = "|".join(
        [
            source,
            dataset,
            country_code.upper(),
            str(measurement_id),
            window_start_utc,
            window_stop_utc,
        ]
    )

    return sha256(
        raw_key.encode("utf-8")
    ).hexdigest()


def build_event_id(
    *,
    window_id: str,
    chunk_index: int,
) -> str:
    """Build deterministic ID for one Kafka chunk."""

    raw_key = (
        f"{window_id}|{chunk_index}"
    )

    return sha256(
        raw_key.encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class RipeAtlasPingEvent:
    """One chunk of one RIPE Atlas ping window."""

    schema_version: int

    event_id: str
    window_id: str

    source: str
    dataset: str

    country_code: str
    measurement_id: str

    window_start_utc: str
    window_stop_utc: str
    fetched_at_utc: str

    chunk_index: int
    chunk_count: int

    payload: dict[str, Any]

    @property
    def kafka_key(self) -> str:
        """Keep one country/measurement stream ordered."""

        return "|".join(
            [
                self.source,
                self.dataset,
                self.country_code.upper(),
                self.measurement_id,
            ]
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)