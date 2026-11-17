"""Small domain models shared by RIPE Atlas ingestion."""

from dataclasses import dataclass
from datetime import datetime, timezone


def ensure_utc(value: datetime) -> datetime:
    """Return an aware UTC datetime; naïve inputs are rejected."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamp must include a timezone")
    return value.astimezone(timezone.utc)


def parse_utc(value: str) -> datetime:
    """Parse ISO-8601 text and normalize it to UTC."""
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    return ensure_utc(datetime.fromisoformat(normalized))


def utc_text(value: datetime) -> str:
    """Format a datetime as canonical second-resolution UTC text."""
    return ensure_utc(value).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class TimeWindow:
    """A non-empty UTC request interval."""

    start: datetime
    stop: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "start", ensure_utc(self.start))
        object.__setattr__(self, "stop", ensure_utc(self.stop))
        if self.stop <= self.start:
            raise ValueError("stop must be after start")

