"""Measurement API models."""

from datetime import datetime

from pydantic import BaseModel


class MeasurementSummary(BaseModel):
    """Summary of one monitored RIPE Atlas measurement."""

    measurement_id: int
    target: str

    first_seen_utc: datetime
    last_seen_utc: datetime

    probe_count: int
    hourly_probe_rows: int