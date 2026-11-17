"""Probe map API models."""

from datetime import datetime

from pydantic import BaseModel


class ProbeMapPoint(BaseModel):
    """One RIPE Atlas probe point for map visualization."""

    hour_start_utc: datetime

    measurement_id: int
    probe_id: int
    target: str

    country_code: str | None

    asn_v4: int | None
    asn_v6: int | None

    latitude: float
    longitude: float

    is_anchor: bool | None

    sample_count: int

    avg_rtt_ms: float | None
    p95_rtt_ms: float | None
    packet_loss_pct: float | None

    measurement_incident_status: str | None