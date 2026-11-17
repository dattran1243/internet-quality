"""Network quality API models."""

from datetime import datetime

from pydantic import BaseModel


class HourlyQualityPoint(BaseModel):
    """Aggregated network quality for one measurement and one hour."""

    hour_start_utc: datetime

    measurement_id: int
    target: str

    probe_count: int
    sample_count: int

    avg_rtt_ms: float | None
    avg_probe_p95_rtt_ms: float | None

    packet_loss_pct: float | None

    total_packets_sent: int
    total_packets_received: int