"""ASN API models."""

from datetime import datetime

from pydantic import BaseModel


class ASNQualitySummary(BaseModel):
    """Hourly network quality summary for one IPv4 ASN."""

    hour_start_utc: datetime

    measurement_id: int
    target: str

    asn_v4: int

    probe_count: int
    sample_count: int

    avg_rtt_ms: float | None
    avg_probe_p95_rtt_ms: float | None
    packet_loss_pct: float | None

    total_packets_sent: int
    total_packets_received: int

    measurement_incident_status: str | None