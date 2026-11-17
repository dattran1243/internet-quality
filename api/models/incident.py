"""Incident API models."""

from datetime import datetime

from pydantic import BaseModel


class IncidentRecord(BaseModel):
    """One measurement-level network incident observation."""

    hour_start_utc: datetime

    measurement_id: int
    target: str | None

    observed_probe_count: int
    eligible_probe_count: int

    warning_probe_count: int
    critical_probe_count: int
    affected_probe_count: int

    observed_asn_count: int
    eligible_asn_count: int

    warning_asn_count: int
    critical_asn_count: int
    affected_asn_count: int

    max_probe_latency_ratio: float | None
    max_probe_latency_delta_ms: float | None
    max_probe_packet_loss_pct: float | None

    max_asn_affected_ratio: float | None

    coverage_status: str | None
    probe_coverage_ratio: float | None

    incident_status: str
    incident_candidate: bool

    severity_score: int | None

    signal_types: str
    incident_summary: str