# VIQ-Full — public technical summary

This public-safe summary replaces the internal mentor technical report.
Original document SHA256:
b72e5cc9b27b72ab128f153b290fdb43393c330011aecb4691c7e3a7408e88e6
It summarizes recorded results; private evidence is retained by the owner and
is not independently reproducible from the public export alone.

## Architecture and processing

The [README](../../README.md) describes the implemented architecture.
Kafka transports batches; MinIO stores raw and Parquet layers; Spark cleans,
enriches and aggregates; Airflow orchestrates; ClickHouse and FastAPI serve data.
PostgreSQL holds Airflow metadata.

Gold grain: hour_start_utc, country_code, measurement_id, probe_id, target.
sample_count preserves the number of input results represented by an aggregate.
RTT/loss sample counts distinguish missing metrics from zero-valued metrics.
Probe metadata is a snapshot; unmatched records and missing coordinates are
limitations of historical analysis.

## Three separate scopes

| Scope | Window UTC | Recorded size |
|---|---|---|
| Historical corpus | 17 Oct 2026 13:00 → 19 Oct 2026 13:00 | 439,170 Silver; 29,479 Gold rows |
| Demo 24h, VN/DE/SG/JP/US | 18 Oct 2026 13:00 → 19 Oct 2026 13:00 | 118 mappable locations; 219,298 raw samples; 14,731 Gold observations |
| Final replay, DE | 18 Oct 2026 13:00 → 14:00 | 375 records; 24 probes; 24 Gold rows |

118 refers to locations in the demo, not RIPE Atlas network size.
Corpus receipts/Parquet do not prove continuous realtime ingestion.
Demo aggregate statistics do not bind an arbitrary running database to exact
row versions. Final E2E acceptance is documented separately in the
[validation summary](../e2e/VIQ_E2E_FRESH_RECOVERY_FINAL_REPORT.md).

## Deployment boundaries

This export includes source, schemas, example configuration and selected report
figures. It does not include the historical inputs, Docker volumes, secrets or
runtime attestations. Public Compose defaults use new lab project names and
managed volumes; they do not import the owner's Docker data.
See [deployment prerequisites](../DEPLOYMENT.md).

Future work: reproducible deployment/data packaging, frontend acceptance,
monitoring/alerting, distributed performance benchmarks and production hardening.
