# VIQ-Full architecture

The current architecture is shown in the [README](../README.md#system-architecture).
RIPE Atlas → Python ingestion → Kafka → MinIO Bronze → Spark Silver →
probe metadata enrichment → Spark Gold → ClickHouse → FastAPI → frontend.

Airflow orchestrates processing. PostgreSQL stores Airflow metadata.
MinIO retains data-lake layers; ClickHouse serves analytical queries.
Historical replay acceptance stops at FastAPI; frontend and live production
ingestion require separate validation. See [public validation summary](e2e/VIQ_E2E_FRESH_RECOVERY_FINAL_REPORT.md).
