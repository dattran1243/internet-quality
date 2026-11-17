# Final historical E2E — public summary

This is a public-safe derivative, not the original runtime execution report.
Source: VIQ_E2E_FRESH_RECOVERY_FINAL_REPORT.md, updated 13 November 2026.
Original document SHA256:
3904a874355de5171bfaab868efcccea22855d2055f04567cda4f5ada54c3e79

The original report and raw receipts remain with the project owner. This export
does not include private paths, credentials, approvals, container identities or
runtime evidence. Its statements describe historical acceptance, not a new run
performed during export. Public source changes have not been requalified by E2E.

| Scope | Recorded result |
|---|---|
| Input | DE, measurement 177401244, 18 October 2026 13:00–14:00 UTC |
| Execution | 13 November 2026; one actual Airflow DagRun |
| Airflow | 14/14 tasks SUCCESS, first try |
| Kafka | 12 messages; DLQ 0 |
| Bronze | 375 records |
| Silver | 375 valid; 0 invalid |
| Dimension | 24 distinct probes |
| Enrichment | 375 matched records |
| Gold | 24 rows; sum(sample_count) = 375 |
| ClickHouse | 24 rows |
| FastAPI | Same-run verification PASS |
| Independent acceptance | 53/53 checks PASS |
| Official Scoped Stop | PASS |

The 14 tasks were guard_run_and_inputs, verify_empty_run_resources,
pin_raw_and_scope, publish_raw_kafka, consume_kafka_bronze, verify_bronze,
silver_12_windows, build_real_probe_dimension, enrich_silver, gold_full_hour,
load_clickhouse_insert_only, verify_clickhouse_rows, verify_fastapi_same_run,
and final_acceptance.

375 input records becoming 24 hourly rows is aggregation, with sample counts
preserved. The 12 Kafka messages are batches, not 12 input records.

This acceptance does not establish frontend same-run acceptance, production
realtime ingestion, throughput, SLA or high availability. The corpus and demo
datasets in the [technical summary](../reports/VIQ_FULL_MENTOR_TECHNICAL_REPORT.md)
have different scopes and must not be combined with this run.

Historical run/resume identities are consumed and are not distributed as
executable configuration. A fresh replay requires new resources, pins and owner
authorization; see the [public preparation guide](VIQ_E2E_RUNBOOK.md).
