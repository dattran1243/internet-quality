-- Render only in Gate B. Execute only after explicit Gate C resource approval.
CREATE DATABASE {{database}};
CREATE TABLE {{database}}.hourly_network_quality_rows
(
    hour_start_utc DateTime64(3, 'UTC'),
    measurement_id UInt64, probe_id UInt64, target String,
    sample_count UInt32, rtt_sample_count UInt32, loss_sample_count UInt32,
    avg_rtt_ms Nullable(Float64), median_rtt_ms Nullable(Float64),
    p95_rtt_ms Nullable(Float64), min_rtt_ms Nullable(Float64), max_rtt_ms Nullable(Float64),
    rtt_stddev_ms Nullable(Float64), avg_packet_loss_pct Nullable(Float64),
    max_packet_loss_pct Nullable(Float64), total_packets_sent UInt64,
    total_packets_received UInt64, weighted_packet_loss_pct Nullable(Float64),
    country_code String, asn_v4 Nullable(UInt64), asn_v6 Nullable(UInt64),
    latitude Nullable(Float64), longitude Nullable(Float64), is_anchor Nullable(Bool),
    source_max_ingested_at_utc Nullable(DateTime64(3, 'UTC')),
    probe_snapshot_at_utc Nullable(DateTime64(3, 'UTC')),
    e2e_run_id String, raw_sha256 String, gold_manifest_sha256 String,
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(hour_start_utc)
ORDER BY (e2e_run_id, country_code, measurement_id, probe_id, hour_start_utc, target);
CREATE VIEW {{database}}.hourly_network_quality AS
SELECT hour_start_utc, measurement_id, probe_id, target, sample_count,
    rtt_sample_count, loss_sample_count, avg_rtt_ms, median_rtt_ms, p95_rtt_ms,
    min_rtt_ms, max_rtt_ms, rtt_stddev_ms, avg_packet_loss_pct, max_packet_loss_pct,
    total_packets_sent, total_packets_received, weighted_packet_loss_pct, country_code,
    asn_v4, asn_v6, latitude, longitude, is_anchor, source_max_ingested_at_utc, probe_snapshot_at_utc
FROM {{database}}.hourly_network_quality_rows WHERE e2e_run_id = '{{run_id}}';
CREATE TABLE {{database}}.run_receipts
(
    e2e_run_id String, rid String, raw_sha256 String, gold_manifest_sha256 String,
    schema_sha256 String, row_count UInt64, sample_count UInt64, status String,
    verified_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree ORDER BY (e2e_run_id);
