CREATE DATABASE IF NOT EXISTS telecom;


CREATE TABLE IF NOT EXISTS telecom.hourly_network_quality
(
    hour_start_utc DateTime64(3, 'UTC'),

    measurement_id UInt64,
    probe_id UInt64,
    target String,

    sample_count UInt32,
    rtt_sample_count UInt32,
    loss_sample_count UInt32,

    avg_rtt_ms Nullable(Float64),
    median_rtt_ms Nullable(Float64),
    p95_rtt_ms Nullable(Float64),
    min_rtt_ms Nullable(Float64),
    max_rtt_ms Nullable(Float64),
    rtt_stddev_ms Nullable(Float64),

    avg_packet_loss_pct Nullable(Float64),
    max_packet_loss_pct Nullable(Float64),

    total_packets_sent UInt64,
    total_packets_received UInt64,

    weighted_packet_loss_pct Nullable(Float64),

    country_code Nullable(String),

    asn_v4 Nullable(UInt64),
    asn_v6 Nullable(UInt64),

    latitude Nullable(Float64),
    longitude Nullable(Float64),

    is_anchor Nullable(Bool),

    source_max_ingested_at_utc Nullable(DateTime64(3, 'UTC')),
    probe_snapshot_at_utc Nullable(DateTime64(3, 'UTC')),

    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(hour_start_utc)
ORDER BY
(
    measurement_id,
    probe_id,
    hour_start_utc,
    target
);


CREATE TABLE IF NOT EXISTS telecom.incident_candidates
(
    hour_start_utc DateTime64(3, 'UTC'),

    measurement_id UInt64,
    target String,

    observed_probe_count UInt64,
    eligible_probe_count UInt64,

    warning_probe_count UInt64,
    critical_probe_count UInt64,
    affected_probe_count UInt64,

    observed_asn_count UInt64,
    eligible_asn_count UInt64,

    warning_asn_count UInt64,
    critical_asn_count UInt64,
    affected_asn_count UInt64,

    max_probe_latency_ratio Nullable(Float64),
    max_probe_latency_delta_ms Nullable(Float64),
    max_probe_packet_loss_pct Nullable(Float64),

    max_asn_affected_ratio Nullable(Float64),

    coverage_status Nullable(String),
    probe_coverage_ratio Nullable(Float64),

    incident_status String,
    incident_candidate Bool,

    severity_score Nullable(Int32),

    signal_types String,
    incident_summary String,

    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(hour_start_utc)
ORDER BY
(
    measurement_id,
    hour_start_utc,
    target
);