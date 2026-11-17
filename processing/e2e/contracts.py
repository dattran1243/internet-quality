"""Fixed scope and portable receipt encoding; no runtime dependencies."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from hashlib import sha256
import json
import math

DAG_ID = "viq_e2e_isolated_once"
RAW_SHA256 = "a574d80beebe7375c7553ce4a8722afae87e9b0c4b0291ef6912c02da932ff7b"
ARCHIVE_SHA256 = "f1190878f89d9fff3a3a930d1187dbd717faca168c40d478c3e7ccb75374be3b"
START, STOP = 1792328400, 1792332000
MID, COUNTRY, TARGET = 177401244, "DE", "fastly.com"
PROBES = (80, 81, 137, 159, 164, 206, 233, 277, 325, 382, 407, 560,
          706, 882, 901, 910, 916, 933, 968, 998, 1206, 1212, 1000315, 1015560)
WINDOW_COUNTS = (26, 26, 47, 25, 26, 32, 43, 25, 25, 26, 49, 25)
STAGES = (
    "guard_run_and_inputs", "verify_empty_run_resources", "pin_raw_and_scope",
    "publish_raw_kafka", "consume_kafka_bronze", "verify_bronze",
    "silver_12_windows", "build_real_probe_dimension", "enrich_silver",
    "gold_full_hour", "load_clickhouse_insert_only", "verify_clickhouse_rows",
    "verify_fastapi_same_run", "final_acceptance",
)
TIMEOUTS = (120, 120, 120, 180, 180, 120, 600, 180, 300, 300, 180, 180, 180, 120)
GOLD_COLUMNS = (
    "hour_start_utc", "measurement_id", "probe_id", "target", "sample_count",
    "rtt_sample_count", "loss_sample_count", "avg_rtt_ms", "median_rtt_ms",
    "p95_rtt_ms", "min_rtt_ms", "max_rtt_ms", "rtt_stddev_ms",
    "avg_packet_loss_pct", "max_packet_loss_pct", "total_packets_sent",
    "total_packets_received", "weighted_packet_loss_pct", "country_code",
    "asn_v4", "asn_v6", "latitude", "longitude", "is_anchor",
    "source_max_ingested_at_utc", "probe_snapshot_at_utc",
)
GOLD_KEY = ("hour_start_utc", "country_code", "measurement_id", "probe_id", "target")
DATE_COLUMNS = ("hour_start_utc", "source_max_ingested_at_utc", "probe_snapshot_at_utc")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def iso(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace("+00:00", "Z")


def normalize(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("NON_FINITE_NUMBER")
    return value


def canonical(value):
    return json.dumps(normalize(value), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(value):
    return sha256(canonical(value)).hexdigest()


def multiset(rows):
    return Counter(canonical(r) for r in rows)


def row_hash(rows):
    return digest(sorted(canonical(r).decode("utf-8") for r in rows))


def check_raw(rows):
    if len(rows) != 375 or {r.get("msm_id") for r in rows} != {MID}:
        raise ValueError("RAW_SCOPE_MISMATCH")
    if {r.get("prb_id") for r in rows} != set(PROBES):
        raise ValueError("RAW_PROBE_MISMATCH")
    if any(not START <= r.get("timestamp", 0) < STOP or r.get("af") != 4 for r in rows):
        raise ValueError("RAW_TIME_OR_AF_MISMATCH")
    windows = [[r for r in rows if START + i*300 <= r["timestamp"] < START + (i+1)*300]
               for i in range(12)]
    if tuple(map(len, windows)) != WINDOW_COUNTS or len(multiset(rows)) != 375:
        raise ValueError("RAW_WINDOW_OR_DUPLICATE_MISMATCH")
    return windows
