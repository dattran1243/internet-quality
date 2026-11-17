"""Data comparisons independent of the production transforms."""
from __future__ import annotations

from datetime import datetime, timezone
import math

from processing.e2e.contracts import DATE_COLUMNS, GOLD_COLUMNS, GOLD_KEY, iso, START, row_hash
from processing.e2e.guards import require


def rows_of(frame):
    require(frame.count() <= 1000, "BOUNDED_REPLAY_ROW_LIMIT")
    return [r.asDict(recursive=True) for r in frame.collect()]


def ch_rows(rows):
    output = []
    for row in rows:
        require(set(GOLD_COLUMNS) <= set(row), "GOLD_COLUMNS_MISSING")
        r = {key: row[key] for key in GOLD_COLUMNS}
        for key in DATE_COLUMNS:
            if r[key] is None:
                continue
            v = r[key]
            if not isinstance(v, datetime):
                v = datetime.fromisoformat(v.replace("Z", "+00:00"))
            v = v.replace(tzinfo=v.tzinfo or timezone.utc).astimezone(timezone.utc)
            r[key] = v.strftime("%Y-%m-%d %H:%M:%S.") + f"{v.microsecond//1000:03d}"
        output.append(r)
    return output


def compare_rows(expected, actual, keys=GOLD_KEY):
    def indexed(rows):
        result = {}
        for row in rows:
            key = tuple(row[k] for k in keys)
            require(key not in result, "DUPLICATE_ANALYTICAL_KEY")
            result[key] = row
        return result
    left, right = indexed(expected), indexed(actual)
    require(left.keys() == right.keys(), "ROW_KEYS_MISMATCH")
    for key, row in left.items():
        other = right[key]
        require(row.keys() == other.keys(), "ROW_SCHEMA_MISMATCH")
        for column, value in row.items():
            candidate = other[column]
            if value is None or candidate is None:
                require(value is candidate, "NULL_MISMATCH")
            elif isinstance(value, float):
                require(isinstance(candidate, (float, int)) and math.isfinite(value) and
                        math.isfinite(candidate) and abs(value-candidate) <= 1e-9 + 1e-9*abs(value), "FLOAT_MISMATCH")
            else:
                require(value == candidate, "EXACT_VALUE_MISMATCH")


def expected_quality(rows):
    def mean(name):
        values = [r[name] for r in rows if r[name] is not None]
        return sum(values)/len(values) if values else None
    sent = sum(r["total_packets_sent"] for r in rows)
    received = sum(r["total_packets_received"] for r in rows)
    return [{"hour_start_utc": datetime.fromtimestamp(START, timezone.utc).isoformat(),
             "measurement_id": 177401244, "target": "fastly.com",
             "probe_count": len({r["probe_id"] for r in rows}),
             "sample_count": sum(r["sample_count"] for r in rows),
             "avg_rtt_ms": mean("avg_rtt_ms"), "avg_probe_p95_rtt_ms": mean("p95_rtt_ms"),
             "packet_loss_pct": (sent-received)*100.0/sent if sent else None,
             "total_packets_sent": sent, "total_packets_received": received}]
