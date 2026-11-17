"""VIQ demo-only 24h Big Data probe explorer (read-only, existing Gold rows).

Entrypoint: python -m uvicorn api.demo_bigdata:app --host 127.0.0.1 --port 8000

Avoids ClickHouse advanced/null-sensitive aggregate SQL. Reads bounded rows
and aggregates in Python. The quality data, including geolocations, are not
modified. The original /probes/map and other API routes are unchanged.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import isfinite

from fastapi import HTTPException, Query

from api.main import app
from api.config import settings
from api.clickhouse import clickhouse, ClickHouseError

ALLOWED_DATABASES = {"viq_demo_24h", "telecom_g1"}
ALLOWED_COUNTRIES = {"VN", "DE", "SG", "JP", "US"}
MAX_OBSERVATIONS = 80000


def _utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise HTTPException(status_code=422, detail="Time requires UTC/timezone offset")
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _float(value):
    if value is None:
        return None
    try:
        result = float(value)
        return result if isfinite(result) else None
    except (ValueError, TypeError):
        return None


def _conditions(start: datetime, stop: datetime, country: str, measurement: int | None):
    if start.tzinfo is None or stop.tzinfo is None or start >= stop:
        raise HTTPException(status_code=422, detail="Invalid UTC time window")
    if stop - start > timedelta(hours=72):
        raise HTTPException(status_code=422, detail="Maximum time range is 72 hours")
    if country != "ALL" and country not in ALLOWED_COUNTRIES:
        raise HTTPException(status_code=422, detail="Unsupported country")
    if measurement is not None and not (1 <= measurement <= 10_000_000_000):
        raise HTTPException(status_code=422, detail="Invalid measurement ID")
    if (settings.clickhouse_database not in ALLOWED_DATABASES
        or settings.clickhouse_host not in {"127.0.0.1", "localhost"}
        or settings.clickhouse_http_port != 18123):
        raise HTTPException(status_code=503,
            detail="Big Data Explorer requires local ClickHouse sandbox on 18123")
    cond = [
        f"hour_start_utc >= toDateTime64('{_utc(start)}', 3, 'UTC')",
        f"hour_start_utc < toDateTime64('{_utc(stop)}', 3, 'UTC')",
    ]
    if country == "ALL":
        cond.append("country_code IN ('VN','DE','SG','JP','US')")
    else:
        cond.append(f"country_code = '{country}'")
    if measurement is not None:
        cond.append(f"measurement_id = {int(measurement)}")
    return " AND ".join(cond)


def _aggregate(rows):
    """Pure Python: no fabricated locations, no artificial probe multiplier."""
    countries, measurements, hours = set(), set(), set()
    groups = {}
    total_samples = 0
    mapped_ids = set()

    for row in rows:
        country = str(row.get("country_code") or "")
        if country not in ALLOWED_COUNTRIES:
            continue
        probe = int(row["probe_id"])
        mid = int(row["measurement_id"])
        hour = str(row["hour_start_utc"])
        samples = int(row.get("sample_count") or 0)
        countries.add(country)
        measurements.add(mid)
        hours.add(hour)
        total_samples += samples

        latitude = _float(row.get("latitude"))
        longitude = _float(row.get("longitude"))
        if (latitude is None or longitude is None
            or not -90 <= latitude <= 90 or not -180 <= longitude <= 180):
            continue

        key = (country, probe)
        mapped_ids.add(key)
        group = groups.get(key)
        if group is None:
            group = {
                "country_code": country, "probe_id": probe,
                "latitude": latitude, "longitude": longitude,
                "last_observed_utc": hour,
                "measurement_ids": set(), "observations": 0,
                "raw_samples": 0, "rtt_sum": 0.0, "rtt_n": 0,
                "loss_sum": 0.0, "loss_n": 0,
            }
            groups[key] = group
        elif hour >= group["last_observed_utc"]:
            # Latitude and longitude come from the same source record.
            group["latitude"] = latitude
            group["longitude"] = longitude
            group["last_observed_utc"] = hour

        group["measurement_ids"].add(mid)
        group["observations"] += 1
        group["raw_samples"] += samples
        rtt = _float(row.get("avg_rtt_ms"))
        loss = _float(row.get("avg_packet_loss_pct"))
        if rtt is not None:
            group["rtt_sum"] += rtt
            group["rtt_n"] += 1
        if loss is not None:
            group["loss_sum"] += loss
            group["loss_n"] += 1

    if len(groups) > 10000:
        raise HTTPException(status_code=422,
            detail="More than 10000 mappable probe positions; narrow filters")
    points = []
    for key, group in sorted(groups.items()):
        points.append({
            "country_code": group["country_code"],
            "probe_id": group["probe_id"],
            "latitude": round(group["latitude"], 6),
            "longitude": round(group["longitude"], 6),
            "measurement_count": len(group["measurement_ids"]),
            "observations": group["observations"],
            "raw_samples": group["raw_samples"],
            "average_rtt_ms": round(group["rtt_sum"] / group["rtt_n"], 2)
                if group["rtt_n"] else None,
            "average_packet_loss_pct": round(group["loss_sum"] / group["loss_n"], 3)
                if group["loss_n"] else None,
            "last_observed_utc": group["last_observed_utc"],
        })

    return {
        "summary": {
            "quality_observations": len(rows),
            "raw_measurement_samples": total_samples,
            "countries": len(countries),
            "measurements": len(measurements),
            "hours": len(hours),
            "mappable_unique_probes": len(mapped_ids),
            "returned_locations": len(points),
        },
        "points": points,
    }


@app.get("/bigdata/probes", tags=["demo-bigdata"])
def explore_probes(
    start: datetime = Query(default=datetime(2026, 10, 18, 13, 0, tzinfo=timezone.utc)),
    stop: datetime = Query(default=datetime(2026, 10, 19, 13, 0, tzinfo=timezone.utc)),
    country: str = Query(default="ALL"),
    measurement_id: int | None = Query(default=None),
):
    country = country.upper().strip()
    where = _conditions(start, stop, country, measurement_id)
    # Straightforward, bounded read: avoids Nullable aggregate conditions and
    # old ClickHouse versions' type inference differences.
    sql = f"""
    SELECT hour_start_utc, country_code, measurement_id, probe_id,
        latitude, longitude, sample_count,
        avg_rtt_ms, avg_packet_loss_pct
    FROM hourly_network_quality
    WHERE {where}
    ORDER BY hour_start_utc, country_code, probe_id
    LIMIT {MAX_OBSERVATIONS + 1}
    """
    try:
        rows = clickhouse.query_json(sql)
    except ClickHouseError as exc:
        raise HTTPException(status_code=503,
            detail="Cannot query ClickHouse Gold. Inspect backend log and demo credentials.") from exc

    if len(rows) > MAX_OBSERVATIONS:
        raise HTTPException(status_code=422,
            detail="More than 80000 observations. Narrow the filters; data was not truncated.")
    result = _aggregate(rows)
    return {
        "source": f"{settings.clickhouse_database}.hourly_network_quality",
        "window": {"start": _utc(start), "stop_exclusive": _utc(stop)},
        "filters": {"country": country, "measurement_id": measurement_id},
        **result,
        "semantics": (
            "Unique probes are distinct (country, probe_id) with real coordinates. "
            "Gold observations are hourly probe-measurement records; raw samples "
            "are the sum of sample_count. The selected country is the monitoring "
            "scope, not a guarantee of probe geolocation. Historical data only."
        ),
        "historical_not_live": True,
    }
