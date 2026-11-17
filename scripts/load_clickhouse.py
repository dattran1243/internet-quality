#!/usr/bin/env python
"""Incrementally load country-scoped Gold Parquet into ClickHouse."""

from __future__ import annotations

import argparse
from datetime import timedelta
import os
from pathlib import Path
import re
import sys

import requests
from dotenv import load_dotenv


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

sys.path.insert(
    0,
    str(PROJECT_ROOT),
)


from configs.monitoring import (
    DEFAULT_COUNTRY_CODE,
    get_country,
)
from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
)
from processing.silver.incremental import (
    validate_hour_aligned_window,
)


load_dotenv(
    PROJECT_ROOT / ".env"
)


# ============================================================
# CLI
# ============================================================


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load country-scoped Gold RIPE Atlas "
            "Parquet from MinIO into ClickHouse."
        )
    )

    parser.add_argument(
        "--country-code",
        default=DEFAULT_COUNTRY_CODE,
        help=(
            "Configured enabled country scope "
            f"(default: {DEFAULT_COUNTRY_CODE})"
        ),
    )

    parser.add_argument(
        "--start",
        required=True,
        help=(
            "UTC inclusive start, e.g. "
            "2026-09-21T00:00:00Z"
        ),
    )

    parser.add_argument(
        "--stop",
        required=True,
        help=(
            "UTC exclusive stop, e.g. "
            "2026-09-21T01:00:00Z"
        ),
    )

    return parser.parse_args()


# ============================================================
# Environment
# ============================================================


def clickhouse_host() -> str:
    return os.getenv(
        "CLICKHOUSE_HOST",
        "localhost",
    )


def clickhouse_port() -> str:
    return os.getenv(
        "CLICKHOUSE_HTTP_PORT",
        "8123",
    )


def clickhouse_database() -> str:
    database = os.getenv(
        "CLICKHOUSE_DATABASE",
        "telecom",
    )

    if not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*",
        database,
    ):
        raise ValueError(
            "CLICKHOUSE_DATABASE contains "
            "invalid characters"
        )

    return database


def clickhouse_user() -> str:
    return os.getenv(
        "CLICKHOUSE_USER",
        "telecom",
    )


def clickhouse_password() -> str:
    return os.getenv(
        "CLICKHOUSE_PASSWORD",
        "",
    )


def minio_bucket() -> str:
    return os.getenv(
        "MINIO_BUCKET",
        "telecom-data",
    )


def minio_internal_endpoint() -> str:
    return os.getenv(
        "MINIO_INTERNAL_ENDPOINT",
        "http://minio:9000",
    ).rstrip("/")


def minio_credentials() -> tuple[str, str]:
    access_key = (
        os.getenv(
            "MINIO_ACCESS_KEY"
        )
        or os.getenv(
            "MINIO_ROOT_USER"
        )
    )

    secret_key = (
        os.getenv(
            "MINIO_SECRET_KEY"
        )
        or os.getenv(
            "MINIO_ROOT_PASSWORD"
        )
    )

    if not access_key:
        raise ValueError(
            "MINIO_ACCESS_KEY is missing "
            "(MINIO_ROOT_USER fallback also missing)"
        )

    if not secret_key:
        raise ValueError(
            "MINIO_SECRET_KEY is missing "
            "(MINIO_ROOT_PASSWORD fallback also missing)"
        )

    return (
        access_key,
        secret_key,
    )


# ============================================================
# SQL helpers
# ============================================================


def sql_string(
    value: str,
) -> str:
    escaped = value.replace(
        "'",
        "''",
    )

    return f"'{escaped}'"


def redact_sql(
    sql: str,
) -> str:
    """
    Prevent credentials embedded in s3() expressions
    from appearing in exception output.
    """

    values = (
        os.getenv(
            "MINIO_ACCESS_KEY"
        ),
        os.getenv(
            "MINIO_SECRET_KEY"
        ),
        os.getenv(
            "MINIO_ROOT_USER"
        ),
        os.getenv(
            "MINIO_ROOT_PASSWORD"
        ),
    )

    redacted = sql

    for value in values:
        if value:
            redacted = redacted.replace(
                value,
                "***REDACTED***",
            )

    return redacted


# ============================================================
# ClickHouse HTTP client
# ============================================================


def clickhouse_query(
    sql: str,
) -> str:
    host = clickhouse_host()
    port = clickhouse_port()
    database = clickhouse_database()
    user = clickhouse_user()
    password = clickhouse_password()

    url = (
        f"http://{host}:{port}/"
    )

    try:
        response = requests.post(
            url,
            params={
                "database": database,
            },
            auth=(
                user,
                password,
            ),
            data=sql.encode(
                "utf-8"
            ),
            timeout=300,
        )

    except requests.RequestException as exc:
        raise RuntimeError(
            "ClickHouse HTTP request failed: "
            f"{exc}"
        ) from exc

    if not response.ok:
        raise RuntimeError(
            "ClickHouse query failed.\n\n"
            f"HTTP status: {response.status_code}\n"
            f"Response:\n{response.text}\n\n"
            "SQL:\n"
            f"{redact_sql(sql)}"
        )

    return response.text


# ============================================================
# Window helpers
# ============================================================


def hours(
    window: TimeWindow,
):
    cursor = window.start

    while cursor < window.stop:
        yield cursor

        cursor += timedelta(
            hours=1
        )


def sql_window_bounds(
    window: TimeWindow,
) -> tuple[str, str]:
    start = window.start.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    stop = window.stop.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    return (
        start,
        stop,
    )


# ============================================================
# MinIO paths
# ============================================================


def minio_gold_url(
    dataset: str,
    country_code: str,
    hour,
) -> str:
    allowed = {
        "hourly_network_quality",
        "incident_candidates",
    }

    if dataset not in allowed:
        raise ValueError(
            f"Unsupported Gold dataset: {dataset}"
        )

    endpoint = (
        minio_internal_endpoint()
    )

    bucket = minio_bucket()

    return (
        f"{endpoint}/"
        f"{bucket}/"
        f"gold/ripe_atlas/"
        f"{dataset}/"
        f"country={country_code}/"
        f"year={hour:%Y}/"
        f"month={hour:%m}/"
        f"day={hour:%d}/"
        f"hour={hour:%H}/"
        "*.parquet"
    )


def s3_function(
    url: str,
) -> str:
    access_key, secret_key = (
        minio_credentials()
    )

    return (
        "s3("
        f"{sql_string(url)}, "
        f"{sql_string(access_key)}, "
        f"{sql_string(secret_key)}, "
        "'Parquet'"
        ")"
    )


# ============================================================
# Preflight
# ============================================================


def require_country_column(
    table: str,
) -> None:
    database = clickhouse_database()

    sql = f"""
SELECT count()
FROM system.columns
WHERE
    database = {sql_string(database)}
    AND table = {sql_string(table)}
    AND name = 'country_code'
"""

    value = clickhouse_query(
        sql
    ).strip()

    if value != "1":
        raise RuntimeError(
            f"{database}.{table} does not have "
            "country_code. Run ClickHouse migration first."
        )


def source_row_count(
    dataset: str,
    country_code: str,
    hour,
) -> int:
    url = minio_gold_url(
        dataset,
        country_code,
        hour,
    )

    source = s3_function(
        url
    )

    sql = f"""
SELECT count()
FROM {source}
"""

    text = (
        clickhouse_query(
            sql
        )
        .strip()
    )

    return int(text)


def validate_sources(
    window: TimeWindow,
    country_code: str,
) -> tuple[int, int]:
    """
    Validate every source hour before DELETE.

    This avoids deleting a ClickHouse window and only then
    discovering that a country-scoped Gold partition is missing.
    """

    quality_rows = 0
    incident_rows = 0

    print(
        "Validating country-scoped Gold sources..."
    )

    for hour in hours(
        window
    ):
        text = hour.strftime(
            "%Y-%m-%d %H:%M UTC"
        )

        quality_count = source_row_count(
            "hourly_network_quality",
            country_code,
            hour,
        )

        incident_count = source_row_count(
            "incident_candidates",
            country_code,
            hour,
        )

        if quality_count <= 0:
            raise RuntimeError(
                "hourly_network_quality source is empty "
                f"for country={country_code} hour={text}"
            )

        if incident_count <= 0:
            raise RuntimeError(
                "incident_candidates source is empty "
                f"for country={country_code} hour={text}"
            )

        print(
            f"  {text}: "
            f"quality={quality_count} "
            f"incidents={incident_count}"
        )

        quality_rows += quality_count
        incident_rows += incident_count

    return (
        quality_rows,
        incident_rows,
    )


# ============================================================
# Idempotent country-scoped replacement
# ============================================================


def clear_window(
    table: str,
    window: TimeWindow,
    country_code: str,
) -> None:
    allowed_tables = {
        "hourly_network_quality",
        "incident_candidates",
    }

    if table not in allowed_tables:
        raise ValueError(
            f"Unsupported ClickHouse table: {table}"
        )

    database = (
        clickhouse_database()
    )

    start, stop = (
        sql_window_bounds(
            window
        )
    )

    sql = f"""
ALTER TABLE {database}.{table}
DELETE WHERE
    country_code = {sql_string(country_code)}
    AND hour_start_utc >= toDateTime64(
        {sql_string(start)},
        3,
        'UTC'
    )
    AND hour_start_utc < toDateTime64(
        {sql_string(stop)},
        3,
        'UTC'
    )
SETTINGS mutations_sync = 1
"""

    clickhouse_query(
        sql
    )


# ============================================================
# Load hourly network quality
# ============================================================


def load_hourly_network_quality(
    country_code: str,
    hour,
) -> None:
    database = (
        clickhouse_database()
    )

    url = minio_gold_url(
        "hourly_network_quality",
        country_code,
        hour,
    )

    source = s3_function(
        url
    )

    sql = f"""
INSERT INTO {database}.hourly_network_quality
(
    hour_start_utc,
    measurement_id,
    probe_id,
    target,

    sample_count,
    rtt_sample_count,
    loss_sample_count,

    avg_rtt_ms,
    median_rtt_ms,
    p95_rtt_ms,
    min_rtt_ms,
    max_rtt_ms,
    rtt_stddev_ms,

    avg_packet_loss_pct,
    max_packet_loss_pct,

    total_packets_sent,
    total_packets_received,

    weighted_packet_loss_pct,

    country_code,
    asn_v4,
    asn_v6,
    latitude,
    longitude,
    is_anchor,

    source_max_ingested_at_utc,
    probe_snapshot_at_utc
)
SELECT
    hour_start_utc,
    measurement_id,
    probe_id,
    target,

    sample_count,
    rtt_sample_count,
    loss_sample_count,

    avg_rtt_ms,
    median_rtt_ms,
    p95_rtt_ms,
    min_rtt_ms,
    max_rtt_ms,
    rtt_stddev_ms,

    avg_packet_loss_pct,
    max_packet_loss_pct,

    total_packets_sent,
    total_packets_received,

    weighted_packet_loss_pct,

    country_code,
    asn_v4,
    asn_v6,
    latitude,
    longitude,
    is_anchor,

    source_max_ingested_at_utc,
    probe_snapshot_at_utc

FROM {source}
"""

    clickhouse_query(
        sql
    )


# ============================================================
# Load incident candidates
# ============================================================


def load_incident_candidates(
    country_code: str,
    hour,
) -> None:
    database = (
        clickhouse_database()
    )

    url = minio_gold_url(
        "incident_candidates",
        country_code,
        hour,
    )

    source = s3_function(
        url
    )

    sql = f"""
INSERT INTO {database}.incident_candidates
(
    hour_start_utc,
    country_code,

    measurement_id,
    target,

    observed_probe_count,
    eligible_probe_count,

    warning_probe_count,
    critical_probe_count,
    affected_probe_count,

    observed_asn_count,
    eligible_asn_count,

    warning_asn_count,
    critical_asn_count,
    affected_asn_count,

    max_probe_latency_ratio,
    max_probe_latency_delta_ms,
    max_probe_packet_loss_pct,

    max_asn_affected_ratio,

    coverage_status,
    probe_coverage_ratio,

    incident_status,
    incident_candidate,
    severity_score,

    signal_types,
    incident_summary
)
SELECT
    hour_start_utc,
    country_code,

    measurement_id,
    target,

    observed_probe_count,
    eligible_probe_count,

    warning_probe_count,
    critical_probe_count,
    affected_probe_count,

    observed_asn_count,
    eligible_asn_count,

    warning_asn_count,
    critical_asn_count,
    affected_asn_count,

    max_probe_latency_ratio,
    max_probe_latency_delta_ms,
    max_probe_packet_loss_pct,

    max_asn_affected_ratio,

    coverage_status,
    probe_coverage_ratio,

    incident_status,
    incident_candidate,
    severity_score,

    signal_types,
    incident_summary

FROM {source}
"""

    clickhouse_query(
        sql
    )


# ============================================================
# Read-back validation
# ============================================================


def loaded_row_count(
    table: str,
    window: TimeWindow,
    country_code: str,
) -> int:
    allowed_tables = {
        "hourly_network_quality",
        "incident_candidates",
    }

    if table not in allowed_tables:
        raise ValueError(
            f"Unsupported ClickHouse table: {table}"
        )

    database = (
        clickhouse_database()
    )

    start, stop = (
        sql_window_bounds(
            window
        )
    )

    sql = f"""
SELECT count()
FROM {database}.{table}
WHERE
    country_code = {sql_string(country_code)}
    AND hour_start_utc >= toDateTime64(
        {sql_string(start)},
        3,
        'UTC'
    )
    AND hour_start_utc < toDateTime64(
        {sql_string(stop)},
        3,
        'UTC'
    )
"""

    return int(
        clickhouse_query(
            sql
        ).strip()
    )


# ============================================================
# Main
# ============================================================


def main() -> None:
    args = arguments()

    country = get_country(
        args.country_code
    )

    if not country.enabled:
        raise ValueError(
            f"Country {country.code} "
            "is configured but disabled"
        )

    window = TimeWindow(
        parse_utc(
            args.start
        ),
        parse_utc(
            args.stop
        ),
    )

    validate_hour_aligned_window(
        window
    )

    # Validate credentials before any mutation.
    minio_credentials()

    # Validate ClickHouse migration before any DELETE.
    require_country_column(
        "hourly_network_quality"
    )

    require_country_column(
        "incident_candidates"
    )

    expected_quality_rows, expected_incident_rows = (
        validate_sources(
            window,
            country.code,
        )
    )

    print(
        "\nClearing existing country-scoped "
        "ClickHouse window..."
    )

    clear_window(
        "hourly_network_quality",
        window,
        country.code,
    )

    clear_window(
        "incident_candidates",
        window,
        country.code,
    )

    for hour in hours(
        window
    ):
        text = hour.strftime(
            "%Y-%m-%d %H:%M UTC"
        )

        print(
            f"\nLoading country={country.code} {text}"
        )

        print(
            "  hourly_network_quality..."
        )

        load_hourly_network_quality(
            country.code,
            hour,
        )

        print(
            "  incident_candidates..."
        )

        load_incident_candidates(
            country.code,
            hour,
        )

    actual_quality_rows = (
        loaded_row_count(
            "hourly_network_quality",
            window,
            country.code,
        )
    )

    actual_incident_rows = (
        loaded_row_count(
            "incident_candidates",
            window,
            country.code,
        )
    )

    print(
        "\n=== CLICKHOUSE READ-BACK ==="
    )

    print(
        "country="
        f"{country.code}"
    )

    print(
        "hourly_network_quality "
        f"expected={expected_quality_rows} "
        f"actual={actual_quality_rows}"
    )

    print(
        "incident_candidates "
        f"expected={expected_incident_rows} "
        f"actual={actual_incident_rows}"
    )

    if (
        actual_quality_rows
        != expected_quality_rows
    ):
        raise RuntimeError(
            "hourly_network_quality read-back "
            "row count mismatch"
        )

    if (
        actual_incident_rows
        != expected_incident_rows
    ):
        raise RuntimeError(
            "incident_candidates read-back "
            "row count mismatch"
        )

    print(
        "\nClickHouse country-scoped load complete."
    )


if __name__ == "__main__":
    main()