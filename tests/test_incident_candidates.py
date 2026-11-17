from datetime import datetime, timezone

import pytest

pyspark = pytest.importorskip(
    "pyspark"
)

from pyspark.sql import SparkSession

from processing.gold.ripe_atlas_incident_candidates import (
    build_incident_candidates,
    validate_incident_candidates,
)


MID = 86206926

TS = datetime(
    2026,
    9,
    20,
    12,
    tzinfo=timezone.utc,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName(
            "test-incident-candidates"
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .getOrCreate()
    )

    yield session

    session.stop()


def probe_rows(
    spark,
    statuses,
):
    rows = []

    for index, status in enumerate(
        statuses,
        start=1,
    ):
        rows.append(
            {
                "hour_start_utc": TS,
                "country_code": "VN",
                "measurement_id": MID,
                "probe_id": index,
                "target": "google.com",
                "asn_v4": 45899,
                "status": status,
                "latency_ratio": (
                    2.5
                    if status
                    in (
                        "warning",
                        "critical",
                    )
                    else 1.0
                ),
                "latency_delta_ms": (
                    30.0
                    if status
                    in (
                        "warning",
                        "critical",
                    )
                    else 0.0
                ),
                "avg_packet_loss_pct": 0.0,
            }
        )

    return spark.createDataFrame(
        rows
    )


def asn_rows(
    spark,
    status="normal",
):
    return spark.createDataFrame(
        [
            {
                "hour_start_utc": TS,
                "country_code": "VN",
                "measurement_id": MID,
                "target": "google.com",
                "asn_v4": 45899,
                "asn_signal_status": status,
                "affected_ratio": (
                    0.7
                    if status
                    in (
                        "warning",
                        "critical",
                    )
                    else 0.0
                ),
            }
        ]
    )


def coverage_rows(
    spark,
    status="normal",
):
    return spark.createDataFrame(
        [
            {
                "hour_start_utc": TS,
                "country_code": "VN",
                "measurement_id": MID,
                "target": "google.com",
                "source_hour_present": True,
                "observed_probe_count": 10,
                "baseline_probe_count_median": 10.0,
                "probe_coverage_ratio": (
                    0.4
                    if status == "critical"
                    else (
                        0.7
                        if status == "warning"
                        else 1.0
                    )
                ),
                "probe_drop_count": (
                    6.0
                    if status == "critical"
                    else (
                        3.0
                        if status == "warning"
                        else 0.0
                    )
                ),
                "probe_drop_pct": (
                    60.0
                    if status == "critical"
                    else (
                        30.0
                        if status == "warning"
                        else 0.0
                    )
                ),
                "coverage_status": status,
            }
        ]
    )


def test_critical_asn_creates_critical_incident(
    spark,
):
    result = build_incident_candidates(
        probe_rows(
            spark,
            [
                "critical",
                "critical",
                "normal",
            ],
        ),
        asn_rows(
            spark,
            "critical",
        ),
        coverage_rows(
            spark,
            "normal",
        ),
    ).first()

    assert (
        result.incident_status
        == "critical"
    )

    assert (
        result.incident_candidate
        is True
    )


def test_critical_coverage_creates_incident(
    spark,
):
    result = build_incident_candidates(
        probe_rows(
            spark,
            [
                "normal",
                "normal",
            ],
        ),
        asn_rows(
            spark,
            "normal",
        ),
        coverage_rows(
            spark,
            "critical",
        ),
    ).first()

    assert (
        result.incident_status
        == "critical"
    )


def test_probe_only_anomaly_is_observe(
    spark,
):
    result = build_incident_candidates(
        probe_rows(
            spark,
            [
                "critical",
                "normal",
                "normal",
            ],
        ),
        asn_rows(
            spark,
            "normal",
        ),
        coverage_rows(
            spark,
            "normal",
        ),
    ).first()

    assert (
        result.incident_status
        == "observe"
    )

    assert (
        result.incident_candidate
        is False
    )


def test_normal_signals_remain_normal(
    spark,
):
    result = build_incident_candidates(
        probe_rows(
            spark,
            [
                "normal",
                "normal",
                "normal",
            ],
        ),
        asn_rows(
            spark,
            "normal",
        ),
        coverage_rows(
            spark,
            "normal",
        ),
    ).first()

    assert (
        result.incident_status
        == "normal"
    )


def test_warning_asn_creates_warning(
    spark,
):
    result = build_incident_candidates(
        probe_rows(
            spark,
            [
                "warning",
                "warning",
                "normal",
            ],
        ),
        asn_rows(
            spark,
            "warning",
        ),
        coverage_rows(
            spark,
            "normal",
        ),
    ).first()

    assert (
        result.incident_status
        == "warning"
    )

    assert (
        result.affected_asn_count
        == 1
    )


def test_validation(
    spark,
):
    result = build_incident_candidates(
        probe_rows(
            spark,
            [
                "normal",
                "warning",
                "normal",
            ],
        ),
        asn_rows(
            spark,
            "warning",
        ),
        coverage_rows(
            spark,
            "normal",
        ),
    )

    validate_incident_candidates(
        result
    )