from datetime import datetime, timezone

import pytest

pyspark = pytest.importorskip(
    "pyspark"
)

from pyspark.sql import SparkSession

from processing.gold.ripe_atlas_asn_degradation import (
    build_asn_degradation,
    validate_asn_degradation,
)


MEASUREMENT_ID = 86206926


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName(
            "test-asn-degradation"
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .getOrCreate()
    )

    yield session

    session.stop()


def anomaly_row(
    probe_id,
    *,
    asn=45899,
    status="normal",
    latency_ratio=1.0,
    latency_delta=0.0,
    loss=0.0,
    loss_delta=0.0,
):
    return {
        "hour_start_utc": datetime(
            2026,
            9,
            20,
            12,
            tzinfo=timezone.utc,
        ),
        "country_code": "VN",
        "measurement_id": MEASUREMENT_ID,
        "target": "google.com",
        "probe_id": probe_id,
        "asn_v4": asn,
        "status": status,
        "latency_ratio": latency_ratio,
        "latency_delta_ms": latency_delta,
        "avg_packet_loss_pct": loss,
        "loss_delta_pct_points": loss_delta,
    }


def test_single_probe_does_not_create_network_signal(
    spark,
):
    df = spark.createDataFrame(
        [
            anomaly_row(
                1,
                status="critical",
                latency_ratio=3.0,
                latency_delta=50.0,
            )
        ]
    )

    result = build_asn_degradation(
        df,
        min_eligible_probes=3,
    ).first()

    assert (
        result.asn_signal_status
        == "insufficient_coverage"
    )


def test_multiple_affected_probes_create_warning(
    spark,
):
    df = spark.createDataFrame(
        [
            anomaly_row(
                1,
                status="warning",
                latency_ratio=1.8,
            ),
            anomaly_row(
                2,
                status="warning",
                latency_ratio=1.7,
            ),
            anomaly_row(
                3,
                status="normal",
            ),
            anomaly_row(
                4,
                status="normal",
            ),
        ]
    )

    result = build_asn_degradation(
        df,
        min_eligible_probes=3,
        min_affected_probes=2,
        warning_ratio=0.30,
        critical_ratio=0.50,
    ).first()

    assert (
        result.eligible_probe_count
        == 4
    )

    assert (
        result.affected_probe_count
        == 2
    )

    assert (
        result.affected_ratio
        == pytest.approx(0.5)
    )

    assert (
        result.asn_signal_status
        == "warning"
    )


def test_multiple_critical_probes_create_critical(
    spark,
):
    df = spark.createDataFrame(
        [
            anomaly_row(
                1,
                status="critical",
                latency_ratio=2.5,
            ),
            anomaly_row(
                2,
                status="critical",
                latency_ratio=2.2,
            ),
            anomaly_row(
                3,
                status="normal",
            ),
        ]
    )

    result = build_asn_degradation(
        df,
        min_eligible_probes=3,
        min_affected_probes=2,
        warning_ratio=0.30,
        critical_ratio=0.50,
    ).first()

    assert (
        result.critical_probe_count
        == 2
    )

    assert (
        result.affected_ratio
        == pytest.approx(
            2 / 3
        )
    )

    assert (
        result.asn_signal_status
        == "critical"
    )


def test_insufficient_history_not_in_denominator(
    spark,
):
    df = spark.createDataFrame(
        [
            anomaly_row(
                1,
                status="warning",
            ),
            anomaly_row(
                2,
                status="warning",
            ),
            anomaly_row(
                3,
                status="normal",
            ),
            anomaly_row(
                4,
                status="insufficient_history",
            ),
            anomaly_row(
                5,
                status="insufficient_history",
            ),
        ]
    )

    result = build_asn_degradation(
        df,
        min_eligible_probes=3,
        min_affected_probes=2,
    ).first()

    assert (
        result.observed_probe_count
        == 5
    )

    assert (
        result.eligible_probe_count
        == 3
    )

    assert (
        result.insufficient_history_probe_count
        == 2
    )

    assert (
        result.affected_ratio
        == pytest.approx(
            2 / 3
        )
    )


def test_null_asn_is_excluded(
    spark,
):
    schema = """
        hour_start_utc timestamp,
        country_code string,
        measurement_id long,
        target string,
        probe_id long,
        asn_v4 long,
        status string,
        latency_ratio double,
        latency_delta_ms double,
        avg_packet_loss_pct double,
        loss_delta_pct_points double
    """

    rows = [
        (
            datetime(
                2026,
                9,
                20,
                12,
                tzinfo=timezone.utc,
            ),
            "VN",
            MEASUREMENT_ID,
            "google.com",
            1,
            None,
            "critical",
            3.0,
            50.0,
            0.0,
            0.0,
        ),
        (
            datetime(
                2026,
                9,
                20,
                12,
                tzinfo=timezone.utc,
            ),
            "VN",
            MEASUREMENT_ID,
            "google.com",
            2,
            45899,
            "normal",
            1.0,
            0.0,
            0.0,
            0.0,
        ),
    ]

    df = spark.createDataFrame(
        rows,
        schema=schema,
    )

    result = build_asn_degradation(
        df,
        min_eligible_probes=1,
    )

    assert result.count() == 1

    assert result.first().asn_v4 == 45899


def test_validate_asn_output(
    spark,
):
    df = spark.createDataFrame(
        [
            anomaly_row(1),
            anomaly_row(2),
            anomaly_row(3),
        ]
    )

    result = build_asn_degradation(
        df
    )

    validate_asn_degradation(
        result,
        [MEASUREMENT_ID],
    )