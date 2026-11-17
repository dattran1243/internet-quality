from datetime import (
    datetime,
    timedelta,
    timezone,
)

import pytest

pyspark = pytest.importorskip(
    "pyspark"
)

from pyspark.sql import SparkSession

from processing.gold.ripe_atlas_anomaly import (
    build_hourly_anomalies,
    validate_anomaly_output,
)


MEASUREMENT_ID = 86206926


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName(
            "test-hourly-anomaly"
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .getOrCreate()
    )

    yield session

    session.stop()


def gold_row(
    hour_offset,
    *,
    rtt=20.0,
    loss=0.0,
    probe_id=1,
):
    start = datetime(
        2026,
        9,
        1,
        tzinfo=timezone.utc,
    )

    ts = start + timedelta(
        hours=hour_offset
    )

    return {
        "hour_start_utc": ts,
        "measurement_id": (
            MEASUREMENT_ID
        ),
        "probe_id": probe_id,
        "target": "google.com",
        "sample_count": 15,
        "rtt_sample_count": 15,
        "loss_sample_count": 15,
        "avg_rtt_ms": rtt,
        "median_rtt_ms": rtt,
        "p95_rtt_ms": rtt,
        "min_rtt_ms": rtt,
        "max_rtt_ms": rtt,
        "rtt_stddev_ms": 1.0,
        "avg_packet_loss_pct": loss,
        "max_packet_loss_pct": loss,
        "total_packets_sent": 45,
        "total_packets_received": (
            45
            if loss == 0.0
            else 30
        ),
        "weighted_packet_loss_pct": (
            loss
        ),
        "country_code": "VN",
        "asn_v4": 45899,
        "asn_v6": 64501,
        "latitude": 10.8,
        "longitude": 106.7,
        "is_anchor": False,
        "source_max_ingested_at_utc": ts,
        "probe_snapshot_at_utc": ts,
        "year": ts.strftime("%Y"),
        "month": ts.strftime("%m"),
        "day": ts.strftime("%d"),
        "hour": ts.strftime("%H"),
    }


def test_insufficient_history(
    spark,
):
    df = spark.createDataFrame(
        [
            gold_row(0),
            gold_row(1),
            gold_row(2),
        ]
    )

    result = (
        build_hourly_anomalies(
            df,
            lookback_hours=24,
            min_history_hours=4,
        )
        .orderBy(
            "hour_start_utc"
        )
        .collect()
    )

    assert (
        result[0].status
        == "insufficient_history"
    )

    assert (
        result[-1].status
        == "insufficient_history"
    )


def test_latency_critical(
    spark,
):
    rows = [
        gold_row(
            i,
            rtt=20.0,
        )
        for i in range(6)
    ]

    rows.append(
        gold_row(
            6,
            rtt=50.0,
        )
    )

    df = spark.createDataFrame(
        rows
    )

    result = (
        build_hourly_anomalies(
            df,
            lookback_hours=24,
            min_history_hours=4,
        )
        .filter(
            "hour = '06'"
        )
        .first()
    )

    assert (
        result.baseline_history_count
        == 6
    )

    assert (
        result.baseline_rtt_median_ms
        == pytest.approx(20.0)
    )

    assert (
        result.latency_ratio
        == pytest.approx(2.5)
    )

    assert (
        result.status
        == "critical"
    )

    assert (
        result.latency_anomaly
        is True
    )


def test_packet_loss_critical(
    spark,
):
    rows = [
        gold_row(
            i,
            loss=0.0,
        )
        for i in range(6)
    ]

    rows.append(
        gold_row(
            6,
            loss=30.0,
        )
    )

    df = spark.createDataFrame(
        rows
    )

    result = (
        build_hourly_anomalies(
            df,
            lookback_hours=24,
            min_history_hours=4,
        )
        .filter(
            "hour = '06'"
        )
        .first()
    )

    assert (
        result.baseline_loss_mean_pct
        == pytest.approx(0.0)
    )

    assert (
        result.loss_delta_pct_points
        == pytest.approx(30.0)
    )

    assert (
        result.packet_loss_anomaly
        is True
    )

    assert (
        result.status
        == "critical"
    )


def test_current_hour_not_used_in_baseline(
    spark,
):
    rows = [
        gold_row(
            i,
            rtt=20.0,
        )
        for i in range(4)
    ]

    rows.append(
        gold_row(
            4,
            rtt=1000.0,
        )
    )

    df = spark.createDataFrame(
        rows
    )

    current = (
        build_hourly_anomalies(
            df,
            lookback_hours=24,
            min_history_hours=4,
        )
        .filter(
            "hour = '04'"
        )
        .first()
    )

    assert (
        current.baseline_rtt_mean_ms
        == pytest.approx(20.0)
    )

    assert (
        current.baseline_rtt_median_ms
        == pytest.approx(20.0)
    )


def test_probe_baselines_are_separate(
    spark,
):
    rows = []

    for i in range(5):
        rows.append(
            gold_row(
                i,
                probe_id=1,
                rtt=20.0,
            )
        )

        rows.append(
            gold_row(
                i,
                probe_id=2,
                rtt=80.0,
            )
        )

    df = spark.createDataFrame(
        rows
    )

    result = (
        build_hourly_anomalies(
            df,
            lookback_hours=24,
            min_history_hours=4,
        )
        .filter(
            "hour = '04'"
        )
        .orderBy(
            "probe_id"
        )
        .collect()
    )

    assert (
        result[0]
        .baseline_rtt_median_ms
        == pytest.approx(20.0)
    )

    assert (
        result[1]
        .baseline_rtt_median_ms
        == pytest.approx(80.0)
    )


def test_validate_anomaly_output(
    spark,
):
    rows = [
        gold_row(
            i,
            rtt=20.0,
        )
        for i in range(5)
    ]

    df = spark.createDataFrame(
        rows
    )

    result = build_hourly_anomalies(
        df,
        lookback_hours=24,
        min_history_hours=4,
    )

    validate_anomaly_output(
        result,
        [MEASUREMENT_ID],
    )