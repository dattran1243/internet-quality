from datetime import datetime, timezone

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from processing.gold.ripe_atlas_hourly import (
    build_hourly_network_quality,
    validate_gold_input,
    validate_hourly_gold,
    write_hourly_gold,
)


GOOGLE = 86206926
CLOUDFLARE = 176906978

SELECTED_IDS = (
    GOOGLE,
    CLOUDFLARE,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-gold-hourly")
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.sql.sources.partitionColumnTypeInference.enabled",
            "false",
        )
        .getOrCreate()
    )

    yield session

    session.stop()


def row(
    minute,
    *,
    measurement_id=GOOGLE,
    probe_id=1,
    rtt=10.0,
    loss=0.0,
    received=3,
    hour=0,
):
    return {
        "event_time_utc": datetime(
            2026,
            9,
            21,
            hour,
            minute,
            tzinfo=timezone.utc,
        ),
        "measurement_id": measurement_id,
        "probe_id": probe_id,
        "target": (
            "google.com"
            if measurement_id == GOOGLE
            else "cloudflare.com"
        ),
        "rtt_avg_ms": rtt,
        "packets_sent": 3,
        "packets_received": received,
        "packet_loss_pct": loss,
        "country_code": "VN",
        "asn_v4": 45899,
        "asn_v6": 64501,
        "latitude": 10.8,
        "longitude": 106.7,
        "is_anchor": False,
        "ingested_at_utc": datetime(
            2026,
            9,
            21,
            2,
            tzinfo=timezone.utc,
        ),
        "probe_snapshot_at_utc": datetime(
            2026,
            9,
            21,
            3,
            tzinfo=timezone.utc,
        ),
        "validation_status": "valid",
    }


def test_hourly_aggregation_metrics(spark):
    df = spark.createDataFrame(
        [
            row(0, rtt=10.0),
            row(4, rtt=20.0),
            row(8, rtt=30.0),
            row(
                12,
                rtt=None,
                loss=100.0,
                received=0,
            ),
        ]
    )

    validate_gold_input(
        df,
        [GOOGLE],
    )

    result = build_hourly_network_quality(
        df
    )

    validate_hourly_gold(
        result,
        [GOOGLE],
    )

    record = result.first()

    assert record.sample_count == 4
    assert record.rtt_sample_count == 3

    assert record.avg_rtt_ms == pytest.approx(
        20.0
    )

    assert record.median_rtt_ms == pytest.approx(
        20.0
    )

    assert record.p95_rtt_ms == pytest.approx(
        30.0
    )

    assert (
        record.avg_packet_loss_pct
        == pytest.approx(25.0)
    )

    assert (
        record.max_packet_loss_pct
        == pytest.approx(100.0)
    )

    assert record.total_packets_sent == 12
    assert record.total_packets_received == 9

    assert (
        record.weighted_packet_loss_pct
        == pytest.approx(25.0)
    )


def test_gold_keeps_probe_and_measurement_grain(
        spark):
    df = spark.createDataFrame(
        [
            row(0, probe_id=1),
            row(4, probe_id=1),
            row(0, probe_id=2),
            row(
                0,
                probe_id=1,
                measurement_id=CLOUDFLARE,
            ),
        ]
    )

    result = build_hourly_network_quality(
        df
    )

    assert result.count() == 3


def test_gold_rejects_measurement_1009(spark):
    df = spark.createDataFrame(
        [
            row(
                0,
                measurement_id=1009,
            )
        ]
    )

    with pytest.raises(
        ValueError,
        match="outside the selected product scope",
    ):
        validate_gold_input(
            df,
            [GOOGLE],
        )


def test_gold_incremental_write_is_idempotent(
        spark,
        tmp_path):
    hour_00 = spark.createDataFrame(
        [
            row(0, hour=0),
            row(4, hour=0),
        ]
    )

    hour_01 = spark.createDataFrame(
        [
            row(0, hour=1),
            row(4, hour=1),
        ]
    )

    gold_00 = build_hourly_network_quality(
        hour_00
    )

    gold_01 = build_hourly_network_quality(
        hour_01
    )

    target = str(
        tmp_path
        / "hourly_network_quality"
    )

    write_hourly_gold(
        gold_00,
        target,
    )

    write_hourly_gold(
        gold_01,
        target,
    )

    # Exact rerun of hour 01.
    write_hourly_gold(
        gold_01,
        target,
    )

    result = spark.read.parquet(
        target
    )

    assert result.count() == 2

    assert (
        result
        .filter("hour = '00'")
        .count()
        == 1
    )

    assert (
        result
        .filter("hour = '01'")
        .count()
        == 1
    )

    assert (
        tmp_path
        / "hourly_network_quality"
        / "year=2026"
        / "month=09"
        / "day=21"
        / "hour=00"
    ).exists()

    assert (
        tmp_path
        / "hourly_network_quality"
        / "year=2026"
        / "month=09"
        / "day=21"
        / "hour=01"
    ).exists()
