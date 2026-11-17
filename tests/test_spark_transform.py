"""Local Spark tests for the real Bronze-to-Silver transformation."""

from datetime import datetime, timezone
import json

import pytest

from processing.silver.ripe_atlas_ping import transform_bronze

pyspark = pytest.importorskip(
    "pyspark",
    reason="PySpark is required for the local transformation test",
)


@pytest.fixture(scope="module")
def spark():
    """Provide one UTC local Spark session for this test module."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-ripe-transform")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_transform_bronze_valid_and_invalid_records(spark, tmp_path):
    """Normalize valid data and retain an invalid row with its quality status."""
    event_time = datetime(2026, 9, 15, 1, 0, tzinfo=timezone.utc)
    payload = {
        "metadata": {
            "source": "ripe_atlas",
            "measurement_id": 1009,
            "ingested_at_utc": "2026-09-15T01:10:00Z",
        },
        "records": [{
            "msm_id": 1009,
            "prb_id": 12345,
            "timestamp": int(event_time.timestamp()),
            "dst_name": "198.41.0.4",
            "dst_addr": "198.41.0.4",
            "af": 4,
            "proto": "ICMP",
            "min": 20.0,
            "avg": 21.0,
            "max": 22.0,
            "sent": 3,
            "rcvd": 2,
            "result": [{"rtt": 20.0}, {"rtt": 22.0}, {"rtt": None}],
        }, {
            "msm_id": 1009,
            "prb_id": 54321,
            "timestamp": int(event_time.timestamp()),
            "dst_name": "a.root-servers.net",
            "dst_addr": "192.0.2.2",
            "af": 4,
            "proto": "ICMP",
            "min": 20.0,
            "avg": -1.0,
            "max": 22.0,
            "sent": 3,
            "rcvd": 2,
            "result": [{"rtt": 20.0}, {"rtt": 22.0}, {"rtt": None}],
        }, {
            "msm_id": 1009,
            "prb_id": 99999,
            "timestamp": int(event_time.timestamp()),
            "dst_name": "198.41.0.4",
            "dst_addr": "198.41.0.4",
            "af": 4,
            "proto": "ICMP",
            "min": -1.0,
            "avg": -1.0,
            "max": -1.0,
            "sent": 3,
            "rcvd": 0,
            "result": [{"x": "*"}, {"x": "*"}, {"x": "*"}],
        }],
    }
    path = tmp_path / "bronze.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    bronze_df = (
        spark.read.option("multiLine", "true").json(str(path))
    )

    rows = {
        row.probe_id: row
        for row in transform_bronze(bronze_df).collect()
    }

    assert len(rows) == 3
    valid = rows[12345]
    assert valid.measurement_id == 1009
    assert valid.probe_id == 12345
    assert valid.target == "198.41.0.4"
    assert valid.destination_address == "198.41.0.4"
    assert valid.address_family == 4
    assert valid.protocol == "ICMP"
    assert valid.rtt_min_ms == pytest.approx(20.0)
    assert valid.rtt_avg_ms == pytest.approx(21.0)
    assert valid.rtt_max_ms == pytest.approx(22.0)
    assert valid.packets_sent == 3
    assert valid.packets_received == 2
    assert valid.packet_loss_pct == pytest.approx(100 / 3)
    assert valid.event_time_utc is not None
    assert valid.validation_status == "valid"
    assert (valid.year, valid.month, valid.day, valid.hour) == (
        "2026", "09", "15", "01"
    )
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

    invalid = rows[54321]
    assert invalid.rtt_avg_ms == pytest.approx(-1.0)
    assert invalid.validation_status == "invalid"

    full_loss = rows[99999]
    assert full_loss.packets_sent == 3
    assert full_loss.packets_received == 0
    assert full_loss.packet_loss_pct == pytest.approx(100.0)
    assert full_loss.rtt_min_ms is None
    assert full_loss.rtt_avg_ms is None
    assert full_loss.rtt_max_ms is None
    assert full_loss.validation_status == "valid"


@pytest.mark.parametrize(
    ("optional_names", "expected"),
    [
        ({"target": "fallback.example"}, "fallback.example"),
        ({}, None),
    ],
)
def test_transform_bronze_optional_target_fields(
        spark, tmp_path, optional_names, expected):
    """Use raw target as fallback, or null when neither name field exists."""
    event_time = datetime(2026, 9, 15, 1, 0, tzinfo=timezone.utc)
    record = {
        "msm_id": 1009,
        "prb_id": 12345,
        "timestamp": int(event_time.timestamp()),
        "dst_addr": "192.0.2.1",
        "af": 4,
        "proto": "ICMP",
        "min": 20.0,
        "avg": 21.0,
        "max": 22.0,
        "sent": 3,
        "rcvd": 3,
        "result": [{"rtt": 20.0}, {"rtt": 21.0}, {"rtt": 22.0}],
        **optional_names,
    }
    payload = {
        "metadata": {"ingested_at_utc": "2026-09-15T01:10:00Z"},
        "records": [record],
    }
    path = tmp_path / "bronze.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    bronze_df = spark.read.option("multiLine", "true").json(str(path))

    row = transform_bronze(bronze_df).first()

    assert row.target == expected


@pytest.mark.parametrize(
    ("counters", "result", "expected_sent", "expected_loss"),
    [
        (
            {"sent": 0, "rcvd": 0},
            [{"error": "connect failed: Network unreachable"}],
            0,
            None,
        ),
        (
            {},
            [{"x": "*"}, {"x": "*"}, {"x": "*"}],
            3,
            100.0,
        ),
    ],
)
def test_transform_bronze_no_successful_response(
        spark, tmp_path, counters, result, expected_sent, expected_loss):
    """Normalize sentinel RTT when counters or packet results show no reply."""
    event_time = datetime(2026, 9, 15, 1, 0, tzinfo=timezone.utc)
    record = {
        "msm_id": 1009,
        "prb_id": 99999,
        "timestamp": int(event_time.timestamp()),
        "dst_name": "198.41.0.4",
        "dst_addr": "198.41.0.4",
        "af": 4,
        "proto": "ICMP",
        "min": -1.0,
        "avg": -1.0,
        "max": -1.0,
        "result": result,
        **counters,
    }
    payload = {
        "metadata": {"ingested_at_utc": "2026-09-15T01:10:00Z"},
        "records": [record],
    }
    path = tmp_path / "bronze.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    bronze_df = spark.read.option("multiLine", "true").json(str(path))

    row = transform_bronze(bronze_df).first()

    assert row.packets_sent == expected_sent
    assert row.packets_received == 0
    if expected_loss is None:
        assert row.packet_loss_pct is None
    else:
        assert row.packet_loss_pct == pytest.approx(expected_loss)
    assert row.rtt_min_ms is None
    assert row.rtt_avg_ms is None
    assert row.rtt_max_ms is None
    assert row.validation_status == "valid"
