"""Tests for the RIPE Atlas current probe dimension snapshot."""

from datetime import datetime, timezone

import pytest

from processing.silver.ripe_atlas_probe import (
    PROBE_DIMENSION_COLUMNS,
    build_probe_dimension,
    probe_dimension_summary,
    validate_probe_dimension,
)

pyspark = pytest.importorskip("pyspark", reason="PySpark is required")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-probe-dimension")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def probe(probe_id=101, coordinates=None, country="VN", status="Connected"):
    return {
        "id": probe_id,
        "country_code": country,
        "asn_v4": 7552,
        "asn_v6": None,
        "geometry": ({"coordinates": coordinates}
                     if coordinates is not None else None),
        "status": {"id": 1, "name": status},
        "description": "test probe",
        "is_anchor": False,
        "is_public": True,
        "address_v4": "192.0.2.1",
        "address_v6": None,
        "prefix_v4": "192.0.2.0/24",
        "prefix_v6": None,
        "first_connected": 1719564604,
        "last_connected": 1719568204,
        "status_since": 1719561004,
    }


def test_normalizes_geojson_nested_status_and_utc_timestamps(spark):
    snapshot = datetime(2026, 9, 16, 2, 3, 4, tzinfo=timezone.utc)
    df = build_probe_dimension(
        spark, [probe(coordinates=[106.6297, 10.8231])], snapshot
    )
    row = df.first()

    assert row.longitude == pytest.approx(106.6297)
    assert row.latitude == pytest.approx(10.8231)
    assert row.status_id == 1
    assert row.status_name == "Connected"
    formatted = df.selectExpr(
        "date_format(first_connected_utc, 'yyyy-MM-dd HH:mm:ss') AS first",
        "date_format(last_connected_utc, 'yyyy-MM-dd HH:mm:ss') AS last",
        "date_format(status_since_utc, 'yyyy-MM-dd HH:mm:ss') AS since",
    ).first()
    assert formatted.first == "2024-06-28 08:50:04"
    assert formatted.last == "2024-06-28 09:50:04"
    assert formatted.since == "2024-06-28 07:50:04"
    assert row.snapshot_at_utc == snapshot.replace(tzinfo=None)
    assert row.asn_v6 is None
    assert row.source == "ripe_atlas"


def test_missing_geometry_is_retained_as_null(spark):
    df = build_probe_dimension(spark, [probe()])
    row = df.first()

    assert row.latitude is None
    assert row.longitude is None
    validate_probe_dimension(df)


def test_schema_and_quality_summary(spark):
    first = probe(101, [106.0, 10.0])
    second = probe(102)
    second["asn_v4"] = None
    second["is_anchor"] = True
    df = build_probe_dimension(spark, [first, second])

    assert df.columns == PROBE_DIMENSION_COLUMNS
    types = {field.name: field.dataType.simpleString() for field in df.schema}
    assert types == {
        "probe_id": "bigint", "country_code": "string",
        "asn_v4": "bigint", "asn_v6": "bigint", "latitude": "double",
        "longitude": "double", "status_id": "int", "status_name": "string",
        "description": "string", "is_anchor": "boolean",
        "is_public": "boolean", "address_v4": "string",
        "address_v6": "string", "prefix_v4": "string",
        "prefix_v6": "string", "first_connected_utc": "timestamp",
        "last_connected_utc": "timestamp", "status_since_utc": "timestamp",
        "snapshot_at_utc": "timestamp", "source": "string",
    }
    validate_probe_dimension(df)
    assert probe_dimension_summary(df) == {
        "probes": 2, "distinct_probe_ids": 2, "distinct_asn_v4": 1,
        "missing_asn_v4": 1, "missing_asn_v6": 2,
        "missing_coordinates": 1, "anchors": 1,
    }


@pytest.mark.parametrize(
    ("record", "message"),
    [
        (probe(coordinates=[181.0, 10.0]), "longitude"),
        (probe(coordinates=[106.0, 91.0]), "latitude"),
        (probe(country="US"), "country_code"),
        (probe(status="Disconnected"), "status_name"),
    ],
)
def test_validation_rejects_invalid_core_values(spark, record, message):
    with pytest.raises(ValueError, match=message):
        validate_probe_dimension(build_probe_dimension(spark, [record]))

