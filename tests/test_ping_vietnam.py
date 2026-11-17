"""Spark tests for selected Vietnam Ping enrichment."""

from datetime import datetime, timezone
from ingestion.ripe_atlas.models import TimeWindow
from processing.silver.incremental import (
    replace_measurements_in_window,
)
import pytest

from processing.silver.ripe_atlas_ping_vietnam import (
    ENRICHED_COLUMNS,
    enrich_ping_vietnam,
    validate_enriched_ping,
)
from processing.silver.ripe_atlas_probe import probe_dimension_schema

pyspark = pytest.importorskip("pyspark", reason="PySpark is required")

SELECTED_ID = 86206926


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-ping-vietnam")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def ping_schema():
    from pyspark.sql.types import (
        DoubleType, IntegerType, LongType, StringType, StructField,
        StructType, TimestampType,
    )
    return StructType([
        StructField("event_time_utc", TimestampType(), True),
        StructField("measurement_id", LongType(), True),
        StructField("probe_id", LongType(), True),
        StructField("target", StringType(), True),
        StructField("destination_address", StringType(), True),
        StructField("address_family", IntegerType(), True),
        StructField("protocol", StringType(), True),
        StructField("rtt_min_ms", DoubleType(), True),
        StructField("rtt_avg_ms", DoubleType(), True),
        StructField("rtt_max_ms", DoubleType(), True),
        StructField("packets_sent", IntegerType(), True),
        StructField("packets_received", IntegerType(), True),
        StructField("packet_loss_pct", DoubleType(), True),
        StructField("ingested_at_utc", TimestampType(), True),
        StructField("validation_status", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True),
        StructField("hour", StringType(), True),
    ])


def ping(probe_id=1, measurement_id=SELECTED_ID, full_loss=False):
    return {
        "event_time_utc": datetime(2026, 9, 15, 1),
        "measurement_id": measurement_id,
        "probe_id": probe_id,
        "target": "google.com",
        "destination_address": "142.250.1.1",
        "address_family": 4,
        "protocol": "ICMP",
        "rtt_min_ms": None if full_loss else 10.0,
        "rtt_avg_ms": None if full_loss else 11.0,
        "rtt_max_ms": None if full_loss else 12.0,
        "packets_sent": 3,
        "packets_received": 0 if full_loss else 3,
        "packet_loss_pct": 100.0 if full_loss else 0.0,
        "ingested_at_utc": datetime(2026, 9, 15, 1, 5),
        "validation_status": "valid",
        "year": "2026", "month": "09", "day": "15", "hour": "01",
    }


def probe(probe_id=1, country="VN", status="Connected"):
    return {
        "probe_id": probe_id,
        "country_code": country,
        "asn_v4": 7552,
        "asn_v6": None,
        "latitude": 10.8231,
        "longitude": 106.6297,
        "status_id": 1,
        "status_name": status,
        "description": "Vietnam test probe",
        "is_anchor": False,
        "is_public": True,
        "address_v4": "192.0.2.1",
        "address_v6": None,
        "prefix_v4": "192.0.2.0/24",
        "prefix_v6": None,
        "first_connected_utc": datetime(2024, 1, 1),
        "last_connected_utc": datetime(2026, 9, 16),
        "status_since_utc": datetime(2024, 1, 1),
        "snapshot_at_utc": datetime(2026, 9, 16, 2),
        "source": "ripe_atlas",
    }


def test_enrichment_filter_coverage_and_row_invariant(spark):
    ping_df = spark.createDataFrame([
        ping(1, full_loss=True),
        ping(2),
        ping(1, measurement_id=1009),
    ], ping_schema())
    probes_df = spark.createDataFrame([probe(1)], probe_dimension_schema())

    enriched, metrics = enrich_ping_vietnam(
        ping_df, probes_df, [SELECTED_ID]
    )
    validate_enriched_ping(enriched, [SELECTED_ID], metrics.matched_ping_rows)
    row = enriched.first()

    assert enriched.columns == ENRICHED_COLUMNS
    assert metrics.input_ping_rows == 2
    assert metrics.input_distinct_probes == 2
    assert metrics.probe_dimension_rows == 1
    assert metrics.matched_ping_rows == 1
    assert metrics.unmatched_ping_rows == 1
    assert metrics.matched_distinct_probes == 1
    assert metrics.unmatched_distinct_probes == 1
    assert metrics.unmatched_probe_ids == (2,)
    assert metrics.output_rows == metrics.matched_ping_rows == 1
    assert row.measurement_id == SELECTED_ID
    assert row.measurement_id != 1009
    assert row.target == "google.com"
    assert row.asn_v4 == 7552
    assert row.asn_v6 is None
    assert row.latitude == pytest.approx(10.8231)
    assert row.longitude == pytest.approx(106.6297)
    assert row.probe_snapshot_at_utc == datetime(2026, 9, 16, 2)
    assert row.rtt_min_ms is None
    assert row.packet_loss_pct == pytest.approx(100.0)
    assert "BroadcastHashJoin" in enriched._jdf.queryExecution().executedPlan().toString()


def test_duplicate_probe_dimension_fails_before_join(spark):
    ping_df = spark.createDataFrame([ping()], ping_schema())
    probes_df = spark.createDataFrame(
        [probe(), probe()], probe_dimension_schema()
    )

    with pytest.raises(ValueError, match="probe_id must be unique"):
        enrich_ping_vietnam(ping_df, probes_df, [SELECTED_ID])


@pytest.mark.parametrize(
    ("country", "status", "message"),
    [
        ("US", "Connected", "country_code"),
        ("VN", "Disconnected", "status_name"),
    ],
)
def test_non_current_vietnam_dimension_fails(
        spark, country, status, message):
    ping_df = spark.createDataFrame([ping()], ping_schema())
    probes_df = spark.createDataFrame(
        [probe(country=country, status=status)], probe_dimension_schema()
    )

    with pytest.raises(ValueError, match=message):
        enrich_ping_vietnam(ping_df, probes_df, [SELECTED_ID])

def test_incremental_ping_vietnam_write_preserves_partition_history(
        spark, tmp_path):
    from pyspark.sql import functions as f

    other_measurement_id = 176906978

    probes_df = spark.createDataFrame(
        [probe(1)],
        probe_dimension_schema(),
    )

    # New selected row for hour=01.
    incoming_ping = spark.createDataFrame(
        [
            ping(
                probe_id=1,
                measurement_id=SELECTED_ID,
            ),
        ],
        ping_schema(),
    )

    incoming, _ = enrich_ping_vietnam(
        incoming_ping,
        probes_df,
        [SELECTED_ID],
    )

    # Another selected measurement sharing the same hour.
    other_ping = spark.createDataFrame(
        [
            ping(
                probe_id=1,
                measurement_id=other_measurement_id,
            ),
        ],
        ping_schema(),
    )

    other_enriched, _ = enrich_ping_vietnam(
        other_ping,
        probes_df,
        [other_measurement_id],
    )

    # Existing stale version of SELECTED_ID.
    old_selected = (
        incoming
        .withColumn(
            "rtt_avg_ms",
            f.lit(99.0),
        )
    )

    # Existing history in another hour.
    history_hour_02 = (
        incoming
        .withColumn(
            "event_time_utc",
            f.lit(
                datetime(
                    2026,
                    9,
                    15,
                    2,
                )
            ),
        )
        .withColumn(
            "hour",
            f.lit("02"),
        )
        .withColumn(
            "rtt_avg_ms",
            f.lit(50.0),
        )
    )

    target = str(
        tmp_path / "ping_vietnam"
    )

    initial = (
        old_selected
        .unionByName(
            other_enriched
        )
        .unionByName(
            history_hour_02
        )
    )

    (
        initial.write
        .mode("overwrite")
        .partitionBy(
            "year",
            "month",
            "day",
            "hour",
        )
        .parquet(target)
    )

    window = TimeWindow(
        datetime(
            2026,
            9,
            15,
            1,
            tzinfo=timezone.utc,
        ),
        datetime(
            2026,
            9,
            15,
            2,
            tzinfo=timezone.utc,
        ),
    )

    # Run once.
    replace_measurements_in_window(
        incoming,
        target,
        window,
        [SELECTED_ID],
    )

    # Rerun exact same window.
    replace_measurements_in_window(
        incoming,
        target,
        window,
        [SELECTED_ID],
    )

    result = spark.read.parquet(
        target
    )

    # Total remains stable:
    # - selected measurement hour 01
    # - other measurement hour 01
    # - historical hour 02
    assert result.count() == 3

    hour_01 = result.filter(
        "hour = '01'"
    )

    assert hour_01.count() == 2

    # Updated measurement exists once and uses new value.
    selected_rows = (
        hour_01
        .filter(
            f"measurement_id = {SELECTED_ID}"
        )
        .collect()
    )

    assert len(selected_rows) == 1

    assert (
        selected_rows[0].rtt_avg_ms
        == pytest.approx(11.0)
    )

    # Other measurement in SAME partition survives.
    assert (
        hour_01
        .filter(
            f"measurement_id = {other_measurement_id}"
        )
        .count()
        == 1
    )

    # Other hour survives.
    hour_02 = result.filter(
        "hour = '02'"
    )

    assert hour_02.count() == 1

    assert (
        hour_02.first().rtt_avg_ms
        == pytest.approx(50.0)
    )

    # Measurement 1009 must never appear in ping_vietnam.
    assert (
        result
        .filter("measurement_id = 1009")
        .count()
        == 0
    )

    # Canonical partition names remain zero padded.
    assert (
        tmp_path
        / "ping_vietnam"
        / "year=2026"
        / "month=09"
        / "day=15"
        / "hour=01"
    ).exists()

    assert not (
        tmp_path
        / "ping_vietnam"
        / "year=2026"
        / "month=9"
        / "day=15"
        / "hour=1"
    ).exists()