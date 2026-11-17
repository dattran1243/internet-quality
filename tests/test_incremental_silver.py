"""Regression tests for hourly incremental Silver replacement."""

from datetime import datetime, timezone

import pytest

from ingestion.ripe_atlas.models import TimeWindow
from processing.silver.incremental import (
    filter_exact_bronze_window,
    replace_measurements_in_window,
    validate_hour_aligned_window,
)

pyspark = pytest.importorskip(
    "pyspark",
    reason="PySpark is required",
)


SELECTED_ID = 86206926
HISTORICAL_ID = 1009


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-incremental-silver")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield session

    session.stop()


def row(
    measurement_id,
    probe_id,
    hour,
    rtt,
):
    return {
        "event_time_utc": datetime(
            2026, 9, 15, hour, 5
        ),
        "measurement_id": measurement_id,
        "probe_id": probe_id,
        "target": "example",
        "destination_address": "192.0.2.1",
        "address_family": 4,
        "protocol": "ICMP",
        "rtt_min_ms": rtt,
        "rtt_avg_ms": rtt,
        "rtt_max_ms": rtt,
        "packets_sent": 3,
        "packets_received": 3,
        "packet_loss_pct": 0.0,
        "ingested_at_utc": datetime(
            2026, 9, 15, hour, 10
        ),
        "validation_status": "valid",
        "year": "2026",
        "month": "09",
        "day": "15",
        "hour": f"{hour:02d}",
    }


def test_incremental_replace_preserves_history_and_is_idempotent(
    spark,
    tmp_path,
):
    target = str(
        tmp_path / "silver"
    )

    initial = spark.createDataFrame([
        # Historical measurement in hour 00.
        row(
            HISTORICAL_ID,
            1,
            0,
            100.0,
        ),

        # Old VN row that must be replaced.
        row(
            SELECTED_ID,
            2,
            0,
            10.0,
        ),

        # Another historical partition that must not be touched.
        row(
            HISTORICAL_ID,
            3,
            1,
            110.0,
        ),
    ])

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
            0,
            tzinfo=timezone.utc,
        ),
        datetime(
            2026,
            9,
            15,
            1,
            tzinfo=timezone.utc,
        ),
    )

    incoming = spark.createDataFrame([
        row(
            SELECTED_ID,
            2,
            0,
            20.0,
        ),
    ])

    # First run.
    replace_measurements_in_window(
        incoming,
        target,
        window,
        [SELECTED_ID],
    )

    # Rerun same window.
    replace_measurements_in_window(
        incoming,
        target,
        window,
        [SELECTED_ID],
    )

    result = spark.read.parquet(
        target
    )

    # Still exactly 3 rows.
    assert result.count() == 3

    hour0 = result.filter(
        "hour = '00'"
    )

    assert hour0.count() == 2

    # Historical 1009 in same partition survives.
    assert (
        hour0
        .filter(
            f"measurement_id = {HISTORICAL_ID}"
        )
        .count()
        == 1
    )

    # VN measurement was replaced, not duplicated.
    selected = (
        hour0
        .filter(
            f"measurement_id = {SELECTED_ID}"
        )
        .collect()
    )

    assert len(selected) == 1

    assert selected[0].rtt_avg_ms == pytest.approx(
        20.0
    )

    # Unrelated hour=01 survives.
    hour1 = result.filter(
        "hour = '01'"
    )

    assert hour1.count() == 1

    assert (
        hour1.first().measurement_id
        == HISTORICAL_ID
    )

    # Prevent accidental duplicate partition naming:
    # month=09/hour=00 versus month=9/hour=0.
    assert (
        tmp_path
        / "silver/year=2026/month=09/day=15/hour=00"
    ).exists()

    assert not (
        tmp_path
        / "silver/year=2026/month=9/day=15/hour=0"
    ).exists()


def test_hour_alignment_is_required():
    window = TimeWindow(
        datetime(
            2026,
            9,
            15,
            0,
            30,
            tzinfo=timezone.utc,
        ),
        datetime(
            2026,
            9,
            15,
            1,
            30,
            tzinfo=timezone.utc,
        ),
    )

    with pytest.raises(
        ValueError,
        match="aligned",
    ):
        validate_hour_aligned_window(
            window
        )


def test_filter_exact_bronze_window(
    spark,
):
    from pyspark.sql import Row

    window = TimeWindow(
        datetime(
            2026,
            9,
            15,
            0,
            tzinfo=timezone.utc,
        ),
        datetime(
            2026,
            9,
            15,
            1,
            tzinfo=timezone.utc,
        ),
    )

    bronze = spark.createDataFrame([
        Row(
            metadata=Row(
                measurement_id=SELECTED_ID,
                requested_start_utc=(
                    "2026-09-15T00:00:00Z"
                ),
                requested_stop_utc=(
                    "2026-09-15T01:00:00Z"
                ),
            ),
            records=[1],
        ),

        Row(
            metadata=Row(
                measurement_id=SELECTED_ID,
                requested_start_utc=(
                    "2026-09-15T01:00:00Z"
                ),
                requested_stop_utc=(
                    "2026-09-15T02:00:00Z"
                ),
            ),
            records=[2],
        ),
    ])

    result = (
        filter_exact_bronze_window(
            bronze,
            window,
        )
        .collect()
    )

    assert len(result) == 1

    assert (
        result[0]
        .metadata
        .measurement_id
        == SELECTED_ID
    )