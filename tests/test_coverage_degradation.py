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

from processing.gold.ripe_atlas_coverage_degradation import (
    build_coverage_degradation,
    validate_coverage_output,
)


GOOGLE = 86206926
CLOUDFLARE = 176906978


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName(
            "test-coverage-degradation"
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .getOrCreate()
    )

    yield session

    session.stop()


START = datetime(
    2026,
    9,
    1,
    tzinfo=timezone.utc,
)


def rows_for_hour(
    hour_offset,
    probe_count,
    *,
    measurement_id=GOOGLE,
):
    ts = START + timedelta(
        hours=hour_offset
    )

    target = (
        "google.com"
        if measurement_id == GOOGLE
        else "cloudflare.com"
    )

    return [
        {
            "event_time_utc": ts,
            "country_code": "VN",
            "measurement_id": measurement_id,
            "probe_id": probe_id,
            "target": target,
        }
        for probe_id
        in range(
            1,
            probe_count + 1,
        )
    ]


def test_insufficient_history(
    spark,
):
    rows = []

    for hour in range(3):
        rows.extend(
            rows_for_hour(
                hour,
                10,
            )
        )

    df = spark.createDataFrame(
        rows
    )

    result = (
        build_coverage_degradation(
            df,
            [GOOGLE],
            country_code="VN",
            series_start=START,
            series_stop=(
                START
                + timedelta(hours=3)
            ),
            lookback_hours=24,
            min_history_hours=4,
        )
        .orderBy(
            "hour_start_utc"
        )
        .collect()
    )

    assert (
        result[-1].coverage_status
        == "insufficient_history"
    )


def test_warning_when_probe_count_drops(
    spark,
):
    rows = []

    for hour in range(4):
        rows.extend(
            rows_for_hour(
                hour,
                10,
            )
        )

    rows.extend(
        rows_for_hour(
            4,
            7,
        )
    )

    df = spark.createDataFrame(
        rows
    )

    current = (
        build_coverage_degradation(
            df,
            [GOOGLE],
            country_code="VN",
            series_start=START,
            series_stop=(
                START
                + timedelta(hours=5)
            ),
            lookback_hours=24,
            min_history_hours=4,
            warning_ratio=0.80,
            critical_ratio=0.50,
            min_probe_drop=2,
        )
        .filter(
            "hour = '04'"
        )
        .first()
    )

    assert (
        current.baseline_probe_count_median
        == pytest.approx(10.0)
    )

    assert (
        current.observed_probe_count
        == 7
    )

    assert (
        current.probe_coverage_ratio
        == pytest.approx(0.7)
    )

    assert (
        current.coverage_status
        == "warning"
    )


def test_critical_when_probe_count_collapses(
    spark,
):
    rows = []

    for hour in range(4):
        rows.extend(
            rows_for_hour(
                hour,
                10,
            )
        )

    rows.extend(
        rows_for_hour(
            4,
            4,
        )
    )

    df = spark.createDataFrame(
        rows
    )

    current = (
        build_coverage_degradation(
            df,
            [GOOGLE],
            country_code="VN",
            series_start=START,
            series_stop=(
                START
                + timedelta(hours=5)
            ),
            lookback_hours=24,
            min_history_hours=4,
        )
        .filter(
            "hour = '04'"
        )
        .first()
    )

    assert (
        current.probe_coverage_ratio
        == pytest.approx(0.4)
    )

    assert (
        current.coverage_status
        == "critical"
    )


def test_current_hour_not_used_in_baseline(
    spark,
):
    rows = []

    for hour in range(4):
        rows.extend(
            rows_for_hour(
                hour,
                10,
            )
        )

    rows.extend(
        rows_for_hour(
            4,
            1,
        )
    )

    df = spark.createDataFrame(
        rows
    )

    current = (
        build_coverage_degradation(
            df,
            [GOOGLE],
            country_code="VN",
            series_start=START,
            series_stop=(
                START
                + timedelta(hours=5)
            ),
            lookback_hours=24,
            min_history_hours=4,
        )
        .filter(
            "hour = '04'"
        )
        .first()
    )

    assert (
        current.baseline_probe_count_median
        == pytest.approx(10.0)
    )


def test_missing_measurement_hour_is_detected(
    spark,
):
    rows = []

    for hour in range(4):
        rows.extend(
            rows_for_hour(
                hour,
                10,
            )
        )

    # There are intentionally ZERO Google rows for hour 04.

    df = spark.createDataFrame(
        rows
    )

    current = (
        build_coverage_degradation(
            df,
            [GOOGLE],
            country_code="VN",
            series_start=START,
            series_stop=(
                START
                + timedelta(hours=5)
            ),
            lookback_hours=24,
            min_history_hours=4,
        )
        .filter(
            "hour = '04'"
        )
        .first()
    )

    assert (
        current.source_hour_present
        is False
    )

    assert (
        current.observed_probe_count
        == 0
    )

    assert (
        current.baseline_probe_count_median
        == pytest.approx(10.0)
    )

    assert (
        current.coverage_status
        == "critical"
    )


def test_measurements_have_separate_baselines(
    spark,
):
    rows = []

    for hour in range(5):
        rows.extend(
            rows_for_hour(
                hour,
                10,
                measurement_id=GOOGLE,
            )
        )

        rows.extend(
            rows_for_hour(
                hour,
                4,
                measurement_id=CLOUDFLARE,
            )
        )

    df = spark.createDataFrame(
        rows
    )

    result = (
        build_coverage_degradation(
            df,
            [
                GOOGLE,
                CLOUDFLARE,
            ],
            country_code="VN",
            series_start=START,
            series_stop=(
                START
                + timedelta(hours=5)
            ),
            lookback_hours=24,
            min_history_hours=4,
        )
        .filter(
            "hour = '04'"
        )
        .orderBy(
            "measurement_id"
        )
        .collect()
    )

    by_measurement = {
        row.measurement_id: row
        for row in result
    }

    assert (
        by_measurement[
            GOOGLE
        ].baseline_probe_count_median
        == pytest.approx(10.0)
    )

    assert (
        by_measurement[
            CLOUDFLARE
        ].baseline_probe_count_median
        == pytest.approx(4.0)
    )


def test_validate_output(
    spark,
):
    rows = []

    for hour in range(5):
        rows.extend(
            rows_for_hour(
                hour,
                10,
            )
        )

    df = spark.createDataFrame(
        rows
    )

    result = build_coverage_degradation(
        df,
        [GOOGLE],
            country_code="VN",
        series_start=START,
        series_stop=(
            START
            + timedelta(hours=5)
        ),
        lookback_hours=24,
        min_history_hours=4,
    )

    validate_coverage_output(
        result,
        [GOOGLE],
    )