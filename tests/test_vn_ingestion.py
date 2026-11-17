from datetime import datetime, timezone
from unittest.mock import Mock, call

import pytest

from ingestion.ripe_atlas.client import RipeAtlasClient
from ingestion.ripe_atlas.ingest import (
    bronze_object_key,
    build_envelope,
    coverage_metrics,
    ingest_filtered_measurements,
)
from ingestion.ripe_atlas.models import TimeWindow


@pytest.fixture
def window():
    return TimeWindow(
        datetime(2026, 9, 15, tzinfo=timezone.utc),
        datetime(2026, 9, 15, 1, tzinfo=timezone.utc),
    )


def response(payload):
    mocked = Mock(status_code=200)
    mocked.raise_for_status.return_value = None
    mocked.json.return_value = payload
    return mocked


def test_probe_api_pagination_deduplicates_and_sorts_ids():
    session = Mock()
    next_url = "https://atlas.ripe.net/api/v2/probes/?page=2"
    session.get.side_effect = [
        response({"results": [{"id": 7}, {"id": 3}], "next": next_url}),
        response({"results": [{"id": 7}, {"id": 5}], "next": None}),
    ]
    client = RipeAtlasClient(
        "https://atlas.ripe.net/api/v2", session=session
    )

    assert client.get_connected_probe_ids("vn") == [3, 5, 7]
    first = session.get.call_args_list[0]
    assert first.kwargs["params"]["country_code"] == "VN"
    assert first.kwargs["params"]["status_name"] == "Connected"
    assert first.kwargs["params"]["page_size"] == 500
    assert session.get.call_args_list[1].args[0] == next_url
    assert session.get.call_args_list[1].kwargs["params"] is None


def test_probe_metadata_exact_duplicate_is_deduplicated():
    session = Mock()
    probe = {"id": 7, "country_code": "VN", "asn_v4": 7552}
    session.get.return_value = response({
        "results": [probe, dict(probe)], "next": None,
    })
    client = RipeAtlasClient(
        "https://atlas.ripe.net/api/v2", session=session
    )

    assert client.get_connected_probes("vn") == [probe]


def test_probe_metadata_conflicting_duplicate_fails():
    session = Mock()
    session.get.return_value = response({
        "results": [
            {"id": 7, "country_code": "VN", "asn_v4": 7552},
            {"id": 7, "country_code": "VN", "asn_v4": 45899},
        ],
        "next": None,
    })
    client = RipeAtlasClient(
        "https://atlas.ripe.net/api/v2", session=session
    )

    with pytest.raises(ValueError, match="Conflicting metadata.*7"):
        client.get_connected_probes("VN")


def test_results_request_contains_probe_filter(window):
    session = Mock()
    session.get.return_value = response([])
    client = RipeAtlasClient(
        "https://atlas.ripe.net/api/v2", session=session
    )

    client.fetch_results(86206926, window, probe_ids=[9, 2, 9])

    assert session.get.call_args.kwargs["params"]["probe_ids"] == "2,9"


def test_multi_measurement_reuses_probes_and_stores_separate_objects(window):
    client = Mock()
    client.get_connected_probe_ids.return_value = [1, 2, 3]
    client.fetch_results.side_effect = [
        [{"prb_id": 1}, {"prb_id": 2}, {"prb_id": 2}],
        [],
    ]
    client.results_url.side_effect = lambda measurement_id: (
        f"https://atlas.ripe.net/api/v2/measurements/{measurement_id}/results/"
    )
    s3 = Mock()
    s3.exceptions.ClientError = Exception
    s3.head_bucket.return_value = {}

    summaries = ingest_filtered_measurements(
        client, s3, "telecom-data", [86206926, 176906978], window
    )

    client.get_connected_probe_ids.assert_called_once_with("VN")
    assert client.fetch_results.call_args_list == [
        call(86206926, window, probe_ids=[1, 2, 3]),
        call(176906978, window, probe_ids=[1, 2, 3]),
    ]
    assert s3.upload_fileobj.call_count == 2
    assert summaries[0]["object_key"] != summaries[1]["object_key"]
    assert summaries[1]["record_count"] == 0
    assert summaries[1]["returned_unique_probe_count"] == 0


def test_coverage_counts_distinct_returned_probes():
    metrics = coverage_metrics(
        [1, 2, 3],
        [{"prb_id": 1}, {"prb_id": 1}, {"prb_id": 2}],
    )

    assert metrics["requested_probe_count"] == 3
    assert metrics["returned_unique_probe_count"] == 2
    assert metrics["missing_probe_count"] == 1
    assert metrics["coverage_pct"] == pytest.approx(66.6666667)


def test_vn_metadata_contains_reproducible_probe_coverage(window):
    metrics = coverage_metrics([3, 1, 2], [{"prb_id": 1}, {"prb_id": 2}])
    envelope = build_envelope(
        86206926, window, [{"prb_id": 1}, {"prb_id": 2}],
        "https://atlas.ripe.net/api/v2/measurements/86206926/results/",
        extra_metadata={
            "country_code": "VN",
            "probe_status_filter": "Connected",
            "requested_probe_ids": [1, 2, 3],
            **metrics,
        },
    )

    metadata = envelope["metadata"]
    assert metadata["country_code"] == "VN"
    assert metadata["probe_status_filter"] == "Connected"
    assert metadata["requested_probe_ids"] == [1, 2, 3]
    assert metadata["record_count"] == 2
    assert metadata["returned_unique_probe_count"] == 2
    assert metadata["missing_probe_count"] == 1


def test_vn_bronze_key_remains_deterministic(window):
    first = bronze_object_key(86206926, window)
    second = bronze_object_key(86206926, window)

    assert first == second
    assert "measurement_id=86206926" in first


def test_probe_ids_tolerate_conflicting_duplicate_metadata():
    session = Mock()

    next_url = (
        "https://atlas.ripe.net/"
        "api/v2/probes/?page=2"
    )

    session.get.side_effect = [
        response({
            "results": [
                {
                    "id": 7,
                    "country_code": "US",
                    "asn_v4": 100,
                },
                {
                    "id": 3,
                    "country_code": "US",
                },
            ],
            "next": next_url,
        }),
        response({
            "results": [
                {
                    "id": 7,
                    "country_code": "US",
                    "asn_v4": 200,
                },
                {
                    "id": 5,
                    "country_code": "US",
                },
            ],
            "next": None,
        }),
    ]

    client = RipeAtlasClient(
        "https://atlas.ripe.net/api/v2",
        session=session,
    )

    assert (
        client.get_connected_probe_ids(
            "US"
        )
        == [3, 5, 7]
    )

    first = (
        session
        .get
        .call_args_list[0]
    )

    assert (
        first.kwargs[
            "params"
        ][
            "country_code"
        ]
        == "US"
    )

    assert (
        first.kwargs[
            "params"
        ][
            "status_name"
        ]
        == "Connected"
    )

    assert (
        first.kwargs[
            "params"
        ][
            "page_size"
        ]
        == 500
    )

    second = (
        session
        .get
        .call_args_list[1]
    )

    assert (
        second.args[0]
        == next_url
    )

    assert (
        second.kwargs[
            "params"
        ]
        is None
    )

