from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from ingestion.ripe_atlas.client import RipeAtlasClient
from ingestion.ripe_atlas.models import (
    TimeWindow,
    parse_utc,
    utc_text,
)


def test_timestamp_conversion_to_utc():
    value = parse_utc(
        "2026-09-12T07:00:00+07:00"
    )

    assert (
        utc_text(value)
        == "2026-09-12T00:00:00Z"
    )


def test_naive_timestamp_rejected():
    with pytest.raises(
        ValueError
    ):
        parse_utc(
            "2026-09-12T00:00:00"
        )


def test_client_uses_official_results_endpoint_without_live_request():
    response = Mock(
        status_code=200
    )

    response.json.return_value = [
        {
            "msm_id": 1009
        }
    ]

    response.raise_for_status.return_value = (
        None
    )

    session = Mock()
    session.get.return_value = response

    client = RipeAtlasClient(
        "https://atlas.ripe.net/api/v2",
        session=session,
    )

    window = TimeWindow(
        datetime(
            2026,
            9,
            12,
            tzinfo=timezone.utc,
        ),
        datetime(
            2026,
            9,
            12,
            1,
            tzinfo=timezone.utc,
        ),
    )

    assert (
        client.fetch_results(
            1009,
            window,
        )
        == [
            {
                "msm_id": 1009
            }
        ]
    )

    _, kwargs = (
        session.get.call_args
    )

    assert (
        kwargs["params"]["start"]
        == "2026-09-12T00:00:00Z"
    )

    assert (
        kwargs["params"]["stop"]
        == "2026-09-12T01:00:00Z"
    )


def test_measurement_current_probe_ids_are_deduplicated_and_sorted():
    response = Mock(
        status_code=200
    )

    response.json.return_value = {
        "id": 86206926,
        "current_probes": [
            9,
            2,
            9,
            5,
        ],
    }

    response.raise_for_status.return_value = (
        None
    )

    session = Mock()
    session.get.return_value = response

    client = RipeAtlasClient(
        "https://atlas.ripe.net/api/v2",
        session=session,
    )

    assert (
        client.get_measurement_current_probe_ids(
            86206926
        )
        == [2, 5, 9]
    )

    args, kwargs = (
        session.get.call_args
    )

    assert (
        args[0]
        == (
            "https://atlas.ripe.net/api/v2/"
            "measurements/86206926/"
        )
    )

    assert (
        kwargs["params"]
        == {
            "optional_fields":
                "current_probes",
        }
    )


def test_measurement_current_probe_ids_reject_non_positive_measurement():
    session = Mock()

    client = RipeAtlasClient(
        "https://atlas.ripe.net/api/v2",
        session=session,
    )

    with pytest.raises(
        ValueError,
        match="measurement_id must be positive",
    ):
        client.get_measurement_current_probe_ids(
            0
        )

    session.get.assert_not_called()


def test_measurement_current_probe_ids_reject_non_object_payload():
    response = Mock(
        status_code=200
    )

    response.json.return_value = [
        1,
        2,
        3,
    ]

    response.raise_for_status.return_value = (
        None
    )

    session = Mock()
    session.get.return_value = response

    client = RipeAtlasClient(
        "https://atlas.ripe.net/api/v2",
        session=session,
    )

    with pytest.raises(
        ValueError,
        match="must be a JSON object",
    ):
        client.get_measurement_current_probe_ids(
            86206926
        )


def test_measurement_current_probe_ids_reject_malformed_payload():
    response = Mock(
        status_code=200
    )

    response.json.return_value = {
        "id": 86206926,
        "current_probes": "1,2,3",
    }

    response.raise_for_status.return_value = (
        None
    )

    session = Mock()
    session.get.return_value = response

    client = RipeAtlasClient(
        "https://atlas.ripe.net/api/v2",
        session=session,
    )

    with pytest.raises(
        ValueError,
        match="current_probes",
    ):
        client.get_measurement_current_probe_ids(
            86206926
        )


def test_measurement_current_probe_ids_reject_invalid_probe_id():
    response = Mock(
        status_code=200
    )

    response.json.return_value = {
        "id": 86206926,
        "current_probes": [
            1,
            -7,
        ],
    }

    response.raise_for_status.return_value = (
        None
    )

    session = Mock()
    session.get.return_value = response

    client = RipeAtlasClient(
        "https://atlas.ripe.net/api/v2",
        session=session,
    )

    with pytest.raises(
        ValueError,
        match="invalid current probe ID",
    ):
        client.get_measurement_current_probe_ids(
            86206926
        )


def test_measurement_current_probe_ids_reject_boolean_probe_id():
    response = Mock(
        status_code=200
    )

    response.json.return_value = {
        "id": 86206926,
        "current_probes": [
            1,
            True,
        ],
    }

    response.raise_for_status.return_value = (
        None
    )

    session = Mock()
    session.get.return_value = response

    client = RipeAtlasClient(
        "https://atlas.ripe.net/api/v2",
        session=session,
    )

    with pytest.raises(
        ValueError,
        match="invalid current probe ID",
    ):
        client.get_measurement_current_probe_ids(
            86206926
        )