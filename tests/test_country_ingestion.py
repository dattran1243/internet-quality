from datetime import datetime, timezone
from unittest.mock import Mock, call

import pytest

import ingestion.ripe_atlas.ingest_country as country_ingest
from ingestion.ripe_atlas.ingest_country import (
    PROBE_SCOPE_SCHEMA_VERSION,
    _validate_probe_scope_manifest,
    get_or_create_probe_scope,
    ingest_country_measurements,
    probe_scope_object_key,
)
from ingestion.ripe_atlas.models import TimeWindow


@pytest.fixture
def window():
    return TimeWindow(
        datetime(
            2026,
            9,
            20,
            0,
            0,
            0,
            tzinfo=timezone.utc,
        ),
        datetime(
            2026,
            9,
            20,
            1,
            0,
            0,
            tzinfo=timezone.utc,
        ),
    )


def test_probe_scope_key_contains_country_measurement_and_window(
    window,
):
    key = probe_scope_object_key(
        "de",
        86206926,
        window,
    )

    assert (
        key
        == (
            "bronze/ripe_atlas/"
            "probe_scope/"
            "country=DE/"
            "measurement_id=86206926/"
            "year=2026/"
            "month=09/"
            "day=20/"
            "hour=00/"
            "DE_86206926_"
            "20260920T000000Z_"
            "20260920T010000Z.json"
        )
    )


def test_probe_scope_key_differs_between_measurements(
    window,
):
    google_key = probe_scope_object_key(
        "DE",
        86206926,
        window,
    )

    cloudflare_key = probe_scope_object_key(
        "DE",
        176906978,
        window,
    )

    assert google_key != cloudflare_key

    assert (
        "measurement_id=86206926"
        in google_key
    )

    assert (
        "measurement_id=176906978"
        in cloudflare_key
    )


def test_new_probe_scope_intersects_country_and_measurement(
    window,
    monkeypatch,
):
    client = Mock()

    client.get_connected_probe_ids.return_value = [
        1,
        2,
        3,
        4,
    ]

    client.get_measurement_current_probe_ids.return_value = [
        2,
        3,
        5,
    ]

    s3 = Mock()

    monkeypatch.setattr(
        country_ingest,
        "ensure_bucket",
        Mock(),
    )

    monkeypatch.setattr(
        country_ingest,
        "_read_json_object",
        Mock(
            return_value=None
        ),
    )

    stored = {}

    def fake_put_json(
        s3_client,
        bucket,
        key,
        payload,
    ):
        stored["bucket"] = bucket
        stored["key"] = key
        stored["payload"] = payload

    monkeypatch.setattr(
        country_ingest,
        "put_json",
        fake_put_json,
    )

    (
        probe_ids,
        scope_key,
        reused,
        country_probe_ids,
    ) = get_or_create_probe_scope(
        client=client,
        s3_client=s3,
        bucket="telecom-data",
        country_code="DE",
        measurement_id=86206926,
        window=window,
        status_name="Connected",
    )

    assert probe_ids == [
        2,
        3,
    ]

    assert country_probe_ids == [
        1,
        2,
        3,
        4,
    ]

    assert reused is False

    assert (
        "country=DE"
        in scope_key
    )

    assert (
        "measurement_id=86206926"
        in scope_key
    )

    client.get_connected_probe_ids.assert_called_once_with(
        "DE",
        "Connected",
    )

    client.get_measurement_current_probe_ids.assert_called_once_with(
        86206926
    )

    manifest = stored[
        "payload"
    ]

    assert (
        manifest["schema_version"]
        == PROBE_SCOPE_SCHEMA_VERSION
    )

    assert (
        manifest["scope_type"]
        == "country_measurement"
    )

    assert (
        manifest["country_code"]
        == "DE"
    )

    assert (
        manifest["measurement_id"]
        == 86206926
    )

    assert (
        manifest["status_name"]
        == "Connected"
    )

    assert (
        manifest["country_connected_probe_count"]
        == 4
    )

    assert (
        manifest["measurement_participant_count"]
        == 3
    )

    assert (
        manifest["probe_count"]
        == 2
    )

    assert (
        manifest["requested_probe_ids"]
        == [2, 3]
    )

    assert (
        manifest[
            "participant_snapshot_at_utc"
        ]
    )

    assert (
        stored["bucket"]
        == "telecom-data"
    )

    assert (
        stored["key"]
        == scope_key
    )


def test_existing_probe_scope_is_reused_without_live_discovery(
    window,
    monkeypatch,
):
    client = Mock()
    s3 = Mock()

    manifest = {
        "schema_version":
            PROBE_SCOPE_SCHEMA_VERSION,

        "source":
            "ripe_atlas",

        "scope_type":
            "country_measurement",

        "country_code":
            "DE",

        "measurement_id":
            86206926,

        "status_name":
            "Connected",

        "requested_start_utc":
            "2026-09-20T00:00:00Z",

        "requested_stop_utc":
            "2026-09-20T01:00:00Z",

        "discovered_at_utc":
            "2026-09-29T12:00:00Z",

        "participant_snapshot_at_utc":
            "2026-09-29T12:00:00Z",

        "country_connected_probe_count":
            4,

        "measurement_participant_count":
            5,

        "probe_count":
            2,

        "requested_probe_ids":
            [
                2,
                3,
            ],
    }

    monkeypatch.setattr(
        country_ingest,
        "ensure_bucket",
        Mock(),
    )

    monkeypatch.setattr(
        country_ingest,
        "_read_json_object",
        Mock(
            return_value=manifest
        ),
    )

    put_json_mock = Mock()

    monkeypatch.setattr(
        country_ingest,
        "put_json",
        put_json_mock,
    )

    (
        probe_ids,
        scope_key,
        reused,
        country_probe_ids,
    ) = get_or_create_probe_scope(
        client=client,
        s3_client=s3,
        bucket="telecom-data",
        country_code="DE",
        measurement_id=86206926,
        window=window,
        status_name="Connected",
    )

    assert probe_ids == [
        2,
        3,
    ]

    assert reused is True

    assert country_probe_ids is None

    assert (
        "measurement_id=86206926"
        in scope_key
    )

    client.get_connected_probe_ids.assert_not_called()

    client.get_measurement_current_probe_ids.assert_not_called()

    put_json_mock.assert_not_called()


def test_probe_scope_manifest_rejects_wrong_measurement(
    window,
):
    manifest = {
        "schema_version":
            PROBE_SCOPE_SCHEMA_VERSION,

        "source":
            "ripe_atlas",

        "scope_type":
            "country_measurement",

        "country_code":
            "DE",

        "measurement_id":
            176906978,

        "status_name":
            "Connected",

        "requested_start_utc":
            "2026-09-20T00:00:00Z",

        "requested_stop_utc":
            "2026-09-20T01:00:00Z",

        "discovered_at_utc":
            "2026-09-29T12:00:00Z",

        "participant_snapshot_at_utc":
            "2026-09-29T12:00:00Z",

        "country_connected_probe_count":
            10,

        "measurement_participant_count":
            10,

        "probe_count":
            2,

        "requested_probe_ids":
            [
                1,
                2,
            ],
    }

    with pytest.raises(
        ValueError,
        match="measurement_id does not match",
    ):
        _validate_probe_scope_manifest(
            manifest,
            country_code="DE",
            measurement_id=86206926,
            status_name="Connected",
            window=window,
        )


def test_multi_measurement_ingestion_uses_distinct_eligible_scopes(
    window,
    monkeypatch,
):
    client = Mock()

    client.get_connected_probe_ids.return_value = [
        1,
        2,
        3,
        4,
    ]

    client.get_measurement_current_probe_ids.side_effect = [
        [
            1,
            2,
            5,
        ],
        [
            2,
            3,
            6,
        ],
    ]

    client.fetch_results.side_effect = [
        [
            {
                "prb_id": 1,
            },
            {
                "prb_id": 2,
            },
        ],
        [
            {
                "prb_id": 2,
            },
            {
                "prb_id": 3,
            },
        ],
    ]

    client.results_url.side_effect = (
        lambda measurement_id: (
            "https://atlas.ripe.net/api/v2/"
            f"measurements/{measurement_id}/"
            "results/"
        )
    )

    s3 = Mock()

    monkeypatch.setattr(
        country_ingest,
        "ensure_bucket",
        Mock(),
    )

    monkeypatch.setattr(
        country_ingest,
        "_read_json_object",
        Mock(
            return_value=None
        ),
    )

    scope_manifests = []

    def fake_put_json(
        s3_client,
        bucket,
        key,
        payload,
    ):
        scope_manifests.append(
            (
                key,
                payload,
            )
        )

    monkeypatch.setattr(
        country_ingest,
        "put_json",
        fake_put_json,
    )

    bronze_metadata = []

    def fake_store_country_bronze_records(
        client,
        s3_client,
        bucket,
        measurement_id,
        window,
        country_code,
        records,
        extra_metadata=None,
    ):
        bronze_metadata.append({
            "measurement_id":
                measurement_id,

            "metadata":
                dict(
                    extra_metadata
                    or {}
                ),
        })

        return (
            "bronze/test/"
            f"{measurement_id}.json"
        )

    monkeypatch.setattr(
        country_ingest,
        "store_country_bronze_records",
        fake_store_country_bronze_records,
    )

    summaries = (
        ingest_country_measurements(
            client=client,
            s3_client=s3,
            bucket="telecom-data",
            measurement_ids=[
                86206926,
                176906978,
            ],
            window=window,
            country_code="DE",
            status_name="Connected",
        )
    )

    # Country Connected inventory is discovered once
    # and reused across new measurement scopes in this run.
    client.get_connected_probe_ids.assert_called_once_with(
        "DE",
        "Connected",
    )

    assert (
        client.get_measurement_current_probe_ids.call_args_list
        == [
            call(
                86206926
            ),
            call(
                176906978
            ),
        ]
    )

    # Google eligibility:
    # {1,2,3,4} INTERSECT {1,2,5}
    # -> {1,2}
    #
    # Cloudflare eligibility:
    # {1,2,3,4} INTERSECT {2,3,6}
    # -> {2,3}
    assert (
        client.fetch_results.call_args_list
        == [
            call(
                86206926,
                window,
                probe_ids=[
                    1,
                    2,
                ],
            ),
            call(
                176906978,
                window,
                probe_ids=[
                    2,
                    3,
                ],
            ),
        ]
    )

    assert len(
        scope_manifests
    ) == 2

    google_scope = (
        scope_manifests[0]
    )

    cloudflare_scope = (
        scope_manifests[1]
    )

    assert (
        google_scope[0]
        != cloudflare_scope[0]
    )

    assert (
        "measurement_id=86206926"
        in google_scope[0]
    )

    assert (
        "measurement_id=176906978"
        in cloudflare_scope[0]
    )

    assert (
        google_scope[1][
            "requested_probe_ids"
        ]
        == [
            1,
            2,
        ]
    )

    assert (
        cloudflare_scope[1][
            "requested_probe_ids"
        ]
        == [
            2,
            3,
        ]
    )

    assert len(
        bronze_metadata
    ) == 2

    assert (
        bronze_metadata[0][
            "metadata"
        ][
            "requested_probe_ids"
        ]
        == [
            1,
            2,
        ]
    )

    assert (
        bronze_metadata[1][
            "metadata"
        ][
            "requested_probe_ids"
        ]
        == [
            2,
            3,
        ]
    )

    assert (
        bronze_metadata[0][
            "metadata"
        ][
            "probe_scope_key"
        ]
        != bronze_metadata[1][
            "metadata"
        ][
            "probe_scope_key"
        ]
    )

    assert len(
        summaries
    ) == 2

    assert (
        summaries[0][
            "probe_scope_key"
        ]
        != summaries[1][
            "probe_scope_key"
        ]
    )

    assert (
        summaries[0][
            "requested_probe_count"
        ]
        == 2
    )

    assert (
        summaries[1][
            "requested_probe_count"
        ]
        == 2
    )