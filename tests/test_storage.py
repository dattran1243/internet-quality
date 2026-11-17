from datetime import datetime, timezone
from unittest.mock import Mock

from ingestion.ripe_atlas.ingest import bronze_object_key
from ingestion.ripe_atlas.models import TimeWindow
from storage.minio_client import put_json


def test_deterministic_bronze_object_key():
    window = TimeWindow(datetime(2026, 9, 12, tzinfo=timezone.utc),
                        datetime(2026, 9, 12, 1, tzinfo=timezone.utc))
    expected = ("bronze/ripe_atlas/ping/measurement_id=1009/year=2026/"
                "month=09/day=12/hour=00/1009_20260912T000000Z_20260912T010000Z.json")
    assert bronze_object_key(1009, window) == expected
    assert bronze_object_key(1009, window) == expected


def test_put_json_uploads_one_object():
    client = Mock()
    put_json(client, "telecom-data", "a.json", {"records": []})
    client.upload_fileobj.assert_called_once()

