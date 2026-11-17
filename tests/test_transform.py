import pytest

from processing.silver.ripe_atlas_ping import (calculate_packet_loss,
                                                transform_record)


@pytest.mark.parametrize(("sent", "received", "expected"), [
    (3, 3, 0.0), (3, 2, 100 / 3), (3, 0, 100.0),
])
def test_packet_loss(sent, received, expected):
    assert calculate_packet_loss(sent, received) == pytest.approx(expected)


@pytest.mark.parametrize(("sent", "received"), [(0, 0), (3, 4), (-1, 0), (3, -1)])
def test_invalid_packet_counts(sent, received):
    assert calculate_packet_loss(sent, received) is None


def test_missing_rtt_fields_and_packet_array_fallback():
    result = transform_record({"timestamp": 1, "msm_id": 1009, "prb_id": 7,
                               "result": [{"rtt": 2.5}, {"x": "*"}]})
    assert result["rtt_min_ms"] is None
    assert result["packets_sent"] == 2
    assert result["packets_received"] == 1
    assert result["packet_loss_pct"] == 50.0


def test_raw_to_silver_mapping():
    result = transform_record({"timestamp": 10, "msm_id": 1009, "prb_id": 22,
                               "dst_name": "a.root-servers.net",
                               "dst_addr": "198.41.0.4", "af": 4, "proto": "ICMP",
                               "min": 1, "avg": 2, "max": 3, "sent": 3, "rcvd": 2})
    assert result["measurement_id"] == 1009
    assert result["destination_address"] == "198.41.0.4"
    assert result["rtt_avg_ms"] == 2

