"""Read-only broker evidence, plus replay using the original project producer."""
from __future__ import annotations

import json

from processing.e2e.contracts import MID, START, iso, canonical, digest, utc_now, check_raw
from processing.e2e.guards import require


def broker_state(m):
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient
    r = m["resources"]
    common = {"bootstrap.servers": m["endpoints"]["kafka"], "client.id": r["rid"]}
    metadata = AdminClient(common).list_topics(timeout=10)
    client = Consumer({**common, "group.id": r["group"], "enable.auto.commit": False,
                       "allow.auto.create.topics": False})
    try:
        result = {}
        for name in (r["topic"], r["dlq"]):
            require(name in metadata.topics and metadata.topics[name].error is None, "TOPIC_MISSING")
            parts = metadata.topics[name].partitions
            require(set(parts) == {0} and len(parts[0].replicas) == 1, "TOPIC_PARTITION_SCOPE")
            low, high = client.get_watermark_offsets(TopicPartition(name, 0), timeout=10, cached=False)
            result[name] = {"low": low, "high": high}
        result["committed"] = client.committed([TopicPartition(r["topic"], 0)], timeout=10)[0].offset
        return result
    finally:
        client.close()  # no subscribe/assignment/commit; this inspector consumes nothing


def publish(evidence):
    from scripts.publish_ripe_country_kafka import chunk_records
    from messaging.contracts import SCHEMA_VERSION, RipeAtlasPingEvent, build_window_id, build_event_id
    from messaging.producer import JsonKafkaProducer
    inputs = evidence.read("pin_raw_and_scope")
    raw = [json.loads(line) for line in evidence.get(inputs["raw"]).splitlines() if line.strip()]
    windows = check_raw(raw)
    producer = JsonKafkaProducer(evidence.m["endpoints"]["kafka"])
    events = []
    for i, rows in enumerate(windows):
        start, stop = iso(START+i*300), iso(START+(i+1)*300)
        chunks = chunk_records(rows)
        require(len(chunks) == 1, "EXPECTED_ONE_CHUNK_PER_WINDOW")
        window_id = build_window_id(source="ripe_atlas", dataset="ping", country_code="DE",
                                   measurement_id=str(MID), window_start_utc=start, window_stop_utc=stop)
        for n, chunk in enumerate(chunks):
            event = RipeAtlasPingEvent(SCHEMA_VERSION, build_event_id(window_id=window_id, chunk_index=n),
                window_id, "ripe_atlas", "ping", "DE", str(MID), start, stop, utc_now(), n, len(chunks),
                {"records": chunk, "record_count": len(chunk), "probe_scope_key": inputs["scope"]["key"],
                 "probe_scope_reused": True, "probe_status_filter": "Connected",
                 "source_url": inputs["source_url"], "coverage": {"source_mode": "verified_historical_raw_replay"}})
            payload = event.to_dict()
            require(len(canonical(payload)) < 900_000, "EVENT_TOO_LARGE")
            producer.publish(topic=evidence.m["resources"]["topic"], key=event.kafka_key, payload=payload)
            events.append({"event_id": event.event_id, "window_id": window_id, "key": event.kafka_key,
                           "payload_sha256": digest(payload), "window_start": start, "records": len(chunk)})
    return {"events": events, "expected_messages": len(events), "source_mode": "verified_historical_raw_replay"}
