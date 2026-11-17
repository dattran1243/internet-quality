"""Kafka topic and connection configuration."""

import os


RIPE_ATLAS_PING_TOPIC = (
    "network.raw.ripe_atlas.ping.v1"
)

RIPE_ATLAS_PING_DLQ_TOPIC = (
    "network.dlq.ripe_atlas.ping.v1"
)

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = (
    "localhost:9092"
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
)

DEFAULT_KAFKA_PARTITIONS = 3
DEFAULT_KAFKA_REPLICATION_FACTOR = 1
BRONZE_RIPE_ATLAS_CONSUMER_GROUP = (
    "bronze-ripe-atlas-ping-v1"
)

# Historical backfill routing.
# The default realtime configuration remains unchanged.
RIPE_ATLAS_PIPELINE_MODE = os.getenv(
    "RIPE_ATLAS_PIPELINE_MODE", "realtime"
).strip().lower()

if RIPE_ATLAS_PIPELINE_MODE not in {"realtime", "historical"}:
    raise ValueError(
        f"Unsupported pipeline mode: {RIPE_ATLAS_PIPELINE_MODE}"
    )

if RIPE_ATLAS_PIPELINE_MODE == "historical":
    RIPE_ATLAS_PING_TOPIC = (
        "network.raw.ripe_atlas.ping.backfill.v1"
    )
    RIPE_ATLAS_PING_DLQ_TOPIC = (
        "network.dlq.ripe_atlas.ping.backfill.v1"
    )
    BRONZE_RIPE_ATLAS_CONSUMER_GROUP = (
        "bronze-backfill-ripe-atlas-ping-v1"
    )
