"""Kafka topic and connection configuration."""

import os


RIPE_ATLAS_PING_TOPIC = (
    os.getenv("RIPE_ATLAS_PING_TOPIC", "network.raw.ripe_atlas.ping.v1")
)

RIPE_ATLAS_PING_DLQ_TOPIC = (
    os.getenv("RIPE_ATLAS_PING_DLQ_TOPIC", "network.dlq.ripe_atlas.ping.v1")
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
    os.getenv("BRONZE_RIPE_ATLAS_CONSUMER_GROUP", "bronze-ripe-atlas-ping-v1")
)
