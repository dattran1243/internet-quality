#!/usr/bin/env python
"""Consume RIPE Atlas Kafka events and persist them to MinIO Bronze."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sys


sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)


from confluent_kafka import (
    Consumer,
    KafkaException,
)

from configs.settings import settings

from ingestion.bronze.ripe_atlas import (
    bronze_object_key,
    build_bronze_envelope,
    validate_event,
)

from messaging.kafka_config import (
    BRONZE_RIPE_ATLAS_CONSUMER_GROUP,
    KAFKA_BOOTSTRAP_SERVERS,
    RIPE_ATLAS_PING_DLQ_TOPIC,
    RIPE_ATLAS_PING_TOPIC,
)

from messaging.producer import (
    JsonKafkaProducer,
)

from storage.minio_client import (
    create_s3_client,
    ensure_bucket,
    put_json,
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help=(
            "Stop after this many Kafka messages. "
            "Useful for local testing."
        ),
    )

    return parser.parse_args()


def decode_key(raw: bytes | None) -> str | None:
    if raw is None:
        return None

    return raw.decode(
        "utf-8",
        errors="replace",
    )


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def main() -> None:
    logging.basicConfig(
        level=settings.log_level,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    log = logging.getLogger(__name__)

    args = arguments()

    settings.require_storage_credentials()

    s3 = create_s3_client(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
    )

    ensure_bucket(
        s3,
        settings.minio_bucket,
    )

    consumer = Consumer(
        {
            "bootstrap.servers":
                KAFKA_BOOTSTRAP_SERVERS,

            "group.id":
                BRONZE_RIPE_ATLAS_CONSUMER_GROUP,

            "enable.auto.commit":
                False,

            "auto.offset.reset":
                "earliest",
        }
    )

    dlq = JsonKafkaProducer(
        KAFKA_BOOTSTRAP_SERVERS
    )

    consumer.subscribe(
        [RIPE_ATLAS_PING_TOPIC]
    )

    processed = 0
    bronze_written = 0
    dlq_written = 0

    log.info(
        "Bronze Kafka consumer start "
        "topic=%s group=%s",
        RIPE_ATLAS_PING_TOPIC,
        BRONZE_RIPE_ATLAS_CONSUMER_GROUP,
    )

    try:
        while True:
            message = consumer.poll(
                timeout=1.0
            )

            if message is None:
                continue

            if message.error():
                raise KafkaException(
                    message.error()
                )

            kafka_key = decode_key(
                message.key()
            )

            raw_value = (
                message.value()
                or b""
            )

            operation_phase = "validation"

            try:
                event = json.loads(
                    raw_value.decode("utf-8")
                )

                if not isinstance(event, dict):
                    raise ValueError(
                        "Kafka value must be JSON object"
                    )

                validate_event(
                    event,
                    kafka_key,
                )

                operation_phase = "processing"

                key = bronze_object_key(
                    event
                )

                envelope = (
                    build_bronze_envelope(
                        event,

                        kafka_topic=(
                            message.topic()
                        ),

                        kafka_partition=(
                            message.partition()
                        ),

                        kafka_offset=(
                            message.offset()
                        ),

                        kafka_key=kafka_key,
                    )
                )

                put_json(
                    s3,
                    settings.minio_bucket,
                    key,
                    envelope,
                )

                # Commit only after MinIO write succeeded.
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                bronze_written += 1

                log.info(
                    "Bronze object written "
                    "country=%s "
                    "measurement=%s "
                    "chunk=%s/%s "
                    "records=%s "
                    "path=s3://%s/%s",
                    event["country_code"],
                    event["measurement_id"],
                    event["chunk_index"] + 1,
                    event["chunk_count"],
                    len(
                        event["payload"]["records"]
                    ),
                    settings.minio_bucket,
                    key,
                )

            except Exception as exc:
                if operation_phase != "validation":
                    log.exception(
                        "Bronze processing failure; "
                        "source offset NOT committed "
                        "partition=%s offset=%s",
                        message.partition(),
                        message.offset(),
                    )
                    raise

                # Invalid contract / poison message:
                # preserve it in DLQ, then commit its source offset.
                dlq_payload = {
                    "failed_at_utc": utc_now(),

                    "source_topic":
                        message.topic(),

                    "source_partition":
                        message.partition(),

                    "source_offset":
                        message.offset(),

                    "source_key":
                        kafka_key,

                    "error":
                        f"{type(exc).__name__}: {exc}",

                    "raw_value":
                        raw_value.decode(
                            "utf-8",
                            errors="replace",
                        ),
                }

                dlq.publish(
                    topic=RIPE_ATLAS_PING_DLQ_TOPIC,

                    key=(
                        kafka_key
                        or (
                            f"{message.topic()}|"
                            f"{message.partition()}|"
                            f"{message.offset()}"
                        )
                    ),

                    payload=dlq_payload,
                )

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                dlq_written += 1

                log.warning(
                    "Message moved to DLQ "
                    "partition=%s offset=%s "
                    "error=%s",
                    message.partition(),
                    message.offset(),
                    exc,
                )

            processed += 1

            if (
                args.max_messages is not None
                and processed >= args.max_messages
            ):
                break

    except KeyboardInterrupt:
        log.info(
            "Bronze consumer interrupted"
        )

    finally:
        consumer.close()

    log.info(
        "Bronze Kafka consumer complete "
        "processed=%s bronze_written=%s dlq_written=%s",
        processed,
        bronze_written,
        dlq_written,
    )


if __name__ == "__main__":
    main()