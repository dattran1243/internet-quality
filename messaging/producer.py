"""Shared JSON Kafka producer."""

from __future__ import annotations

import json
from typing import Any

from confluent_kafka import Producer


class JsonKafkaProducer:
    """Publish JSON messages with reliable Kafka acknowledgements."""

    def __init__(
        self,
        bootstrap_servers: str,
    ) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    def publish(
        self,
        *,
        topic: str,
        key: str,
        payload: dict[str, Any],
    ) -> None:
        """Publish one JSON event and wait for delivery."""

        delivery_error: list[Exception] = []

        def on_delivery(error, message) -> None:
            if error is not None:
                delivery_error.append(
                    RuntimeError(str(error))
                )

        value = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value,
            on_delivery=on_delivery,
        )

        remaining = self._producer.flush(30)

        if remaining:
            raise TimeoutError(
                f"{remaining} Kafka message(s) "
                "were not delivered"
            )

        if delivery_error:
            raise delivery_error[0]