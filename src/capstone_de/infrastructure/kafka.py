"""Kafka adapters using the production confluent-kafka client."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ConsumedMessage:
    """Decoded Kafka message with immutable source coordinates."""

    value: dict[str, Any]
    topic: str
    partition: int
    offset: int


class KafkaClient:
    """Owns producer, consumer, topic administration, and delivery handling."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def ensure_topics(self) -> None:
        """Create required topics when they do not already exist."""
        admin = AdminClient({"bootstrap.servers": self._settings.kafka_bootstrap_servers})
        requested = [
            NewTopic(topic, num_partitions=3, replication_factor=1)
            for topic in (
                self._settings.kafka_raw_topic,
                self._settings.kafka_validated_topic,
                self._settings.kafka_quarantine_topic,
            )
        ]
        futures = admin.create_topics(requested, request_timeout=15)
        for topic, future in futures.items():
            try:
                future.result(15)
                LOGGER.info("created Kafka topic", extra={"topic": topic})
            except Exception as error:
                if "TOPIC_ALREADY_EXISTS" not in str(error):
                    raise RuntimeError(f"Unable to create Kafka topic {topic}") from error
                LOGGER.info("Kafka topic already exists", extra={"topic": topic})

    def publish(self, topic: str, payload: dict[str, Any], key: str) -> None:
        """Publish a JSON message synchronously so failed delivery is never hidden."""
        producer = Producer({"bootstrap.servers": self._settings.kafka_bootstrap_servers})
        delivery_error: list[Exception] = []

        def delivered(error: KafkaError | None, message: Message) -> None:
            if error is not None:
                delivery_error.append(RuntimeError(str(error)))
                return
            LOGGER.info(
                "Kafka message delivered",
                extra={
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                },
            )

        producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            on_delivery=delivered,
        )
        remaining = producer.flush(30)
        if remaining != 0 or delivery_error:
            detail = str(delivery_error[0]) if delivery_error else "delivery timed out"
            raise RuntimeError(f"Kafka publish failed for {topic}: {detail}")

    def consume_forever(self, topic: str, group_id: str) -> Iterator[ConsumedMessage]:
        """Yield decoded messages and commit only after the caller successfully handles them."""
        consumer = Consumer(
            {
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])
        try:
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"Kafka consumer error: {message.error()}")
                try:
                    raw_value = message.value().decode("utf-8")
                    parsed = json.loads(raw_value)
                    if not isinstance(parsed, dict):
                        raise ValueError("Kafka message must contain a JSON object")
                    yield ConsumedMessage(
                        parsed, message.topic(), message.partition(), message.offset()
                    )
                    consumer.commit(message=message, asynchronous=False)
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                    LOGGER.error("unreadable Kafka payload", exc_info=error)
                    raise
        finally:
            consumer.close()
