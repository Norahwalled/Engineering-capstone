"""Kafka boundary validation and dead-letter/quarantine routing."""

from __future__ import annotations

import logging

from pydantic import ValidationError

from capstone_de.application.contracts import KafkaMetadata, QuarantineRecord
from capstone_de.domain.events import CustomerEvent
from capstone_de.infrastructure.kafka import ConsumedMessage, KafkaClient
from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)


class ValidationConsumer:
    """Validates raw messages before any lakehouse write can occur."""

    def __init__(self, kafka: KafkaClient, settings: Settings) -> None:
        self._kafka = kafka
        self._settings = settings

    def run_forever(self) -> None:
        """Consume raw events, publish valid events, and quarantine invalid events."""
        for message in self._kafka.consume_forever(
            self._settings.kafka_raw_topic,
            self._settings.kafka_consumer_group,
        ):
            self._handle_message(message)

    def _handle_message(self, message: ConsumedMessage) -> None:
        try:
            event = CustomerEvent.model_validate(message.value)
        except ValidationError as error:
            record = QuarantineRecord(
                payload=message.value,
                rejection_reason=error.json(),
                source=KafkaMetadata(
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset,
                ),
            )
            self._kafka.publish(
                self._settings.kafka_quarantine_topic,
                record.model_dump(mode="json"),
                key=f"{message.topic}:{message.partition}:{message.offset}",
            )
            LOGGER.warning(
                "quarantined invalid event",
                extra={"offset": message.offset, "topic": message.topic},
            )
            return

        self._kafka.publish(
            self._settings.kafka_validated_topic,
            event.model_dump(mode="json"),
            key=str(event.event_id),
        )
        LOGGER.info("validated event", extra={"event_id": str(event.event_id)})
