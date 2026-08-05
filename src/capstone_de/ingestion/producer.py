"""Kafka producer entry service for validated event serialization."""

from __future__ import annotations

import logging

from capstone_de.domain.events import CustomerEvent
from capstone_de.infrastructure.kafka import KafkaClient
from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)


class EventProducer:
    """Publishes domain events to the raw Kafka topic."""

    def __init__(self, kafka: KafkaClient, settings: Settings) -> None:
        self._kafka = kafka
        self._settings = settings

    def publish(self, event: CustomerEvent) -> None:
        """Serialize and publish an event with a stable business key."""
        self._kafka.publish(
            self._settings.kafka_raw_topic,
            event.model_dump(mode="json"),
            key=str(event.event_id),
        )
        LOGGER.info("published raw event", extra={"event_id": str(event.event_id)})
