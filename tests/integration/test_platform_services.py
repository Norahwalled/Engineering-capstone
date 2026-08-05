"""Integration checks that execute only against real Docker Compose services."""

from __future__ import annotations

import os

import pytest

from capstone_de.infrastructure.kafka import KafkaClient
from capstone_de.settings import Settings


@pytest.mark.integration
def test_real_kafka_topic_administration() -> None:
    """Verify a reachable real Kafka broker accepts required topic administration."""
    if os.getenv("CAPSTONE_RUN_INTEGRATION") != "1":
        pytest.skip("Set CAPSTONE_RUN_INTEGRATION=1 after starting Docker Compose services")
    KafkaClient(Settings()).ensure_topics()
