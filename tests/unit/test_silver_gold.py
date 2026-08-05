"""Tests for historical Silver, current-state Silver, and Gold grains."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from capstone_de.lakehouse.gold import aggregate_historical_events
from capstone_de.lakehouse.silver import (
    transform_bronze_to_silver,
    transform_history_to_current,
)


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Provide a small local Spark session for deterministic transformation tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("silver-gold-tests")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def _event(
    event_id: str,
    occurred_at: datetime,
    amount: str,
    *,
    customer_id: str = "customer-001",
    event_type: str = "support_case",
    currency: str = "SAR",
    ingested_minute: int = 0,
    kafka_offset: int = 0,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "customer_id": customer_id,
        "occurred_at": occurred_at,
        "amount": Decimal(amount),
        "currency": currency,
        "document_text": f"Document for {event_id}",
        "schema_version": "1.0",
        "kafka_topic": "validated.events",
        "kafka_partition": 0,
        "kafka_offset": kafka_offset,
        "kafka_timestamp": datetime(2026, 8, 5, 10, ingested_minute, tzinfo=UTC),
        "ingested_at": datetime(2026, 8, 5, 10, ingested_minute, tzinfo=UTC),
    }


def test_historical_silver_preserves_distinct_events_and_deduplicates_replays(
    spark: SparkSession,
) -> None:
    """Separate business events survive while replayed event IDs retain the newest copy."""
    rows = [
        _event("event-1", datetime(2026, 8, 4, 8, tzinfo=UTC), "10.00", kafka_offset=1),
        _event(
            "event-1",
            datetime(2026, 8, 4, 8, tzinfo=UTC),
            "12.00",
            ingested_minute=1,
            kafka_offset=2,
        ),
        _event("event-2", datetime(2026, 8, 4, 9, tzinfo=UTC), "20.00", kafka_offset=3),
    ]

    history = transform_bronze_to_silver(spark.createDataFrame(rows))
    actual = {row.event_id: row.amount for row in history.select("event_id", "amount").collect()}

    assert actual == {"event-1": Decimal("12.00"), "event-2": Decimal("20.00")}


def test_current_state_selects_latest_event_without_removing_history(
    spark: SparkSession,
) -> None:
    """Current state has one latest record while historical Silver retains both events."""
    history = transform_bronze_to_silver(
        spark.createDataFrame(
            [
                _event("event-1", datetime(2026, 8, 4, 8, tzinfo=UTC), "10.00"),
                _event("event-2", datetime(2026, 8, 5, 8, tzinfo=UTC), "20.00"),
            ]
        )
    )

    current = transform_history_to_current(history).collect()

    assert history.count() == 2
    assert len(current) == 1
    assert current[0].event_id == "event-2"


def test_gold_aggregates_all_historical_events_by_customer_day_and_currency(
    spark: SparkSession,
) -> None:
    """Daily metrics include every unique event at the currency-safe Gold grain."""
    history = transform_bronze_to_silver(
        spark.createDataFrame(
            [
                _event("event-1", datetime(2026, 8, 4, 8, tzinfo=UTC), "10.00"),
                _event("event-2", datetime(2026, 8, 4, 9, tzinfo=UTC), "20.00"),
                _event("event-3", datetime(2026, 8, 5, 8, tzinfo=UTC), "30.00"),
            ]
        )
    )

    rows = aggregate_historical_events(history).orderBy("event_day").collect()

    assert [(row.currency, row.event_count, row.total_amount) for row in rows] == [
        ("SAR", 2, Decimal("30.00")),
        ("SAR", 1, Decimal("30.00")),
    ]


def test_gold_does_not_combine_different_currencies(spark: SparkSession) -> None:
    """Amounts in different currencies produce separate rows for one customer and day."""
    history = transform_bronze_to_silver(
        spark.createDataFrame(
            [
                _event("event-sar", datetime(2026, 8, 4, 8, tzinfo=UTC), "100.00"),
                _event(
                    "event-usd",
                    datetime(2026, 8, 4, 9, tzinfo=UTC),
                    "25.00",
                    currency="USD",
                ),
            ]
        )
    )

    rows = aggregate_historical_events(history).orderBy("currency").collect()

    assert [(row.currency, row.event_count, row.total_amount) for row in rows] == [
        ("SAR", 1, Decimal("100.00")),
        ("USD", 1, Decimal("25.00")),
    ]
