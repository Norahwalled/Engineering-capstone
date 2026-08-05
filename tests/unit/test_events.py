"""Unit tests for ingress contracts that protect the Kafka boundary."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from capstone_de.domain.events import CustomerEvent


def valid_event() -> dict[str, object]:
    """Return a valid raw event payload representative of source-system input."""
    return {
        "event_id": str(uuid4()),
        "event_type": "support_case",
        "customer_id": "customer-001",
        "occurred_at": datetime.now(UTC).isoformat(),
        "amount": str(Decimal("10.50")),
        "currency": "SAR",
        "document_text": "The customer asked about their monthly invoice.",
        "schema_version": "1.0",
    }


def test_event_contract_accepts_valid_timezone_aware_payload() -> None:
    """The Pydantic contract accepts a complete correctly typed business event."""
    event = CustomerEvent.model_validate(valid_event())

    assert event.currency == "SAR"
    assert event.occurred_at.tzinfo is UTC


def test_event_contract_rejects_invalid_currency() -> None:
    """The Pydantic contract rejects values that violate the data contract."""
    payload = valid_event()
    payload["currency"] = "saudi-riyal"

    with pytest.raises(ValidationError):
        CustomerEvent.model_validate(payload)
