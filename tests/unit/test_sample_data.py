"""Contract checks for the lightweight synthetic GitHub sample data."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError
from scripts.produce_sample_events import load_records

from capstone_de.domain.events import CustomerEvent

SAMPLES = Path(__file__).parents[2] / "data" / "samples"


def test_valid_late_and_duplicate_samples_satisfy_the_event_contract() -> None:
    """Every fixture intended for normal ingestion must pass Pydantic validation."""
    for filename in ("valid_events.json", "late_events.json", "duplicate_events.json"):
        records = load_records(SAMPLES / filename)
        assert records
        assert all(CustomerEvent.model_validate(record) for record in records)


def test_invalid_samples_are_rejected_by_the_event_contract() -> None:
    """Every quarantine fixture must violate at least one contract rule."""
    for record in load_records(SAMPLES / "invalid_events.json"):
        with pytest.raises(ValidationError):
            CustomerEvent.model_validate(record)


def test_duplicate_samples_replay_known_event_ids() -> None:
    """Duplicate fixtures must reference IDs already present in the valid batch."""
    valid_ids = {record["event_id"] for record in load_records(SAMPLES / "valid_events.json")}
    duplicate_ids = {
        record["event_id"] for record in load_records(SAMPLES / "duplicate_events.json")
    }

    assert duplicate_ids
    assert duplicate_ids < valid_ids
