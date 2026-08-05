"""Shared message envelope contracts for reliable pipeline hand-offs."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class KafkaMetadata(BaseModel):
    """Source coordinates needed for traceability and replay."""

    model_config = ConfigDict(extra="forbid")

    topic: str = Field(min_length=1)
    partition: int = Field(ge=0)
    offset: int = Field(ge=0)
    received_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class QuarantineRecord(BaseModel):
    """Rejected input retained with its exact reason and Kafka provenance."""

    model_config = ConfigDict(extra="forbid")

    payload: dict[str, Any]
    rejection_reason: str = Field(min_length=1)
    source: KafkaMetadata
    rejected_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
