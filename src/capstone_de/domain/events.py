"""Versioned event contracts accepted at the Kafka ingestion boundary."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CustomerEvent(BaseModel):
    """A business event used by the lakehouse and RAG document pipeline."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    event_id: UUID
    event_type: str = Field(min_length=3, max_length=80)
    customer_id: str = Field(min_length=1, max_length=80)
    occurred_at: datetime
    amount: Decimal = Field(ge=Decimal("0"), max_digits=16, decimal_places=2)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    document_text: str | None = Field(default=None, max_length=20_000)
    schema_version: str = Field(pattern=r"^1\.0$")

    @field_validator("occurred_at")
    @classmethod
    def require_timezone_aware_timestamp(cls, value: datetime) -> datetime:
        """Reject timestamps that cannot be ordered consistently across regions."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone offset")
        return value.astimezone(UTC)
