"""Tests for typed runtime configuration."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from capstone_de.settings import Environment, Settings


def test_settings_load_valid_values() -> None:
    """Settings accept supported deployment values."""
    settings = Settings(
        environment=Environment.STAGING,
        log_level="warning",
        service_name="platform-ingestion",
    )

    assert settings.environment is Environment.STAGING
    assert settings.log_level == "WARNING"
    assert settings.service_name == "platform-ingestion"


def test_settings_reject_invalid_log_level() -> None:
    """Settings reject non-standard logging levels."""
    with pytest.raises(ValidationError, match="Unsupported logging level"):
        Settings(log_level="diagnostic")
