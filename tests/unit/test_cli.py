"""Tests for the operational command-line interface."""

from __future__ import annotations

import json

import pytest

from capstone_de.cli import main
from capstone_de.settings import get_settings


def test_validate_config_reports_valid_status(capsys: pytest.CaptureFixture[str]) -> None:
    """The validation command reports a validated default configuration."""
    exit_code = main(["validate-config"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["service_name"] == "modern-data-engineering-ai"
    assert payload["environment"] == "development"


def test_validate_config_returns_error_for_invalid_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The validation command returns a failure status for invalid configuration."""
    get_settings.cache_clear()
    monkeypatch.setenv("CAPSTONE_LOG_LEVEL", "not-a-log-level")

    exit_code = main(["validate-config"])

    assert exit_code == 2
    get_settings.cache_clear()
