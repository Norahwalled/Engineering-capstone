"""Centralized structured logging configuration for platform processes."""

from __future__ import annotations

import logging
from typing import Final

LOG_FORMAT: Final[str] = "%(asctime)s %(levelname)s %(name)s %(message)s"


def configure_logging(log_level: str) -> None:
    """Configure process logging with a stable, operations-friendly format."""
    logging.basicConfig(level=log_level, format=LOG_FORMAT, force=True)
