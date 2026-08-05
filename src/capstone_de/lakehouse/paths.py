"""Single source of truth for persisted Delta Lake dataset paths."""

from __future__ import annotations

from dataclasses import dataclass

from capstone_de.settings import Settings


@dataclass(frozen=True, slots=True)
class LakehousePaths:
    """Fully qualified Delta table locations."""

    bronze: str
    silver: str
    silver_current: str
    gold: str
    checkpoints: str

    @classmethod
    def from_settings(cls, settings: Settings) -> LakehousePaths:
        """Derive all locations from the configured Delta root."""
        root = settings.delta_base_path.rstrip("/")
        return cls(
            bronze=f"{root}/bronze/customer_events",
            silver=f"{root}/silver/customer_events",
            silver_current=f"{root}/silver/customer_event_current",
            gold=f"{root}/gold/customer_daily_metrics",
            checkpoints=f"{root}/checkpoints",
        )
