"""Publish a small JSON-array fixture to Kafka one event at a time."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from capstone_de.domain.events import CustomerEvent
from capstone_de.infrastructure.kafka import KafkaClient
from capstone_de.ingestion.producer import EventProducer
from capstone_de.settings import Settings


def load_records(path: Path) -> list[dict[str, Any]]:
    """Load a JSON array containing object records."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Unable to read sample data from {path}") from error
    if not isinstance(value, list) or not all(isinstance(record, dict) for record in value):
        raise ValueError("Sample file must contain a JSON array of objects")
    return value


def build_parser() -> argparse.ArgumentParser:
    """Build arguments for validated or deliberately raw publication."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sample_file", type=Path)
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Bypass producer validation to exercise consumer quarantine handling.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Publish every fixture record and return a process status code."""
    args = build_parser().parse_args(argv)
    records = load_records(args.sample_file)
    settings = Settings()
    kafka = KafkaClient(settings)
    if args.raw:
        for index, record in enumerate(records):
            key = str(record.get("event_id") or f"sample-{index}")
            kafka.publish(settings.kafka_raw_topic, record, key=key)
    else:
        producer = EventProducer(kafka, settings)
        for record in records:
            producer.publish(CustomerEvent.model_validate(record))
    print(f"Published {len(records)} records from {args.sample_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
