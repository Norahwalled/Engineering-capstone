"""Operational command-line entry points for the production data platform."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from capstone_de.logging_config import configure_logging
from capstone_de.settings import Settings, get_settings

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for all runnable platform components."""
    parser = argparse.ArgumentParser(prog="capstone")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate-config", help="Validate runtime configuration.")
    subparsers.add_parser("ensure-topics", help="Create Kafka topics if absent.")
    subparsers.add_parser("consume", help="Run Pydantic validation and quarantine routing.")
    producer_parser = subparsers.add_parser(
        "produce", help="Publish one event JSON document to Kafka."
    )
    producer_parser.add_argument("event_file", type=Path)
    subparsers.add_parser("bronze", help="Run bounded Kafka-to-Bronze streaming.")
    subparsers.add_parser("silver", help="Run business-keyed Silver Delta MERGE.")
    subparsers.add_parser("schema-enforcement", help="Verify Delta rejects incompatible writes.")
    subparsers.add_parser("gold", help="Build Gold aggregates.")
    quality_parser = subparsers.add_parser(
        "quality", help="Run a blocking Great Expectations gate."
    )
    quality_parser.add_argument("layer", choices=("bronze", "silver", "gold"))
    subparsers.add_parser("index", help="Chunk, embed, and index Silver documents.")
    serve_parser = subparsers.add_parser("serve", help="Run the cited RAG API.")
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", default=8080, type=int)
    return parser


def _produce(event_file: Path, settings: Settings) -> None:
    """Read one JSON event file, validate it, and publish it to real Kafka."""
    from capstone_de.domain.events import CustomerEvent
    from capstone_de.infrastructure.kafka import KafkaClient
    from capstone_de.ingestion.producer import EventProducer

    try:
        payload: dict[str, Any] = json.loads(event_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Unable to read event JSON from {event_file}") from error
    EventProducer(KafkaClient(settings), settings).publish(CustomerEvent.model_validate(payload))


def _component_actions(
    settings: Settings, args: argparse.Namespace
) -> dict[str, Callable[[], object]]:
    """Build commands that require an initialized settings object."""
    from capstone_de.airflow.tasks import (
        bronze_ingestion,
        ensure_kafka_topics,
        gold_aggregate,
        quality_gate,
        rag_indexing,
        schema_enforcement,
        silver_merge,
    )
    from capstone_de.infrastructure.kafka import KafkaClient
    from capstone_de.ingestion.validator import ValidationConsumer

    def serve() -> None:
        """Start the RAG HTTP service without loading web dependencies for other commands."""
        import uvicorn

        uvicorn.run("capstone_de.rag.api:app", host=args.host, port=args.port)

    return {
        "ensure-topics": ensure_kafka_topics,
        "consume": lambda: ValidationConsumer(KafkaClient(settings), settings).run_forever(),
        "produce": lambda: _produce(args.event_file, settings),
        "bronze": bronze_ingestion,
        "silver": silver_merge,
        "schema-enforcement": schema_enforcement,
        "gold": gold_aggregate,
        "quality": lambda: quality_gate(args.layer),
        "index": rag_indexing,
        "serve": serve,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Run a platform operation and return an explicit process status code."""
    args = build_parser().parse_args(argv)
    try:
        settings = get_settings()
    except ValidationError as error:
        logging.basicConfig(level=logging.ERROR, force=True)
        LOGGER.error("runtime configuration validation failed", exc_info=error)
        return 2
    configure_logging(settings.log_level)
    if args.command == "validate-config":
        print(
            json.dumps(
                settings.model_dump(mode="json", exclude={"llm_base_url", "llm_api_key"}),
                sort_keys=True,
            )
        )
        return 0
    action = _component_actions(settings, args).get(args.command)
    if action is None:
        LOGGER.error("unhandled command", extra={"command": args.command})
        return 2
    try:
        action()
    except Exception:
        LOGGER.exception("platform command failed", extra={"command": args.command})
        return 1
    LOGGER.info("platform command completed", extra={"command": args.command})
    return 0
