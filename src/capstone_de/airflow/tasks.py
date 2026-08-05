"""Thin Airflow-callable adapters around independently executable application jobs."""

from __future__ import annotations

from openlineage.client.run import RunState

from capstone_de.infrastructure.kafka import KafkaClient
from capstone_de.lakehouse.bronze import start_bronze_stream
from capstone_de.lakehouse.gold import build_gold_aggregates
from capstone_de.lakehouse.schema_enforcement import verify_schema_enforcement
from capstone_de.lakehouse.silver import merge_silver
from capstone_de.lineage.emitter import LineageEmitter
from capstone_de.quality.gates import run_quality_gate
from capstone_de.rag.indexer import index_silver_documents
from capstone_de.settings import get_settings


def ensure_kafka_topics() -> None:
    """Create required real Kafka topics before data producers and consumers start."""
    settings = get_settings()
    lineage = LineageEmitter(settings)
    run_id = lineage.new_run_id()
    topics = [
        settings.kafka_raw_topic,
        settings.kafka_validated_topic,
        settings.kafka_quarantine_topic,
    ]
    lineage.emit(RunState.START, "ensure_kafka_topics", run_id, [], topics)
    try:
        KafkaClient(settings).ensure_topics()
    except Exception:
        lineage.emit(RunState.FAIL, "ensure_kafka_topics", run_id, [], topics)
        raise
    lineage.emit(RunState.COMPLETE, "ensure_kafka_topics", run_id, [], topics)


def bronze_ingestion() -> None:
    """Execute bounded Kafka-to-Bronze streaming ingestion."""
    settings = get_settings()
    start_bronze_stream(settings, LineageEmitter(settings))


def quality_gate(layer: str) -> None:
    """Execute a blocking Great Expectations quality validation for one layer."""
    settings = get_settings()
    run_quality_gate(layer, settings, LineageEmitter(settings))


def silver_merge() -> None:
    """Execute business-keyed Silver Delta upsert."""
    settings = get_settings()
    merge_silver(settings, LineageEmitter(settings))


def schema_enforcement() -> None:
    """Execute the required Delta incompatible-schema rejection proof."""
    settings = get_settings()
    verify_schema_enforcement(settings, LineageEmitter(settings))


def gold_aggregate() -> None:
    """Build the independent Gold aggregate data product."""
    settings = get_settings()
    build_gold_aggregates(settings, LineageEmitter(settings))


def rag_indexing() -> int:
    """Index Silver documents into the real OpenSearch vector database."""
    settings = get_settings()
    return index_silver_documents(settings, LineageEmitter(settings))
