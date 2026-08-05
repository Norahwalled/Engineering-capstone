"""Delta-to-OpenSearch indexing job with lineage and idempotent source identifiers."""

from __future__ import annotations

import logging

from openlineage.client.run import RunState

from capstone_de.lakehouse.paths import LakehousePaths
from capstone_de.lakehouse.spark import create_spark_session
from capstone_de.lineage.emitter import LineageEmitter
from capstone_de.rag.chunking import chunk_document
from capstone_de.rag.embeddings import EmbeddingService
from capstone_de.rag.opensearch_store import OpenSearchVectorStore
from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)


def index_silver_documents(settings: Settings, lineage: LineageEmitter) -> int:
    """Chunk, embed, and index Silver document content in the real vector database."""
    paths = LakehousePaths.from_settings(settings)
    run_id = lineage.new_run_id()
    lineage.emit(
        RunState.START, "rag_indexing", run_id, [paths.silver], [settings.opensearch_index]
    )
    try:
        rows = (
            create_spark_session(settings, "rag-indexing")
            .read.format("delta")
            .load(paths.silver)
            .where("document_text IS NOT NULL AND trim(document_text) <> ''")
            .select("event_id", "customer_id", "event_type", "document_text")
            .collect()
        )
        embeddings = EmbeddingService(settings)
        store = OpenSearchVectorStore(settings)
        indexed = 0
        for row in rows:
            chunks = chunk_document(
                document_id=str(row.event_id),
                source_uri=f"delta://silver/customer_events/{row.event_id}",
                title=f"{row.event_type} event for customer {row.customer_id}",
                text=str(row.document_text),
            )
            vectors = embeddings.embed([chunk.text for chunk in chunks])
            store.ensure_index(len(vectors[0]))
            for chunk, vector in zip(chunks, vectors, strict=True):
                store.upsert(chunk, vector)
                indexed += 1
    except Exception:
        lineage.emit(
            RunState.FAIL, "rag_indexing", run_id, [paths.silver], [settings.opensearch_index]
        )
        LOGGER.exception("RAG indexing failed")
        raise
    lineage.emit(
        RunState.COMPLETE, "rag_indexing", run_id, [paths.silver], [settings.opensearch_index]
    )
    LOGGER.info("RAG indexing completed", extra={"indexed_chunks": indexed})
    return indexed
