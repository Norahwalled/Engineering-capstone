"""OpenSearch vector database adapter supporting dense and BM25 retrieval."""

from __future__ import annotations

import logging
from typing import Any, cast
from urllib.parse import urlparse

from opensearchpy import OpenSearch

from capstone_de.rag.models import DocumentChunk, RetrievedChunk
from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)


class OpenSearchVectorStore:
    """Persists documents in a real OpenSearch index and executes hybrid retrieval primitives."""

    def __init__(self, settings: Settings) -> None:
        parsed = urlparse(settings.opensearch_url)
        if not parsed.hostname:
            raise ValueError("CAPSTONE_OPENSEARCH_URL must contain a hostname")
        self._settings = settings
        self._client = OpenSearch(
            hosts=[{"host": parsed.hostname, "port": parsed.port or 9200}],
            use_ssl=parsed.scheme == "https",
            verify_certs=parsed.scheme == "https",
        )

    def ensure_index(self, vector_dimension: int) -> None:
        """Create the vector/BM25 index with strict mappings if it does not exist."""
        if self._client.indices.exists(index=self._settings.opensearch_index):
            return
        body = {
            "settings": {"index": {"knn": True}},
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "chunk_id": {"type": "keyword"},
                    "document_id": {"type": "keyword"},
                    "source_uri": {"type": "keyword"},
                    "title": {"type": "text"},
                    "chunk_number": {"type": "integer"},
                    "text": {"type": "text"},
                    "embedding": {"type": "knn_vector", "dimension": vector_dimension},
                },
            },
        }
        self._client.indices.create(index=self._settings.opensearch_index, body=body)
        LOGGER.info(
            "created OpenSearch vector index", extra={"index": self._settings.opensearch_index}
        )

    def upsert(self, chunk: DocumentChunk, embedding: list[float]) -> None:
        """Index a chunk idempotently, preserving source metadata for citations."""
        self._client.index(
            index=self._settings.opensearch_index,
            id=chunk.chunk_id,
            body={**chunk.model_dump(), "embedding": embedding},
            refresh="wait_for",
        )

    def dense_search(self, embedding: list[float], limit: int) -> list[RetrievedChunk]:
        """Return semantic candidates using the OpenSearch KNN query."""
        response = self._client.search(
            index=self._settings.opensearch_index,
            body={
                "size": limit,
                "query": {"knn": {"embedding": {"vector": embedding, "k": limit}}},
            },
        )
        return [self._to_retrieved(hit, dense=True) for hit in response["hits"]["hits"]]

    def lexical_search(self, query: str, limit: int) -> list[RetrievedChunk]:
        """Return keyword candidates through OpenSearch's BM25 text ranking."""
        response = self._client.search(
            index=self._settings.opensearch_index,
            body={
                "size": limit,
                "query": {"multi_match": {"query": query, "fields": ["title^2", "text"]}},
            },
        )
        return [self._to_retrieved(hit, dense=False) for hit in response["hits"]["hits"]]

    @staticmethod
    def _to_retrieved(hit: dict[str, object], dense: bool) -> RetrievedChunk:
        source = dict(cast(dict[str, Any], hit["_source"]))
        source.pop("embedding", None)
        source["dense_score" if dense else "lexical_score"] = float(cast(float, hit["_score"]))
        return RetrievedChunk.model_validate(source)
