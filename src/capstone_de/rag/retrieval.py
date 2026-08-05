"""Hybrid search, Reciprocal Rank Fusion, and cross-encoder reranking."""

from __future__ import annotations

from collections import defaultdict

from sentence_transformers import CrossEncoder

from capstone_de.rag.embeddings import EmbeddingService
from capstone_de.rag.models import RetrievedChunk
from capstone_de.rag.opensearch_store import OpenSearchVectorStore


def reciprocal_rank_fusion(
    result_sets: list[list[RetrievedChunk]], constant: int = 60
) -> list[RetrievedChunk]:
    """Fuse independently ranked retrieval lists using the standard RRF formula."""
    if constant <= 0:
        raise ValueError("RRF constant must be positive")
    by_id: dict[str, RetrievedChunk] = {}
    scores: defaultdict[str, float] = defaultdict(float)
    for result_set in result_sets:
        for rank, result in enumerate(result_set, start=1):
            by_id[result.chunk_id] = result
            scores[result.chunk_id] += 1.0 / (constant + rank)
    return [
        by_id[chunk_id].model_copy(update={"fused_score": score})
        for chunk_id, score in sorted(scores.items(), key=lambda item: item[1], reverse=True)
    ]


class HybridRetriever:
    """Combines dense vector retrieval, BM25, RRF, and cross-encoder reranking."""

    def __init__(
        self, store: OpenSearchVectorStore, embeddings: EmbeddingService, reranker_model: str
    ) -> None:
        self._store = store
        self._embeddings = embeddings
        self._reranker = CrossEncoder(reranker_model)

    def retrieve(self, query: str, candidates: int = 20, limit: int = 5) -> list[RetrievedChunk]:
        """Return cross-encoder-reranked results from a real hybrid search execution."""
        if not query.strip():
            raise ValueError("query cannot be empty")
        dense = self._store.dense_search(self._embeddings.embed([query])[0], candidates)
        lexical = self._store.lexical_search(query, candidates)
        fused = reciprocal_rank_fusion([dense, lexical])[:candidates]
        scores = self._reranker.predict([(query, result.text) for result in fused])
        reranked = [
            result.model_copy(update={"rerank_score": float(score)})
            for result, score in zip(fused, scores, strict=True)
        ]
        return sorted(reranked, key=lambda result: result.rerank_score, reverse=True)[:limit]
