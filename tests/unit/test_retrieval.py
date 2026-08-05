"""Unit tests for the deterministic Reciprocal Rank Fusion algorithm."""

from __future__ import annotations

from capstone_de.rag.models import RetrievedChunk
from capstone_de.rag.retrieval import reciprocal_rank_fusion


def _chunk(chunk_id: str) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        document_id=f"document-{chunk_id}",
        source_uri=f"https://example.test/{chunk_id}",
        title=chunk_id,
        chunk_number=0,
        text=f"Text for {chunk_id}",
    )


def test_rrf_promotes_chunks_returned_by_multiple_retrievers() -> None:
    """A shared candidate receives the combined RRF score from dense and BM25 rankings."""
    fused = reciprocal_rank_fusion([[_chunk("a"), _chunk("b")], [_chunk("b"), _chunk("c")]])

    assert fused[0].chunk_id == "b"
    assert fused[0].fused_score > fused[1].fused_score
