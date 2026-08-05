"""Unit tests for deterministic RAG document chunking."""

from __future__ import annotations

from capstone_de.rag.chunking import chunk_document


def test_chunking_preserves_source_provenance_and_stable_ids() -> None:
    """Chunking produces deterministic chunks that preserve citation metadata."""
    text = " ".join(["Invoice policy details."] * 100)

    first = chunk_document(
        "document-1", "https://example.test/invoice", "Invoice Policy", text, 100, 20
    )
    second = chunk_document(
        "document-1", "https://example.test/invoice", "Invoice Policy", text, 100, 20
    )

    assert len(first) > 1
    assert [chunk.chunk_id for chunk in first] == [chunk.chunk_id for chunk in second]
    assert {chunk.source_uri for chunk in first} == {"https://example.test/invoice"}
