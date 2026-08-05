"""Deterministic document chunking for repeatable indexing and citations."""

from __future__ import annotations

import hashlib

from capstone_de.rag.models import DocumentChunk


def chunk_document(
    document_id: str,
    source_uri: str,
    title: str,
    text: str,
    chunk_size: int = 800,
    overlap: int = 120,
) -> list[DocumentChunk]:
    """Split normalized text into overlapping chunks with stable content-derived IDs."""
    if chunk_size <= 0 or overlap < 0 or overlap >= chunk_size:
        raise ValueError("chunk_size must be positive and overlap must be smaller than chunk_size")
    normalized = " ".join(text.split())
    if not normalized:
        raise ValueError("document text cannot be empty")
    chunks: list[DocumentChunk] = []
    start = 0
    number = 0
    while start < len(normalized):
        end = min(start + chunk_size, len(normalized))
        if end < len(normalized):
            boundary = normalized.rfind(" ", start, end)
            if boundary > start:
                end = boundary
        segment = normalized[start:end]
        digest = hashlib.sha256(f"{document_id}:{number}:{segment}".encode()).hexdigest()
        chunks.append(
            DocumentChunk(
                chunk_id=digest,
                document_id=document_id,
                source_uri=source_uri,
                title=title,
                chunk_number=number,
                text=segment,
            )
        )
        number += 1
        start = end - overlap if end < len(normalized) else end
    return chunks
