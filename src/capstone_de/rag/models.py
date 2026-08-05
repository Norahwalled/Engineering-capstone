"""Typed RAG domain models preserving source-level citation provenance."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class DocumentChunk(BaseModel):
    """An independently retrievable source fragment."""

    model_config = ConfigDict(extra="forbid")

    chunk_id: str = Field(min_length=1)
    document_id: str = Field(min_length=1)
    source_uri: str = Field(min_length=1)
    title: str = Field(min_length=1)
    chunk_number: int = Field(ge=0)
    text: str = Field(min_length=1)


class RetrievedChunk(DocumentChunk):
    """A chunk with retriever and reranker relevance scores."""

    dense_score: float = 0.0
    lexical_score: float = 0.0
    fused_score: float = 0.0
    rerank_score: float = 0.0


class Citation(BaseModel):
    """A verifiable source reference returned alongside an answer."""

    document_id: str
    chunk_id: str
    source_uri: str
    title: str


class RAGResponse(BaseModel):
    """Grounded answer and the exact source chunks supporting it."""

    answer: str
    citations: list[Citation]
