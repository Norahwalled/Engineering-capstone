"""FastAPI surface for the fully grounded and cited RAG service."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from capstone_de.rag.embeddings import EmbeddingService
from capstone_de.rag.models import RAGResponse
from capstone_de.rag.opensearch_store import OpenSearchVectorStore
from capstone_de.rag.retrieval import HybridRetriever
from capstone_de.rag.service import GroundedAnswerService
from capstone_de.settings import get_settings

app = FastAPI(title="Modern Data Engineering RAG API", version="0.1.0")


class QuestionRequest(BaseModel):
    """Request model for a user question."""

    question: str = Field(min_length=1, max_length=4_000)


def _service() -> GroundedAnswerService:
    settings = get_settings()
    retriever = HybridRetriever(
        OpenSearchVectorStore(settings),
        EmbeddingService(settings),
        reranker_model=settings.reranker_model,
    )
    return GroundedAnswerService(settings, retriever)


@app.get("/health")
def health() -> dict[str, str]:
    """Expose a lightweight service-health response."""
    return {"status": "ok"}


@app.post("/v1/answer", response_model=RAGResponse)
def answer_question(request: QuestionRequest) -> RAGResponse:
    """Retrieve evidence, rerank it, and return a grounded answer with citations."""
    try:
        return _service().answer(request.question)
    except (RuntimeError, ValueError) as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
