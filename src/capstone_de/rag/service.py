"""Grounded answer generation using retrieved context and deterministic citations."""

from __future__ import annotations

import logging

import httpx

from capstone_de.rag.models import Citation, RAGResponse
from capstone_de.rag.retrieval import HybridRetriever
from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)


class GroundedAnswerService:
    """Generates answers from hybrid-retrieved evidence and returns its source citations."""

    def __init__(self, settings: Settings, retriever: HybridRetriever) -> None:
        self._settings = settings
        self._retriever = retriever

    def answer(self, question: str) -> RAGResponse:
        """Answer from retrieved chunks and return citations for the supplied evidence."""
        chunks = self._retriever.retrieve(question)
        if not chunks:
            return RAGResponse(answer="Insufficient context to answer the question.", citations=[])
        context = "\n\n".join(f"[{index + 1}] {chunk.text}" for index, chunk in enumerate(chunks))
        payload = {
            "model": self._settings.llm_model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Answer only from the supplied context. "
                        "State insufficient context when unsupported."
                    ),
                },
                {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {question}"},
            ],
            "temperature": 0,
        }
        headers = {"Content-Type": "application/json"}
        api_key = (
            self._settings.llm_api_key.get_secret_value().strip()
            if self._settings.llm_api_key is not None
            else ""
        )
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        try:
            response = httpx.post(
                f"{self._settings.llm_base_url.rstrip('/')}/v1/chat/completions",
                json=payload,
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            answer = response.json()["choices"][0]["message"]["content"].strip()
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as error:
            LOGGER.exception("grounded answer generation failed")
            raise RuntimeError("LLM did not return a valid grounded answer") from error
        citations = [
            Citation(
                document_id=chunk.document_id,
                chunk_id=chunk.chunk_id,
                source_uri=chunk.source_uri,
                title=chunk.title,
            )
            for chunk in chunks
        ]
        return RAGResponse(answer=answer, citations=citations)
