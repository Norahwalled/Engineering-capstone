"""Embedding-model adapter used by both OpenSearch indexing and retrieval."""

from __future__ import annotations

import logging
from functools import lru_cache

from sentence_transformers import SentenceTransformer

from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _model(model_name: str) -> SentenceTransformer:
    """Load one configured embedding model per process."""
    LOGGER.info("loading embedding model", extra={"model": model_name})
    return SentenceTransformer(model_name)


class EmbeddingService:
    """Produces normalized dense vectors from the configured embedding model."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Return a vector per non-empty input string."""
        if not texts or any(not text.strip() for text in texts):
            raise ValueError("Embedding input must contain one or more non-empty texts")
        vectors = _model(self._settings.embedding_model).encode(
            texts,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return [vector.astype(float).tolist() for vector in vectors]
