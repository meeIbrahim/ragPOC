"""Embedder over the host Ollama (PRD L7: qwen3-embedding)."""

from __future__ import annotations

import logging
from collections.abc import Sequence

import httpx
from ollama import Client, ResponseError

from rag.ports import DependencyUnavailable, ModelNotAvailable, Vector

logger = logging.getLogger(__name__)

_SERVICE = "ollama"
_PROBE = "dimension probe"


class OllamaEmbedder:
    def __init__(self, client: Client, model: str, query_instruction: str) -> None:
        self._client = client
        self._model = model
        self._query_instruction = query_instruction

    def _embed(self, texts: Sequence[str]) -> list[Vector]:
        try:
            response = self._client.embed(model=self._model, input=list(texts))
        except (ConnectionError, httpx.HTTPError, ResponseError) as exc:
            # Every Ollama error is retryable here, 4xx included: chunks are bounded by
            # chunk_size and Ollama truncates long input by default, so a 4xx points at the
            # server (e.g. a 404 when the model was removed under a running service), and
            # max_attempts / poison bound the retries. At startup dimension() reports a 404.
            raise DependencyUnavailable(_SERVICE, _describe(exc)) from exc
        vectors = [list(v) for v in response.embeddings]
        if len(vectors) != len(texts):
            raise DependencyUnavailable(_SERVICE, f"{len(vectors)} vectors for {len(texts)} inputs")
        return vectors

    def embed_documents(self, texts: Sequence[str]) -> list[Vector]:
        if not texts:
            return []
        return self._embed(texts)

    def embed_query(self, text: str) -> Vector:
        # The exact format from the Qwen3-Embedding model card (no space after "Query:");
        # documents are embedded without an instruction (spec D2).
        return self._embed([f"Instruct: {self._query_instruction}\nQuery:{text}"])[0]

    def dimension(self) -> int:
        try:
            return len(self._embed([_PROBE])[0])
        except DependencyUnavailable as exc:
            cause = exc.__cause__
            if isinstance(cause, ResponseError) and cause.status_code == 404:
                raise ModelNotAvailable(self._model) from None
            raise


def _describe(exc: Exception) -> str:
    if isinstance(exc, ResponseError):
        return f"HTTP {exc.status_code}"
    return type(exc).__name__
