"""Retrieval-augmented answers (PRD 4.4, 5.1; spec R6). Knows nothing about gRPC.

ask_stream validates and retrieves before it returns, so a bad query or a retrieval outage is an
error before any message. The answer is generated lazily as the caller iterates: message 1
carries the sources and an empty answer, then only answer deltas follow (PRD L16, spec D9).
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field

from rag.ports import DependencyUnavailable, Embedder, Hit, LLM, VectorIndex

NO_RELEVANT_ANSWER = "No relevant information was found in the indexed documents."


class InvalidQuery(Exception):
    """The request itself is wrong (empty query, over a limit)."""


class QueryUnavailable(Exception):
    """Ollama, Qdrant or Groq failed. The message names the service, never document text."""


@dataclass(frozen=True)
class QuerySettings:
    top_k: int
    min_score: float
    max_query_chars: int
    max_document_ids: int
    system_prompt: str


@dataclass(frozen=True)
class Message:
    """One streamed message: sources (message 1 only) or an answer delta."""

    sources: list[Hit] = field(default_factory=list)
    answer: str = ""


def build_user_message(query: str, hits: Sequence[Hit]) -> str:
    """Numbered context blocks, so the model can cite them as [n], then the question."""
    blocks = "\n\n".join(f"[{n}] ({hit.filename})\n{hit.text}" for n, hit in enumerate(hits, start=1))
    return f"Context:\n\n{blocks}\n\nQuestion: {query}"


class QueryService:
    def __init__(self, embedder: Embedder, index: VectorIndex, llm: LLM, settings: QuerySettings) -> None:
        self._embedder = embedder
        self._index = index
        self._llm = llm
        self._settings = settings

    def retrieve(self, query: str, document_ids: Sequence[str], min_score: float | None = None) -> list[Hit]:
        """Validated retrieval, best first. min_score overrides the configured threshold (the eval
        uses 0 to see every score)."""
        self._validate(query, document_ids)
        try:
            vector = self._embedder.embed_query(query)
            hits = self._index.search(
                vector,
                self._settings.top_k,
                self._settings.min_score if min_score is None else min_score,
                list(document_ids),
            )
        except DependencyUnavailable as exc:
            raise QueryUnavailable(str(exc)) from exc
        return sorted(hits, key=lambda hit: hit.score, reverse=True)

    def ask_stream(self, query: str, document_ids: Sequence[str]) -> Iterator[Message]:
        hits = self.retrieve(query, document_ids)
        return self._answer(query, hits)

    def ask(self, query: str, document_ids: Sequence[str]) -> tuple[str, list[Hit]]:
        messages = list(self.ask_stream(query, document_ids))
        return "".join(m.answer for m in messages), messages[0].sources

    def _answer(self, query: str, hits: list[Hit]) -> Iterator[Message]:
        yield Message(sources=hits)
        if not hits:
            # Nothing passed the threshold: say so without calling Groq, so no text leaves (D1).
            yield Message(answer=NO_RELEVANT_ANSWER)
            return
        try:
            for delta in self._llm.stream(self._settings.system_prompt, build_user_message(query, hits)):
                yield Message(answer=delta)
        except DependencyUnavailable as exc:
            raise QueryUnavailable(str(exc)) from exc

    def _validate(self, query: str, document_ids: Sequence[str]) -> None:
        if not query.strip():
            raise InvalidQuery("query is empty")
        if len(query) > self._settings.max_query_chars:
            raise InvalidQuery(f"query is longer than {self._settings.max_query_chars} characters")
        if len(document_ids) > self._settings.max_document_ids:
            raise InvalidQuery(f"more than {self._settings.max_document_ids} document_ids")
