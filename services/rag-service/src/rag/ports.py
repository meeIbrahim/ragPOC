"""The interfaces the ingest and query services depend on (PRD 5.1).

The services know nothing about MinIO, Ollama, Qdrant, Groq or SQLite: each sits behind one of
these protocols, with an adapter in rag.adapters and a fake in tests/fakes.py.

Failure contract:
- The embedder, the vector index and the LLM are shared by ingest and query, so they raise the
  neutral DependencyUnavailable when the service behind them is down, too slow, or erroring.
  Each service maps it to its own error (ingest: TransientError DEPENDENCY_UNAVAILABLE; query:
  QueryUnavailable).
- The blob store and the parsers serve ingest only and raise rag.ingest.errors types directly.
- No message ever contains document text.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

Vector = list[float]


class DependencyUnavailable(Exception):
    """An external service failed in a way a retry may fix (down, timed out, 5xx)."""

    def __init__(self, service: str, detail: str) -> None:
        self.service = service
        self.detail = detail
        super().__init__(f"{service}: {detail}")


class ModelNotAvailable(Exception):
    """The embedding model is not installed where the embedder runs. Only a setup change helps."""

    def __init__(self, model: str) -> None:
        self.model = model
        super().__init__(model)


def extension(filename: str) -> str:
    """The lowercase extension: ".PDF" -> ".pdf"; "notes" and ".pdf" (a dotfile) -> ""."""
    return os.path.splitext(filename)[1].lower()


@dataclass(frozen=True)
class Point:
    """One chunk to store. Its payload is exactly hash, filename, chunk_index and text (PRD 5.2)."""

    hash: str
    filename: str
    chunk_index: int
    text: str
    vector: Vector


@dataclass(frozen=True)
class Hit:
    """One retrieved chunk, with its similarity score."""

    hash: str
    filename: str
    chunk_index: int
    text: str
    score: float


@dataclass(frozen=True)
class IndexedDocument:
    """A row of indexed_documents: its presence means the document is fully indexed."""

    hash: str
    filename: str
    object_path: str
    chunk_count: int
    indexed_at: datetime


class BlobStore(Protocol):
    def get(self, key: str, max_bytes: int) -> bytes:
        """The object's bytes. More than max_bytes raises PermanentError (file too large)."""
        ...


class Parser(Protocol):
    def parse(self, data: bytes) -> str:
        """The document's text. Unreadable input raises PermanentError(CORRUPT_FILE)."""
        ...


class ParserFor(Protocol):
    def __call__(self, filename: str) -> Parser:
        """The parser for the filename's extension; unsupported ones raise PermanentError."""
        ...


class Embedder(Protocol):
    def embed_documents(self, texts: Sequence[str]) -> list[Vector]: ...

    def embed_query(self, text: str) -> Vector: ...

    def dimension(self) -> int:
        """The model's output size, measured by embedding a probe string. Raises
        ModelNotAvailable when the model is not installed."""
        ...


class VectorIndex(Protocol):
    def delete_by_hash(self, hash: str) -> None: ...

    def upsert(self, points: Sequence[Point]) -> None:
        """Idempotent: a point's id is derived from (hash, chunk_index)."""
        ...

    def search(self, vector: Vector, top_k: int, min_score: float, hashes: Sequence[str]) -> list[Hit]:
        """Best hits first. min_score 0 means no threshold; empty hashes means no filter."""
        ...

    def collection_exists(self) -> bool: ...

    def count(self) -> int: ...

    def clear(self) -> None:
        """Delete every point; keep the collection and its payload index."""
        ...


class LLM(Protocol):
    def stream(self, system: str, user: str) -> Iterator[str]:
        """Answer text deltas only; reasoning is never yielded (PRD L16)."""
        ...


class IndexRegistry(Protocol):
    def exists(self, hash: str) -> bool: ...

    def register(self, document: IndexedDocument) -> None:
        """Insert the row; an existing row for the hash is kept as is."""
        ...

    def count(self) -> int: ...

    def clear(self) -> None: ...
