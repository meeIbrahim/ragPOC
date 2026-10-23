"""The real ingest pipeline (PRD 5.2, spec R4): it implements the Processor seam the stub used.

It raises typed errors only; run_job() in rag.ingest.outcome turns them into the status outcome.
It knows nothing about Redis: the stream consumer publishes the outcome and acks.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from rag.adapters.parsers import extension, parser_for
from rag.ingest import chunking
from rag.ingest.errors import PermanentError, TransientError
from rag.ingest.outcome import Indexed, Outcome
from rag.ports import (
    BlobStore,
    DependencyUnavailable,
    Embedder,
    IndexedDocument,
    IndexRegistry,
    Parser,
    Point,
    VectorIndex,
)
from rag.transport.status_event import IngestEvent, Reason
from rag.transport.watchdog import Deadline


@dataclass(frozen=True)
class IngestSettings:
    chunk_size: int
    chunk_overlap: int
    embed_batch_size: int
    max_file_bytes: int


class IngestService:
    def __init__(
        self,
        registry: IndexRegistry,
        blobs: BlobStore,
        embedder: Embedder,
        index: VectorIndex,
        settings: IngestSettings,
        parser_for: Callable[[str], Parser] = parser_for,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._registry = registry
        self._blobs = blobs
        self._embedder = embedder
        self._index = index
        self._settings = settings
        self._parser_for = parser_for
        self._now = now

    def process(self, event: IngestEvent, deadline: Deadline) -> Outcome:
        # A row means the document is completely indexed: a redelivery or a second upload of the
        # same content reports INDEXED again without redoing the work (PRD 5.2.1).
        if self._registry.exists(event.hash):
            return Indexed(chunk_count=None)

        # Both checks come before the fetch, so an unsupported or oversized file costs nothing.
        parser = self._parser_for(event.filename)
        if event.size > self._settings.max_file_bytes:
            raise PermanentError(Reason.UNSUPPORTED_TYPE, f"file too large (over {self._settings.max_file_bytes} bytes)")

        deadline.check()
        data = self._blobs.get(event.bucket_path, self._settings.max_file_bytes)
        deadline.check()
        text = parser.parse(data)
        if not text.strip():
            raise PermanentError(Reason.EMPTY_TEXT, "no extractable text")
        chunks = chunking.split(text, extension(event.filename), self._settings.chunk_size, self._settings.chunk_overlap)
        if not chunks:
            raise PermanentError(Reason.EMPTY_TEXT, "no extractable text")

        try:
            vectors = []
            for start in range(0, len(chunks), self._settings.embed_batch_size):
                deadline.check()
                vectors.extend(self._embedder.embed_documents(chunks[start : start + self._settings.embed_batch_size]))
            deadline.check()
            points = [
                Point(event.hash, event.filename, i, chunk, vector)
                for i, (chunk, vector) in enumerate(zip(chunks, vectors, strict=True))
            ]
            # Delete first: a re-ingest with fewer chunks must not leave the old tail behind.
            self._index.delete_by_hash(event.hash)
            self._index.upsert(points)
        except DependencyUnavailable as exc:
            raise TransientError(Reason.DEPENDENCY_UNAVAILABLE, str(exc)) from exc

        # No deadline check here: the expensive work is done, and registering is one row.
        self._registry.register(
            IndexedDocument(event.hash, event.filename, event.bucket_path, len(points), self._now())
        )
        return Indexed(chunk_count=len(points))
