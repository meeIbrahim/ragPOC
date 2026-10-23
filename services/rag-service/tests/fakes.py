"""In-memory fakes for every port in rag.ports.

Each records its calls in `calls` (name, args) and raises `error` from the next call when one is
set, so tests can check order and inject failures.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence

from rag.ports import Hit, IndexedDocument, Point, Vector


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []
        self.error: BaseException | None = None

    def _record(self, name: str, *args) -> None:
        self.calls.append((name, args))
        if self.error is not None:
            raise self.error

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]


class FakeBlobStore(_Recorder):
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        super().__init__()
        self.objects = dict(objects or {})

    def get(self, key: str, max_bytes: int) -> bytes:
        self._record("get", key, max_bytes)
        return self.objects[key]


class FakeParser(_Recorder):
    def __init__(self, text: str = "") -> None:
        super().__init__()
        self.text = text

    def parse(self, data: bytes) -> str:
        self._record("parse", data)
        return self.text


class FakeEmbedder(_Recorder):
    """Vectors are [len(text), 1.0, 0.0, ...] padded to dim. on_call runs before every embed
    call (e.g. to advance a fake clock past a deadline)."""

    def __init__(self, dim: int = 4, on_call: Callable[[], None] | None = None) -> None:
        super().__init__()
        self.dim = dim
        self.on_call = on_call

    def _vector(self, text: str) -> Vector:
        return ([float(len(text)), 1.0] + [0.0] * self.dim)[: self.dim]

    def embed_documents(self, texts: Sequence[str]) -> list[Vector]:
        if self.on_call:
            self.on_call()
        self._record("embed_documents", list(texts))
        return [self._vector(t) for t in texts]

    def embed_query(self, text: str) -> Vector:
        if self.on_call:
            self.on_call()
        self._record("embed_query", text)
        return self._vector(text)

    def dimension(self) -> int:
        self._record("dimension")
        return self.dim


class FakeIndex(_Recorder):
    """Points keyed by (hash, chunk_index), like the real deterministic ids. search returns
    `hits` (filtered by hash and min_score) so tests control scores."""

    def __init__(self, hits: Sequence[Hit] = (), exists: bool = True) -> None:
        super().__init__()
        self.points: dict[tuple[str, int], Point] = {}
        self.hits = list(hits)
        self.exists = exists

    def delete_by_hash(self, hash: str) -> None:
        self._record("delete_by_hash", hash)
        self.points = {k: p for k, p in self.points.items() if k[0] != hash}

    def upsert(self, points: Sequence[Point]) -> None:
        self._record("upsert", list(points))
        for point in points:
            self.points[(point.hash, point.chunk_index)] = point

    def search(self, vector: Vector, top_k: int, min_score: float, hashes: Sequence[str]) -> list[Hit]:
        self._record("search", vector, top_k, min_score, list(hashes))
        hits = [h for h in self.hits if (not hashes or h.hash in hashes) and h.score >= min_score]
        return sorted(hits, key=lambda h: h.score, reverse=True)[:top_k]

    def collection_exists(self) -> bool:
        self._record("collection_exists")
        return self.exists

    def count(self) -> int:
        self._record("count")
        return len(self.points)

    def clear(self) -> None:
        self._record("clear")
        self.points = {}

    def count_for(self, hash: str) -> int:
        return sum(1 for k in self.points if k[0] == hash)


class FakeLLM(_Recorder):
    """Yields `deltas`; with fail_after=n it raises `failure` after yielding n deltas."""

    def __init__(self, deltas: Sequence[str] = ("an ", "answer"), fail_after: int | None = None,
                 failure: BaseException | None = None) -> None:
        super().__init__()
        self.deltas = list(deltas)
        self.fail_after = fail_after
        self.failure = failure
        self.yielded = 0

    def stream(self, system: str, user: str) -> Iterator[str]:
        self._record("stream", system, user)
        for i, delta in enumerate(self.deltas):
            if self.fail_after is not None and i == self.fail_after:
                raise self.failure or RuntimeError("fake LLM failure")
            self.yielded += 1
            yield delta


class FakeRegistry(_Recorder):
    def __init__(self, indexed: Sequence[str] = ()) -> None:
        super().__init__()
        # Pre-indexed hashes have no row details (None); registered ones keep theirs.
        self.rows: dict[str, IndexedDocument | None] = dict.fromkeys(indexed)

    def exists(self, hash: str) -> bool:
        self._record("exists", hash)
        return hash in self.rows

    def register(self, document: IndexedDocument) -> None:
        self._record("register", document)
        self.rows.setdefault(document.hash, document)

    def count(self) -> int:
        self._record("count")
        return len(self.rows)

    def clear(self) -> None:
        self._record("clear")
        self.rows = {}
