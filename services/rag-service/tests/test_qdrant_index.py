"""R2 QdrantIndex: deterministic ids (L13), payload, filter, threshold, errors (spec E2, E3, E9)."""

import os
import uuid
from types import SimpleNamespace

import httpx
import pytest
from qdrant_client import models
from qdrant_client.http.exceptions import ResponseHandlingException

from rag.adapters.qdrant_index import NAMESPACE, QdrantIndex, point_id
from rag.ports import DependencyUnavailable, Hit, Point

H1, H2 = "a" * 64, "b" * 64


def point(hash=H1, i=0, text="t", vector=(0.1, 0.2)):
    return Point(hash, "guide.pdf", i, text, list(vector))


class FakeClient:
    def __init__(self, error=None, scored=()):
        self.error = error
        self.scored = list(scored)
        self.calls = []

    def _call(self, name, **kwargs):
        self.calls.append((name, kwargs))
        if self.error:
            raise self.error

    def upsert(self, **kwargs):
        self._call("upsert", **kwargs)

    def delete(self, **kwargs):
        self._call("delete", **kwargs)

    def query_points(self, **kwargs):
        self._call("query_points", **kwargs)
        return SimpleNamespace(points=self.scored)

    def collection_exists(self, **kwargs):
        self._call("collection_exists", **kwargs)
        return True

    def count(self, **kwargs):
        self._call("count", **kwargs)
        return SimpleNamespace(count=7)


def index(client, batch=2):
    return QdrantIndex(client, collection="c", upsert_batch=batch)


def test_point_ids_are_uuid5_of_hash_and_index():
    assert point_id(H1, 3) == str(uuid.uuid5(NAMESPACE, f"{H1}:3"))
    assert point_id(H1, 3) == point_id(H1, 3)
    assert point_id(H1, 3) != point_id(H1, 4) != point_id(H2, 3)


def test_upsert_writes_exactly_the_four_payload_keys_in_batches():
    client = FakeClient()
    index(client, batch=2).upsert([point(i=0), point(i=1), point(i=2)])
    batches = [kwargs["points"] for name, kwargs in client.calls]
    assert [len(b) for b in batches] == [2, 1]
    first = batches[0][0]
    assert first.id == point_id(H1, 0)
    assert first.payload == {"hash": H1, "filename": "guide.pdf", "chunk_index": 0, "text": "t"}
    assert all(kwargs["wait"] is True and kwargs["collection_name"] == "c" for _, kwargs in client.calls)


def test_delete_by_hash_filters_on_the_hash_payload():
    client = FakeClient()
    index(client).delete_by_hash(H1)
    (name, kwargs), = client.calls
    condition = kwargs["points_selector"].filter.must[0]
    assert (name, condition.key, condition.match.value, kwargs["wait"]) == ("delete", "hash", H1, True)


def test_search_maps_hits_and_passes_no_filter_or_threshold_by_default():
    scored = [SimpleNamespace(score=0.9, payload={"hash": H1, "filename": "a.md", "chunk_index": 2, "text": "x"})]
    client = FakeClient(scored=scored)
    hits = index(client).search([0.1], top_k=5, min_score=0.0, hashes=[])
    assert hits == [Hit(H1, "a.md", 2, "x", 0.9)]
    kwargs = client.calls[0][1]
    assert kwargs["limit"] == 5 and kwargs["query_filter"] is None and kwargs["score_threshold"] is None
    assert kwargs["with_payload"] is True


def test_search_filters_on_hashes_and_applies_the_threshold():
    client = FakeClient()
    index(client).search([0.1], top_k=3, min_score=0.4, hashes=[H1, H2])
    kwargs = client.calls[0][1]
    condition = kwargs["query_filter"].must[0]
    assert condition.key == "hash" and condition.match.any == [H1, H2]
    assert kwargs["score_threshold"] == 0.4


def test_count_is_exact():
    client = FakeClient()
    assert index(client).count() == 7
    assert client.calls[0][1]["exact"] is True


@pytest.mark.parametrize(
    "error",
    [ResponseHandlingException(httpx.ConnectError("refused")), httpx.ReadTimeout("slow"), ConnectionError("down")],
    ids=["response-handling", "timeout", "connection"],
)
@pytest.mark.parametrize("op", ["upsert", "delete", "search", "count", "exists"])
def test_failures_are_dependency_unavailable(error, op):
    idx = index(FakeClient(error=error))
    calls = {
        "upsert": lambda: idx.upsert([point()]),
        "delete": lambda: idx.delete_by_hash(H1),
        "search": lambda: idx.search([0.1], 5, 0.0, []),
        "count": idx.count,
        "exists": idx.collection_exists,
    }
    with pytest.raises(DependencyUnavailable):
        calls[op]()


# ---- integration: a real Qdrant (QDRANT_URL, e.g. http://localhost:16333)


@pytest.fixture
def real_index():
    from qdrant_client import QdrantClient

    url = os.environ.get("QDRANT_URL")
    if not url:
        pytest.skip("set QDRANT_URL")
    client = QdrantClient(url=url, timeout=10)
    name = "p1-it-" + uuid.uuid4().hex[:8]
    client.create_collection(name, vectors_config=models.VectorParams(size=2, distance=models.Distance.COSINE))
    client.create_payload_index(name, "hash", models.PayloadSchemaType.KEYWORD, wait=True)
    try:
        yield QdrantIndex(client, collection=name, upsert_batch=2)
    finally:
        client.delete_collection(name)


@pytest.mark.integration
def test_reupserting_the_same_chunks_does_not_grow_the_collection(real_index):
    points = [point(H1, i, f"t{i}", (1.0, i / 10)) for i in range(3)]
    real_index.upsert(points)
    real_index.upsert(points)
    assert real_index.count() == 3


@pytest.mark.integration
def test_delete_by_hash_removes_only_that_document(real_index):
    real_index.upsert([point(H1, 0), point(H1, 1), point(H2, 0)])
    real_index.delete_by_hash(H1)
    assert real_index.count() == 1


@pytest.mark.integration
def test_filter_threshold_and_clear(real_index):
    real_index.upsert([point(H1, 0, vector=(1.0, 0.0)), point(H2, 0, vector=(0.0, 1.0))])
    assert {h.hash for h in real_index.search([1.0, 0.0], 5, 0.0, [H2])} == {H2}
    assert [h.hash for h in real_index.search([1.0, 0.0], 5, 0.5, [])] == [H1]
    assert real_index.collection_exists() is True
    real_index.clear()
    assert real_index.count() == 0
    assert real_index.collection_exists() is True
