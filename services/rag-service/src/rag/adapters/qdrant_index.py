"""VectorIndex over a Qdrant collection (PRD L13, 5.2)."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence

import httpx
from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import ApiException

from rag.ports import DependencyUnavailable, Hit, Point, Vector

logger = logging.getLogger(__name__)

# Fixed forever: changing it changes every point id, so re-ingest would leave the old points.
NAMESPACE = uuid.UUID("6f1c2b5e-3d0a-4c2e-9b7a-1f4e8d2c6a90")

_SERVICE = "qdrant"
_HASH = "hash"


def point_id(hash: str, chunk_index: int) -> str:
    """Deterministic, so re-ingesting a document overwrites its points instead of adding more."""
    return str(uuid.uuid5(NAMESPACE, f"{hash}:{chunk_index}"))


class QdrantIndex:
    def __init__(self, client: QdrantClient, collection: str, batch_size: int) -> None:
        self._client = client
        self._collection = collection
        self._batch_size = batch_size

    def delete_by_hash(self, hash: str) -> None:
        self._guard(
            lambda: self._client.delete(
                collection_name=self._collection,
                points_selector=models.FilterSelector(filter=_hash_filter([hash], match_any=False)),
                wait=True,
            )
        )

    def upsert(self, points: Sequence[Point]) -> None:
        for start in range(0, len(points), self._batch_size):
            batch = [
                models.PointStruct(
                    id=point_id(p.hash, p.chunk_index),
                    vector=p.vector,
                    payload={"hash": p.hash, "filename": p.filename, "chunk_index": p.chunk_index, "text": p.text},
                )
                for p in points[start : start + self._batch_size]
            ]
            self._guard(lambda batch=batch: self._client.upsert(collection_name=self._collection, points=batch, wait=True))

    def search(self, vector: Vector, top_k: int, min_score: float, hashes: Sequence[str]) -> list[Hit]:
        response = self._guard(
            lambda: self._client.query_points(
                collection_name=self._collection,
                query=vector,
                limit=top_k,
                query_filter=_hash_filter(hashes, match_any=True) if hashes else None,
                score_threshold=min_score if min_score > 0 else None,
                with_payload=True,
            )
        )
        return [
            Hit(
                hash=p.payload["hash"],
                filename=p.payload["filename"],
                chunk_index=p.payload["chunk_index"],
                text=p.payload["text"],
                score=p.score,
            )
            for p in response.points
        ]

    def collection_exists(self) -> bool:
        return self._guard(lambda: self._client.collection_exists(collection_name=self._collection))

    def count(self) -> int:
        return self._guard(lambda: self._client.count(collection_name=self._collection, exact=True)).count

    def clear(self) -> None:
        # An empty filter matches every point; the collection and its payload index stay.
        self._guard(
            lambda: self._client.delete(
                collection_name=self._collection,
                points_selector=models.FilterSelector(filter=models.Filter()),
                wait=True,
            )
        )

    @staticmethod
    def _guard(call):
        try:
            return call()
        except (ApiException, httpx.HTTPError, OSError) as exc:
            # ApiException covers UnexpectedResponse (any non-2xx) and ResponseHandlingException
            # (connection and timeout failures inside the client).
            logger.warning("Qdrant call failed: %s", type(exc).__name__)
            raise DependencyUnavailable(_SERVICE, type(exc).__name__) from exc


def _hash_filter(hashes: Sequence[str], match_any: bool) -> models.Filter:
    match = models.MatchAny(any=list(hashes)) if match_any else models.MatchValue(value=hashes[0])
    return models.Filter(must=[models.FieldCondition(key=_HASH, match=match)])
