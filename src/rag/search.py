from typing import Any

from qdrant_client.models import SearchParams, ScoredPoint

from core import config_reader, qdrant_manager
from rag import cross_encoder, embeddings


def search(query: str, limit: int = 5) -> list[ScoredPoint]:
    collection = config_reader.get_config().rag.collection
    client = qdrant_manager.get_client()
    query_embedding = embeddings.embed(query)
    results = client.query_points(
        collection_name=collection,
        query=query_embedding,
        with_payload=True,
        limit=limit,
        search_params=SearchParams(
            hnsw_ef=12,
            exact=False,
        ),
    ).points
    return results


def rerank(results: list[ScoredPoint], query: str, top_k: int = 5) -> list[tuple[ScoredPoint, Any]]:
    pairs = [
        (query, result.payload["text"])
        for result in results
        if result.payload
    ]
    ranked = cross_encoder.rerank(results, pairs)
    return ranked[:top_k]
