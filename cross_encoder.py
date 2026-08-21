from typing import Any

from qdrant_client.conversions.common_types import Payload, ScoredPoint
from sentence_transformers import CrossEncoder

reranker = CrossEncoder(
    "cross-encoder/ms-marco-MiniLM-L-6-v2"
)


def rerank(results: list[ScoredPoint], pairs: list[tuple[str,str]]) -> list[tuple[ScoredPoint, Any]]:
    scores = reranker.predict(pairs)
    ranked = sorted(
            zip(results, scores),
            key=lambda x: x[1],
            reverse=True,
        )
    return ranked
