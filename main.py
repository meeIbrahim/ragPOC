from typing import Any

from qdrant_client.conversions.common_types import Payload, Points, ScoredPoint
import torch
import ollama
import qdrant
import cross_encoder
import sentences
import pdf
from qdrant_client.models import SearchParams
from qdrant_client.models import PointStruct


MODEL: str = "qwen3-embedding"
COLLECTION_NAME = "document"


def embed(text: str) -> list[float]:
    response = ollama.embed(
        model=MODEL,
        input=text
    )
    return response["embeddings"][0]

def cosine_similarity(a: torch.Tensor, b: torch.Tensor) -> float:
    return torch.nn.functional.cosine_similarity(
        a.unsqueeze(0),
        b.unsqueeze(0)
    ).item()

def sentenceChunks():
    points = []
    for id,chunk in enumerate(sentences.chunks):
        vector = embed(chunk)
        points.append(
            PointStruct(
                id = id,
                vector = vector,
                payload={
                    "text": chunk,
                }
            )
        )
    return points

def index(points: Points):
    qdrant.client.upsert(
        collection_name=COLLECTION_NAME,
        points=points
    )

def pdfChunks():
    chunks = pdf.fixedChunks("./doc")
    points = []
    for id,chunk in enumerate(chunks):
        vector = embed(chunk.page_content)
        points.append(
            PointStruct(
                id=id,
                vector=vector,
                payload={
                    "text": chunk.page_content,
                    "source": chunk.metadata.get("source"),
                    "page": chunk.metadata.get("page")
                }
            )
        )
    return points

def search(query: str) -> list[ScoredPoint]:
    client = qdrant.get_client()
    query_embeddings = ollama.embed(model=MODEL, input=query)["embeddings"][0]
    results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_embeddings,
        with_payload=True,
        limit=5,
        search_params=SearchParams(
            hnsw_ef=12,
            exact=False
        )
    ).points
    return results

def rerank(results: list[ScoredPoint], query: str, top_k: int = 5) -> list[tuple[ScoredPoint,Any]]:
    pairs = [
        (query, result.payload["text"])
        for result in results
        if result.payload
    ]
    ranked = cross_encoder.rerank(results,pairs)
    return ranked[:top_k]

query="which area has the most divese geography"

if __name__ == "__main__":
    embeddings = ollama.embed(model=MODEL, input="sample text")["embeddings"][0]
    dimension = len(embeddings)
    qdrant.ensure_collection_exists(COLLECTION_NAME,dimension)
    points = sentenceChunks()
    index(points)
    results = search(query=query)
    for result in results:
        print(result.id)
        print(result.score)
        if (result.payload):
            print(result.payload.get("text"))

    reranked = rerank(results, query)
    print("-------- RERANKED --------")
    for point in reranked:
        result = point[0]
        rerank_score = point[1]
        print(result.id)
        print(result.score)
        if (result.payload):
            print(result.payload.get("text"))
        print(rerank_score)
    qdrant.close_client()
