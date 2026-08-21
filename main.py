from typing import Any

from qdrant_client.conversions.common_types import Payload, ScoredPoint
import torch
import ollama
import qdrant
import cross_encoder
from qdrant_client.models import SearchParams
from qdrant_client.models import PointStruct


MODEL: str = "qwen3-embedding"
COLLECTION_NAME = "sentences"
chunks = [
    # Technology
    "Kubernetes automatically schedules containers across available nodes.",
    "Docker containers package applications together with their dependencies.",
    "Python is widely used for automation, data analysis, and machine learning.",
    "Git tracks changes to source code using commits and branches.",
    "Distributed systems must account for network failures and partial availability.",
    "Database indexes can significantly improve query performance.",
    "REST APIs commonly use HTTP methods such as GET, POST, PUT, and DELETE.",
    "Caching reduces the amount of work required to repeatedly retrieve the same data.",
    "Load balancers distribute incoming requests across multiple servers.",
    "Continuous integration automatically builds and tests code changes.",

    # Cloud and Infrastructure
    "AWS provides virtual machines through its EC2 service.",
    "Private subnets commonly use NAT gateways to access the public internet.",
    "Cloud storage allows applications to store data independently of compute instances.",
    "Infrastructure as code allows infrastructure to be managed through configuration files.",
    "Monitoring systems collect metrics to help engineers identify operational problems.",
    "Horizontal scaling adds more instances when application demand increases.",
    "DNS translates human-readable domain names into IP addresses.",
    "A firewall controls network traffic according to configured security rules.",
    "Virtual machines share physical hardware while remaining isolated from each other.",
    "Container orchestration systems can automatically restart failed workloads.",

    # Science
    "Water freezes at zero degrees Celsius under standard atmospheric pressure.",
    "Photosynthesis allows plants to convert light energy into chemical energy.",
    "Gravity causes objects with mass to attract one another.",
    "The Earth completes one rotation approximately every twenty-four hours.",
    "Sound travels through air as a mechanical wave.",
    "DNA contains genetic instructions used by living organisms.",
    "The speed of light in a vacuum is approximately three hundred thousand kilometers per second.",
    "Mammals regulate their internal body temperature through metabolic processes.",
    "Atoms consist of a nucleus surrounded by electrons.",
    "The Moon's gravitational pull contributes significantly to ocean tides.",

    # Geography
    "The Amazon rainforest covers a large portion of South America.",
    "The Sahara is the largest hot desert in the world.",
    "Mount Everest is located in the Himalayan mountain range.",
    "The Pacific Ocean is the largest ocean on Earth.",
    "The Nile River flows through northeastern Africa.",
    "Japan is an island country located in East Asia.",
    "The Mediterranean Sea lies between Europe, Africa, and Asia.",
    "The Andes mountain range extends along the western edge of South America.",
    "Australia is both a country and a continent.",
    "The Arctic region surrounds the Earth's northern polar area.",

    # History and Society
    "The Roman Empire developed an extensive network of roads across Europe.",
    "The printing press dramatically increased the availability of written material in Europe.",
    "The Industrial Revolution transformed manufacturing through mechanization.",
    "Ancient Egyptian civilization developed along the Nile River.",
    "The Silk Road connected trading communities across large parts of Asia and Europe.",
    "The Renaissance produced major developments in European art, science, and literature.",
    "The invention of agriculture allowed many human communities to become more settled.",
    "Ancient Greek philosophers influenced later Western approaches to logic and political thought.",
    "The development of modern democracies involved centuries of political change.",
    "Railways played an important role in the expansion of nineteenth-century trade.",

    # Health and Biology
    "Regular physical activity can improve cardiovascular fitness.",
    "Sleep plays an important role in memory consolidation and recovery.",
    "The human heart pumps blood throughout the circulatory system.",
    "Muscles generate movement by contracting against bones and connective tissue.",
    "The immune system helps protect the body from pathogens.",
    "The human brain contains billions of interconnected neurons.",
    "A balanced diet provides nutrients required for normal physiological function.",
    "The lungs exchange oxygen and carbon dioxide during respiration.",
    "Bones provide structural support and protect internal organs.",
    "The digestive system breaks food into nutrients that can be absorbed by the body.",

    # Economics and Business
    "Inflation reduces the purchasing power of a currency over time.",
    "Supply and demand influence the prices of goods in competitive markets.",
    "Interest rates affect the cost of borrowing money.",
    "Companies use financial statements to communicate their economic performance.",
    "A diversified portfolio can reduce exposure to individual investments.",
    "Productivity measures how efficiently inputs are converted into outputs.",
    "Businesses often use forecasting to estimate future demand.",
    "Unemployment measures the proportion of the labor force actively seeking work without employment.",
    "Compound interest allows returns to generate additional returns over time.",
    "Market competition can encourage companies to improve products and reduce costs.",
]


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

def index():
    points = []
    for id,chunk in enumerate(chunks):
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
    qdrant.client.upsert(
        collection_name=COLLECTION_NAME,
        points=points
    )


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
    index()
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
