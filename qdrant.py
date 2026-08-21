from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, HnswConfigDiff


DB_PATH = "./qdrant_data"

# Persistent local Qdrant database
client = QdrantClient(path=DB_PATH)

# Qwen3-Embedding dimensionality.
# Better: obtain this dynamically from Ollama before creating the collection.
VECTOR_SIZE = 4096

def ensure_collection_exists(collection_name: str, vector_size: int = 4096):
    if client.collection_exists(collection_name):
        print(f"Collection '{collection_name}' already exists")
    else:
        client.create_collection(
            collection_name=collection_name,

            vectors_config=VectorParams(
                size=vector_size,
                distance=Distance.COSINE,
            ),

            hnsw_config=HnswConfigDiff(
                m=4,
                ef_construct=100,
                full_scan_threshold=1
            ),
        )
        print(f"Created collection '{collection_name}'")


def get_client() -> QdrantClient:
    return client

def close_client():
    client.close()
