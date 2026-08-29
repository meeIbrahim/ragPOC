

import logging

import ollama
from qdrant_client.models import Distance, HnswConfigDiff, VectorParams

from core import config_reader, qdrant_manager

logger = logging.getLogger(__name__)

class QdrantCollection:
    desc = "Create Qdrant Collection"

    def run(self):
        collection = config_reader.get_config().rag.collection
        model = config_reader.get_config().rag.model
        client = qdrant_manager.get_client()
        if client.collection_exists(collection):
            logger.info(f"Collecton {collection} already initialized")

        else:
            logger.info(f"Creating collection {collection}")
            embeddings = ollama.embed(model=model, input="sample text")["embeddings"][0]
            vector_size = len(embeddings)

            client.create_collection(
                collection_name = collection,
                vectors_config=VectorParams(
                    size = vector_size,
                    distance=Distance.COSINE
                ),
                hnsw_config=HnswConfigDiff(
                    m=4,
                    ef_construct=100,
                    full_scan_threshold=1
                )
            )
            logger.info("Collection %s created", collection)
