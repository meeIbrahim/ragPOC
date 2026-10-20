import logging

from qdrant_client.models import Distance, HnswConfigDiff, VectorParams

from rag.core import config_reader, qdrant_manager

logger = logging.getLogger(__name__)


class QdrantCollection:
    desc = "Create Qdrant Collection"

    def __init__(self, client=None):
        self._client = client

    def run(self):
        config = config_reader.get_config().rag
        dim = config.embedding_dim
        if dim <= 0:
            raise RuntimeError(
                "rag.embedding_dim must be > 0 to create the collection: set it to the output "
                f"size of the embedding model {config.embedding_model!r} in config.toml"
            )

        client = self._client or qdrant_manager.get_client()
        name = config.collection
        if client.collection_exists(name):
            existing = _vector_size(client.get_collection(name).config.params.vectors)
            if existing != dim:
                raise RuntimeError(
                    f"collection {name!r} exists with vector size {existing} but rag.embedding_dim "
                    f"is {dim}. Fix embedding_dim, or drop the collection and run the migration "
                    "again (this deletes its indexed vectors)."
                )
            logger.info("Collection %s already initialized", name)
            return

        logger.info("Creating collection %s (vector size %d)", name, dim)
        client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
            hnsw_config=HnswConfigDiff(m=4, ef_construct=100, full_scan_threshold=10),
        )
        logger.info("Collection %s created", name)


def _vector_size(vectors):
    """The size of the unnamed vector config, or None for a named-vectors collection."""
    return getattr(vectors, "size", None)
