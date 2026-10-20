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
            self._check_existing(name, dim, client.get_collection(name).config.params.vectors)
            logger.info("Collection %s already initialized", name)
            return

        logger.info("Creating collection %s (vector size %d)", name, dim)
        client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
            hnsw_config=HnswConfigDiff(m=4, ef_construct=100, full_scan_threshold=10),
        )
        logger.info("Collection %s created", name)


    @staticmethod
    def _check_existing(name, dim, vectors):
        drop = "drop the collection and run the migration again (this deletes its indexed vectors)"
        size = getattr(vectors, "size", None)
        if size is None:  # a dict of named vectors: this service writes one unnamed vector
            raise RuntimeError(
                f"collection {name!r} uses named or sparse vectors, but this service needs one "
                f"unnamed dense vector; {drop}."
            )
        if size != dim:
            raise RuntimeError(
                f"collection {name!r} exists with vector size {size} but rag.embedding_dim is "
                f"{dim}. Fix embedding_dim, or {drop}."
            )
        if getattr(vectors, "multivector_config", None) is not None:
            raise RuntimeError(
                f"collection {name!r} is a multivector collection, but this service writes one "
                f"dense vector per chunk; {drop}."
            )
        distance = getattr(vectors, "distance", None)
        if distance != Distance.COSINE:
            raise RuntimeError(
                f"collection {name!r} uses distance {distance}, but retrieval scores assume "
                f"cosine; {drop}."
            )
