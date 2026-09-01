from core import config_reader
from qdrant_client import QdrantClient

_client = QdrantClient(url=config_reader.get_config().qdrant.url)


def get_client() -> QdrantClient:
    return _client
