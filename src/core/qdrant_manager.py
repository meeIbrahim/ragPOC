from core import config_reader, utils
from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue


DB_PATH = utils.REPO_ROOT / config_reader.get_config().qdrant.dir
if not DB_PATH.exists:
    DB_PATH.mkdir(parents=True,exist_ok=True)

_client = QdrantClient(path=str(DB_PATH.resolve()))

def get_client() -> QdrantClient:
    return _client

def delete_document_vectors(collection: str, document_id: int):
    get_client().delete(
        collection_name=collection,
        points_selector=Filter(
            must=[
                FieldCondition(key="document_id", match=MatchValue(value=document_id))
            ]
        ),
    )
