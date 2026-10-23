import logging

from qdrant_client.models import PayloadSchemaType

from rag.core import config_reader, qdrant_manager

logger = logging.getLogger(__name__)

# Every chunk's payload carries its document's content hash. Retrieval filters on it
# (document_ids) and ingest deletes by it before an upsert, so both need this index.
FIELD = "hash"


class QdrantPayloadIndex:
    desc = "Create Qdrant payload index on hash"

    def __init__(self, client=None):
        self._client = client

    def run(self):
        name = config_reader.get_config().rag.collection
        client = self._client or qdrant_manager.get_client()
        existing = client.get_collection(name).payload_schema.get(FIELD)
        if existing is not None:
            if str(getattr(existing.data_type, "value", existing.data_type)) != PayloadSchemaType.KEYWORD.value:
                raise RuntimeError(
                    f"payload index {FIELD!r} on {name!r} is {existing.data_type}, not keyword: "
                    "delete that index in Qdrant and run the migration again"
                )
            logger.info("Payload index %s on %s already exists", FIELD, name)
            return

        logger.info("Creating keyword payload index %s on %s", FIELD, name)
        client.create_payload_index(
            collection_name=name,
            field_name=FIELD,
            field_schema=PayloadSchemaType.KEYWORD,
            wait=True,
        )
