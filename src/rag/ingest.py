import uuid

from qdrant_client.models import PointStruct

from core import config_reader, qdrant_manager
from db.queries import indexed_documents
from rag import chunking, embeddings
from storage import minio_storage

POINT_ID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def _chunk_point_id(hash_id: str, chunk_index: int) -> str:
    return str(uuid.uuid5(POINT_ID_NAMESPACE, f"{hash_id}:{chunk_index}"))


def ingest_document(hash_id: str, object_path: str) -> int | None:
    collection = config_reader.get_config().rag.collection

    if indexed_documents.get_by_hash(collection, hash_id) is not None:
        print(f"Skipping already-indexed document: {hash_id}")
        return None

    local_path = minio_storage.download_to_tempfile(object_path)
    try:
        chunks = chunking.load_pdf_chunks(local_path)
        points = [
            PointStruct(
                id=_chunk_point_id(hash_id, chunk_index),
                vector=embeddings.embed(chunk.page_content),
                payload={
                    "text": chunk.page_content,
                    "source": chunk.metadata.get("source"),
                    "page": chunk.metadata.get("page"),
                    "hash_id": hash_id,
                    "chunk_id": chunk_index,
                    "object_path": object_path,
                },
            )
            for chunk_index, chunk in enumerate(chunks)
        ]

        qdrant_manager.get_client().upsert(collection_name=collection, points=points)
        indexed_documents.save_indexed(collection, hash_id, object_path, len(points))
        print(f"Indexed {len(points)} chunks: {hash_id}")
        return len(points)
    finally:
        local_path.unlink(missing_ok=True)
