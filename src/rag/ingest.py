import uuid
from pathlib import Path

from qdrant_client.models import PointStruct

from core import config_reader, qdrant_manager
from db.queries import documents
from rag import chunking, embeddings

POINT_ID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def _chunk_point_id(document_hash: str, chunk_index: int) -> str:
    return str(uuid.uuid5(POINT_ID_NAMESPACE, f"{document_hash}:{chunk_index}"))


def ingest_document(document_path: Path) -> None:
    collection = config_reader.get_config().rag.collection

    to_index, existing_document_id, document_hash = documents.needs_indexing(collection, document_path)
    if not to_index:
        print(f"Skipping unchanged: {document_path}")
        return

    if existing_document_id is not None:
        print(f"Document changed, re-indexing: {document_path}")
        qdrant_manager.delete_document_vectors(collection, existing_document_id)

    document_id = documents.save_document(collection, document_path, document_hash)

    chunks = chunking.load_pdf_chunks(document_path)
    points = [
        PointStruct(
            id=_chunk_point_id(document_hash, chunk_index),
            vector=embeddings.embed(chunk.page_content),
            payload={
                "text": chunk.page_content,
                "source": chunk.metadata.get("source"),
                "page": chunk.metadata.get("page"),
                "document_id": document_id,
                "document_hash": document_hash,
                "chunk_index": chunk_index,
            },
        )
        for chunk_index, chunk in enumerate(chunks)
    ]

    qdrant_manager.get_client().upsert(collection_name=collection, points=points)
    print(f"Indexed {len(points)} chunks: {document_path}")


def ingest_directory(pdf_dir: Path) -> None:
    for document_path in chunking.iter_pdfs(pdf_dir):
        ingest_document(document_path)
