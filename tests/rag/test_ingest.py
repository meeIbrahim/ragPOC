from unittest.mock import MagicMock, patch

from rag import ingest


@patch("rag.ingest.qdrant_manager")
@patch("rag.ingest.indexed_documents")
@patch("rag.ingest.minio_storage")
@patch("rag.ingest.chunking")
@patch("rag.ingest.embeddings")
@patch("rag.ingest.config_reader")
def test_skips_already_indexed_document(
    mock_config_reader, mock_embeddings, mock_chunking, mock_minio_storage,
    mock_indexed_documents, mock_qdrant_manager,
):
    mock_config_reader.get_config.return_value.rag.collection = "pdf-fixed"
    mock_indexed_documents.get_by_hash.return_value = {"hash_id": "abc"}

    result = ingest.ingest_document("abc", "some/object/path.pdf")

    assert result is None
    mock_minio_storage.download_to_tempfile.assert_not_called()
    mock_qdrant_manager.get_client.assert_not_called()


@patch("rag.ingest.qdrant_manager")
@patch("rag.ingest.indexed_documents")
@patch("rag.ingest.minio_storage")
@patch("rag.ingest.chunking")
@patch("rag.ingest.embeddings")
@patch("rag.ingest.config_reader")
def test_indexes_new_document_with_deterministic_point_ids(
    mock_config_reader, mock_embeddings, mock_chunking, mock_minio_storage,
    mock_indexed_documents, mock_qdrant_manager,
):
    mock_config_reader.get_config.return_value.rag.collection = "pdf-fixed"
    mock_indexed_documents.get_by_hash.return_value = None

    fake_local_path = MagicMock()
    mock_minio_storage.download_to_tempfile.return_value = fake_local_path

    chunk = MagicMock()
    chunk.page_content = "some text"
    chunk.metadata = {"source": "doc.pdf", "page": 0}
    mock_chunking.load_pdf_chunks.return_value = [chunk]

    mock_embeddings.embed.return_value = [0.1, 0.2]

    result = ingest.ingest_document("abc123", "some/object/path.pdf")

    assert result == 1
    upsert_call = mock_qdrant_manager.get_client.return_value.upsert.call_args
    points = upsert_call.kwargs["points"]
    assert points[0].payload["hash_id"] == "abc123"
    assert points[0].payload["chunk_id"] == 0
    assert points[0].id == ingest._chunk_point_id("abc123", 0)

    mock_indexed_documents.save_indexed.assert_called_once_with(
        "pdf-fixed", "abc123", "some/object/path.pdf", 1
    )
    fake_local_path.unlink.assert_called_once_with(missing_ok=True)
