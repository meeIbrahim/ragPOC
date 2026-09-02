from unittest.mock import MagicMock, patch

from rag import consumer


def test_handle_acks_after_successful_ingest():
    client = MagicMock()
    redis_config = MagicMock(stream="ingest.docs", consumer_group="rag-pdf-fixed")

    with patch("rag.consumer.ingest") as mock_ingest:
        mock_ingest.ingest_document.return_value = 3
        consumer._handle(client, redis_config, "1-0", {"hash_id": "abc", "object_path": "objects/abc.pdf"})

    mock_ingest.ingest_document.assert_called_once_with("abc", "objects/abc.pdf")
    client.xack.assert_called_once_with("ingest.docs", "rag-pdf-fixed", "1-0")


def test_handle_does_not_ack_on_ingest_failure():
    client = MagicMock()
    redis_config = MagicMock(stream="ingest.docs", consumer_group="rag-pdf-fixed")

    with patch("rag.consumer.ingest") as mock_ingest:
        mock_ingest.ingest_document.side_effect = RuntimeError("boom")
        consumer._handle(client, redis_config, "1-0", {"hash_id": "abc", "object_path": "objects/abc.pdf"})

    client.xack.assert_not_called()
