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


def test_run_reclaims_pending_entries_on_startup():
    client = MagicMock()
    redis_config = MagicMock(stream="ingest.docs", consumer_group="rag-pdf-fixed")

    # Pending entry from prior crash
    pending_message_id = "2-0"
    pending_fields = {"hash_id": "pending_hash", "object_path": "objects/pending.pdf"}

    # First xreadgroup call with "0" returns pending entries
    # Second xreadgroup call with ">" returns empty
    # Third and beyond: raise KeyboardInterrupt to break the loop
    client.xreadgroup.side_effect = [
        [("ingest.docs", [(pending_message_id, pending_fields)])],  # Pending reclaim
        [],  # New messages (empty, loop continues)
        KeyboardInterrupt(),  # Break the loop
    ]

    with patch("rag.consumer.ingest") as mock_ingest, \
         patch("rag.consumer.config_reader") as mock_config_reader, \
         patch("rag.consumer.get_client", return_value=client), \
         patch("rag.consumer.logger"):
        mock_config_reader.get_config.return_value.redis = redis_config
        mock_ingest.ingest_document.return_value = 1

        try:
            consumer.run()
        except KeyboardInterrupt:
            pass  # Expected to break the loop

    # Verify pending entry was processed and acked
    mock_ingest.ingest_document.assert_called_once_with("pending_hash", "objects/pending.pdf")
    client.xack.assert_called_once_with("ingest.docs", "rag-pdf-fixed", pending_message_id)

    # Verify xreadgroup was called at least twice: first with "0" for reclaim, then with ">" for new
    assert client.xreadgroup.call_count >= 2
    calls = client.xreadgroup.call_args_list
    assert calls[0][0] == ("rag-pdf-fixed", "rag-worker-1", {"ingest.docs": "0"})
    assert calls[1][0] == ("rag-pdf-fixed", "rag-worker-1", {"ingest.docs": ">"})
