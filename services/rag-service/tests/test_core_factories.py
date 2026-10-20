"""Task 20: the client factories build clients from the shared config without touching the network."""

import logging

from rag.core import logging_setup, minio_client, qdrant_manager


def test_minio_client_uses_the_config_endpoint_and_credentials(rag_config):
    client = minio_client.get_client()
    assert client._base_url.host == "localhost:9000"
    assert client._base_url.is_https is False  # the Go service talks plain HTTP to MinIO too
    assert client._provider is not None  # static credentials from the config


def test_minio_client_is_cached(rag_config):
    assert minio_client.get_client() is minio_client.get_client()


def test_minio_client_has_a_bounded_timeout(rag_config):
    # Every external call must be bounded by [rag].call_timeout (PRD 5.1); urllib3 enforces it.
    timeout = minio_client.get_client()._http.connection_pool_kw["timeout"]
    assert timeout.connect_timeout == rag_config.rag.call_timeout.total_seconds()
    assert timeout.read_timeout == rag_config.rag.call_timeout.total_seconds()


def test_minio_factory_creates_no_bucket(rag_config):
    # Go owns both buckets (L5). The factory must not even offer a way to create them.
    assert not hasattr(minio_client, "initialize_bucket")
    assert not hasattr(minio_client, "ensure_bucket")


def test_qdrant_client_uses_the_config_url_and_timeout(rag_config):
    client = qdrant_manager.get_client()
    try:
        assert client.init_options["url"] == "http://localhost:6333"
        assert client.init_options["timeout"] == int(rag_config.rag.call_timeout.total_seconds())
    finally:
        client.close()


def test_qdrant_client_is_cached(rag_config):
    first = qdrant_manager.get_client()
    try:
        assert first is qdrant_manager.get_client()
    finally:
        first.close()


def test_logging_configure_is_idempotent_and_sets_the_level():
    root = logging.getLogger()
    before = list(root.handlers)
    try:
        logging_setup.configure("DEBUG")
        logging_setup.configure("DEBUG")
        assert root.level == logging.DEBUG
        assert len(root.handlers) <= len(before) + 1  # a second call must not add another handler
    finally:
        root.handlers[:] = before
        root.setLevel(logging.WARNING)


def test_logging_levels_come_from_the_environment(monkeypatch):
    root = logging.getLogger()
    before = list(root.handlers)
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    try:
        logging_setup.configure()
        assert root.level == logging.WARNING
    finally:
        root.handlers[:] = before
        root.setLevel(logging.WARNING)
