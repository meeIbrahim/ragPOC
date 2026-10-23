"""Startup checks (spec R5, R7, A7, A8, E12, E22) and the adapter wiring."""

import pytest

from fakes import FakeEmbedder, FakeIndex
from rag.adapters.groq_llm import GroqLLM
from rag.adapters.minio_blob_store import MinioBlobStore
from rag.adapters.ollama_embedder import OllamaEmbedder
from rag.adapters.qdrant_index import QdrantIndex
from rag.adapters.sqlite_index_registry import SqliteIndexRegistry
from rag.core import wiring
from rag.core.startup import StartupError, check_collection, check_embedding_dim
from rag.ingest.service import IngestService
from rag.ports import DependencyUnavailable


def test_a_matching_dimension_passes():
    check_embedding_dim(FakeEmbedder(dim=2560), expected=2560, model="qwen3-embedding:4b")


def test_a_dimension_mismatch_names_both_sizes_and_the_fix():
    with pytest.raises(StartupError) as excinfo:
        check_embedding_dim(FakeEmbedder(dim=1024), expected=2560, model="qwen3-embedding:4b")
    message = str(excinfo.value)
    assert excinfo.value.transient is False
    assert "1024" in message and "2560" in message and "qwen3-embedding:4b" in message
    assert "embedding_dim" in message
    # Without clearing the registry first, every document would be skipped as already indexed.
    assert message.index("reset_index") < message.index("run_migrations")


def test_an_unreachable_embedder_is_a_startup_error_naming_the_service():
    embedder = FakeEmbedder()
    embedder.error = DependencyUnavailable("ollama", "ConnectionError")
    with pytest.raises(StartupError, match="ollama") as excinfo:
        check_embedding_dim(embedder, expected=4, model="m")
    assert excinfo.value.transient is True


def test_a_missing_model_names_the_pull_command():
    from rag.ports import ModelNotAvailable

    embedder = FakeEmbedder()
    embedder.error = ModelNotAvailable("qwen3-embedding:4b")
    with pytest.raises(StartupError, match=r"ollama pull qwen3-embedding:4b") as excinfo:
        check_embedding_dim(embedder, expected=4, model="qwen3-embedding:4b")
    assert excinfo.value.transient is False


def test_an_existing_collection_passes():
    check_collection(FakeIndex(exists=True), "pdf-fixed")


def test_a_missing_collection_names_the_migration():
    with pytest.raises(StartupError, match=r"pdf-fixed.*make run_migrations"):
        check_collection(FakeIndex(exists=False), "pdf-fixed")


def test_an_unreachable_qdrant_is_a_startup_error():
    index = FakeIndex()
    index.error = DependencyUnavailable("qdrant", "ConnectError")
    with pytest.raises(StartupError, match="qdrant") as excinfo:
        check_collection(index, "pdf-fixed")
    assert excinfo.value.transient is True


def test_wiring_builds_real_adapters_from_the_given_config(rag_config, monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key-not-used")
    service = wiring.ingest_service(rag_config)
    assert isinstance(service, IngestService)
    assert isinstance(service._blobs, MinioBlobStore)
    assert isinstance(service._registry, SqliteIndexRegistry)
    assert isinstance(service._embedder, OllamaEmbedder)
    assert isinstance(service._index, QdrantIndex)
    assert isinstance(wiring.llm(rag_config), GroqLLM)


def test_the_llm_needs_the_api_key(rag_config, monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    with pytest.raises(StartupError, match="GROQ_API_KEY"):
        wiring.llm(rag_config)
