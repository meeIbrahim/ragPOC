"""Task 22: the Qdrant collection migration, against a fake client (no Qdrant, no Ollama)."""

from types import SimpleNamespace

import pytest
from qdrant_client.models import Distance

from migration.qdrant_collection_v1 import QdrantCollection


class FakeQdrant:
    def __init__(self, existing_size=None):
        self.existing_size = existing_size
        self.created = []

    def collection_exists(self, name):
        return self.existing_size is not None

    def get_collection(self, name):
        vectors = SimpleNamespace(size=self.existing_size, distance=Distance.COSINE)
        return SimpleNamespace(config=SimpleNamespace(params=SimpleNamespace(vectors=vectors)))

    def create_collection(self, **kwargs):
        self.created.append(kwargs)


def test_creates_the_collection_with_the_configured_dimension(rag_config):
    client = FakeQdrant()
    QdrantCollection(client).run()
    assert len(client.created) == 1
    created = client.created[0]
    assert created["collection_name"] == "pdf-fixed"
    assert created["vectors_config"].size == 4096
    assert created["vectors_config"].distance == Distance.COSINE


def test_leaves_a_matching_collection_alone(rag_config):
    client = FakeQdrant(existing_size=4096)
    QdrantCollection(client).run()
    assert client.created == []


def test_refuses_an_existing_collection_of_another_size(rag_config):
    # D3: embedding_dim was chosen before the model was pulled. A mismatch must stop the run and
    # say what to do, not corrupt the index later.
    client = FakeQdrant(existing_size=1024)
    with pytest.raises(RuntimeError, match=r"1024.*4096|4096.*1024"):
        QdrantCollection(client).run()
    assert client.created == []


@pytest.mark.parametrize("dim", [0, -5])
def test_refuses_a_non_positive_embedding_dim(rag_config, monkeypatch, tmp_path, dim):
    from rag.core import config_reader

    path = tmp_path / "bad.toml"
    path.write_text((tmp_path / "config.toml").read_text().replace("embedding_dim=4096", f"embedding_dim={dim}"))
    monkeypatch.setenv("RAG_CONFIG", str(path))
    config_reader.get_config.cache_clear()
    if dim < 0:
        with pytest.raises(config_reader.ConfigError, match="embedding_dim"):
            QdrantCollection(FakeQdrant()).run()
    else:
        with pytest.raises(RuntimeError, match="embedding_dim"):
            QdrantCollection(FakeQdrant()).run()


def test_never_calls_ollama():
    import migration.qdrant_collection_v1 as module

    source = open(module.__file__).read()
    assert "ollama" not in source.lower()
