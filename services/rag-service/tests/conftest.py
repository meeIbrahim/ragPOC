import pytest

from rag.core import config_reader
from support import VALID_TOML


def _clear_caches() -> None:
    config_reader.get_config.cache_clear()
    # The client factories cache one client per process; imported lazily so tests that do not
    # need them do not need the client libraries' import side effects.
    from rag.core import minio_client, qdrant_manager

    qdrant_manager.get_client.cache_clear()
    minio_client.get_client.cache_clear()


@pytest.fixture
def rag_config(tmp_path, monkeypatch):
    """A valid config whose SQLite directory is inside tmp_path, selected through RAG_CONFIG."""
    body = VALID_TOML.replace('dir="./sqlite_db"', f'dir="{tmp_path / "sqlite_db"}"')
    path = tmp_path / "config.toml"
    path.write_text(body)
    monkeypatch.setenv("RAG_CONFIG", str(path))
    _clear_caches()
    yield config_reader.load(path)
    _clear_caches()
