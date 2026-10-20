"""Review round 1: logging levels, SQLite manager re-entry, and the Qdrant/migration edge cases."""

import logging
from types import SimpleNamespace

import pytest
from qdrant_client.models import Distance

from migration import main as migration_main
from migration.qdrant_collection_v1 import QdrantCollection
from rag.core import logging_setup
from rag.db.sqlite_manager import SQLiteManager


# ---- logging

@pytest.mark.parametrize("level", ["debug", "Info", "WARNING", "error"])
def test_log_level_names_are_case_insensitive(monkeypatch, level):
    root = logging.getLogger()
    before = list(root.handlers)
    monkeypatch.setenv("LOG_LEVEL", level)
    try:
        logging_setup.configure()
        assert root.level == getattr(logging, level.upper())
    finally:
        root.handlers[:] = before
        root.setLevel(logging.WARNING)


# ---- sqlite manager

def test_reentering_a_manager_is_an_error_not_silent_data_loss(tmp_path):
    manager = SQLiteManager("c", str(tmp_path))
    with manager as conn:
        conn.execute("CREATE TABLE t (x)")
        conn.execute("INSERT INTO t VALUES (1)")
        with pytest.raises(RuntimeError, match="already open"):
            manager.__enter__()
    with SQLiteManager("c", str(tmp_path)) as conn:  # the outer block still committed
        assert conn.execute("SELECT x FROM t").fetchall() == [(1,)]


def test_a_manager_can_be_used_again_after_it_closed(tmp_path):
    manager = SQLiteManager("c", str(tmp_path))
    with manager as conn:
        conn.execute("CREATE TABLE t (x)")
    with manager as conn:
        assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0


# ---- qdrant migration against collections the happy-path fake never returned

class Fake:
    def __init__(self, vectors):
        self.vectors = vectors
        self.created = []

    def collection_exists(self, name):
        return True

    def get_collection(self, name):
        return SimpleNamespace(config=SimpleNamespace(params=SimpleNamespace(vectors=self.vectors)))

    def create_collection(self, **kwargs):
        self.created.append(kwargs)


def test_a_collection_with_another_distance_is_refused(rag_config):
    fake = Fake(SimpleNamespace(size=4096, distance=Distance.DOT))
    with pytest.raises(RuntimeError, match="distance"):
        QdrantCollection(fake).run()
    assert fake.created == []


def test_a_named_vectors_collection_gets_its_own_message(rag_config):
    fake = Fake({"dense": SimpleNamespace(size=4096, distance=Distance.COSINE)})
    with pytest.raises(RuntimeError, match="named"):
        QdrantCollection(fake).run()


def test_a_matching_cosine_collection_passes(rag_config):
    fake = Fake(SimpleNamespace(size=4096, distance=Distance.COSINE))
    QdrantCollection(fake).run()
    assert fake.created == []


# ---- the migration entry point

class Boom:
    desc = "Always fails"

    def run(self):
        raise ConnectionError("Connection refused")


@pytest.fixture
def keep_pytest_log_capture(monkeypatch):
    # configure() replaces the root handlers, which would remove caplog's.
    monkeypatch.setattr(migration_main.logging_setup, "configure", lambda *a, **k: None)


def test_main_returns_1_and_names_the_qdrant_url_on_failure(rag_config, monkeypatch, caplog, keep_pytest_log_capture):
    monkeypatch.setattr(migration_main, "CreateSqLiteTables", Boom)
    monkeypatch.setattr(migration_main, "QdrantCollection", Boom)
    with caplog.at_level(logging.ERROR):
        code = migration_main.main()
    assert code == 1
    assert "http://localhost:6333" in caplog.text


def test_main_returns_0_when_every_migration_passes(rag_config, monkeypatch, keep_pytest_log_capture):
    class Ok:
        desc = "ok"

        def run(self):
            pass

    monkeypatch.setattr(migration_main, "CreateSqLiteTables", Ok)
    monkeypatch.setattr(migration_main, "QdrantCollection", Ok)
    assert migration_main.main() == 0


def test_the_traceback_is_logged_once(rag_config, monkeypatch, caplog, keep_pytest_log_capture):
    monkeypatch.setattr(migration_main, "CreateSqLiteTables", Boom)
    monkeypatch.setattr(migration_main, "QdrantCollection", Boom)
    with caplog.at_level(logging.ERROR):
        migration_main.main()
    assert caplog.text.count("Traceback") == 1
