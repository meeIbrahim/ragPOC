"""R2 SqliteIndexRegistry against a real SQLite file with the migration applied (spec E1)."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from migration.sqlite_create_table_v1 import CreateSqLiteTables
from rag.adapters.sqlite_index_registry import SqliteIndexRegistry
from rag.db.sqlite_manager import SQLiteManager
from rag.ports import IndexedDocument

H1, H2 = "a" * 64, "b" * 64


@pytest.fixture
def registry(rag_config):
    CreateSqLiteTables().run()
    return SqliteIndexRegistry(rag_config.rag.collection, rag_config.sqlite.dir)


def doc(hash=H1, filename="guide.pdf", chunks=3, at=datetime(2026, 10, 23, 7, 0, tzinfo=UTC)):
    return IndexedDocument(hash, filename, f"aa/bb/{hash}", chunks, at)


def rows(rag_config):
    with SQLiteManager(rag_config.rag.collection, rag_config.sqlite.dir) as conn:
        return conn.execute("SELECT hash, filename, object_path, chunk_count, indexed_at FROM indexed_documents").fetchall()


def test_exists_is_false_until_registered(registry):
    assert registry.exists(H1) is False
    registry.register(doc())
    assert registry.exists(H1) is True
    assert registry.exists(H2) is False


def test_register_writes_every_column_with_utc_iso_time(registry, rag_config):
    registry.register(doc(at=datetime(2026, 10, 23, 12, 0, tzinfo=timezone(timedelta(hours=5)))))
    assert rows(rag_config) == [(H1, "guide.pdf", f"aa/bb/{H1}", 3, "2026-10-23T07:00:00+00:00")]


def test_a_second_register_keeps_the_first_row(registry, rag_config):
    registry.register(doc(filename="first.pdf", chunks=3))
    registry.register(doc(filename="second.pdf", chunks=9))
    assert [(r[1], r[3]) for r in rows(rag_config)] == [("first.pdf", 3)]


def test_count_and_clear(registry):
    registry.register(doc(H1))
    registry.register(doc(H2))
    assert registry.count() == 2
    registry.clear()
    assert registry.count() == 0
    assert registry.exists(H1) is False


def test_a_naive_timestamp_is_rejected(registry):
    with pytest.raises(ValueError):
        registry.register(doc(at=datetime(2026, 10, 23, 7, 0)))
