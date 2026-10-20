"""Task 21: SQLiteManager and the indexed_documents schema."""

import sqlite3

import pytest

from migration.sqlite_create_table_v1 import CreateSqLiteTables
from rag.db.sqlite_manager import SQLiteManager


def test_creates_the_directory_and_uses_wal(tmp_path):
    target = tmp_path / "nested" / "dir"
    with SQLiteManager("pdf-fixed", str(target)) as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
    assert (target / "pdf-fixed.db").exists()


def test_commits_on_success(tmp_path):
    with SQLiteManager("c", str(tmp_path)) as conn:
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
    with SQLiteManager("c", str(tmp_path)) as conn:
        assert conn.execute("SELECT x FROM t").fetchall() == [(1,)]


def test_rolls_back_when_the_block_raises(tmp_path):
    with SQLiteManager("c", str(tmp_path)) as conn:
        conn.execute("CREATE TABLE t (x INTEGER)")
    with pytest.raises(RuntimeError):
        with SQLiteManager("c", str(tmp_path)) as conn:
            conn.execute("INSERT INTO t VALUES (1)")
            raise RuntimeError("boom")
    with SQLiteManager("c", str(tmp_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0


def test_closes_the_connection(tmp_path):
    manager = SQLiteManager("c", str(tmp_path))
    with manager as conn:
        pass
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")


def test_separate_collections_use_separate_files(tmp_path):
    with SQLiteManager("a", str(tmp_path)) as conn:
        conn.execute("CREATE TABLE t (x)")
    with SQLiteManager("b", str(tmp_path)) as conn:
        assert conn.execute("SELECT name FROM sqlite_master WHERE name = 't'").fetchall() == []


# ---- the migration

def columns(rag_config):
    with SQLiteManager(rag_config.rag.collection, rag_config.sqlite.dir) as conn:
        return {row[1]: (row[2], bool(row[3]), bool(row[5])) for row in conn.execute("PRAGMA table_info(indexed_documents)")}


def test_migration_creates_indexed_documents_per_the_prd_schema(rag_config):
    CreateSqLiteTables().run()
    cols = columns(rag_config)
    assert cols["hash"] == ("TEXT", True, True)  # primary key, not null
    assert cols["filename"] == ("TEXT", True, False)
    assert cols["object_path"] == ("TEXT", True, False)
    assert cols["chunk_count"] == ("INTEGER", True, False)
    assert cols["indexed_at"][0] == "TIMESTAMP"
    assert set(cols) == {"hash", "filename", "object_path", "chunk_count", "indexed_at"}


def test_migration_is_idempotent_and_keeps_rows(rag_config):
    CreateSqLiteTables().run()
    with SQLiteManager(rag_config.rag.collection, rag_config.sqlite.dir) as conn:
        conn.execute("INSERT INTO indexed_documents (hash, filename, object_path, chunk_count) VALUES ('h', 'a.txt', 'h/p', 3)")
    CreateSqLiteTables().run()
    with SQLiteManager(rag_config.rag.collection, rag_config.sqlite.dir) as conn:
        assert conn.execute("SELECT hash, indexed_at IS NOT NULL FROM indexed_documents").fetchall() == [("h", 1)]


def test_migration_refuses_a_table_from_the_old_schema(rag_config):
    # Phase 0 replaced hash_id with hash and added filename; CREATE IF NOT EXISTS would leave the
    # old table in place and fail later, far from the cause.
    with SQLiteManager(rag_config.rag.collection, rag_config.sqlite.dir) as conn:
        conn.execute("CREATE TABLE indexed_documents (hash_id TEXT PRIMARY KEY, object_path TEXT, chunk_count INTEGER, indexed_at TIMESTAMP)")
    with pytest.raises(RuntimeError, match="delete"):
        CreateSqLiteTables().run()


def test_migration_does_not_import_transformers():
    import migration.sqlite_create_table_v1 as module

    source = open(module.__file__).read()
    assert "transformers" not in source
