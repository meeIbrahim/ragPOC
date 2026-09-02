import pytest

from db.queries import indexed_documents
from db.sqlite_manager import SQLiteManager


@pytest.fixture
def db_dir(tmp_path):
    manager = SQLiteManager("pdf-fixed", str(tmp_path))
    with manager as conn:
        conn.execute("""
            CREATE TABLE indexed_documents (
                hash_id TEXT PRIMARY KEY NOT NULL,
                object_path TEXT NOT NULL,
                chunk_count INTEGER NOT NULL,
                indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    return str(tmp_path)


def test_get_by_hash_returns_none_when_absent(db_dir):
    assert indexed_documents.get_by_hash("pdf-fixed", "missing", db_dir=db_dir) is None


def test_save_then_get_round_trip(db_dir):
    indexed_documents.save_indexed("pdf-fixed", "abc123", "objects/abc.pdf", 3, db_dir=db_dir)

    row = indexed_documents.get_by_hash("pdf-fixed", "abc123", db_dir=db_dir)

    assert row["object_path"] == "objects/abc.pdf"
    assert row["chunk_count"] == 3
