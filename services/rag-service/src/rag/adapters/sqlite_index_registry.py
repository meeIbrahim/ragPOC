"""IndexRegistry over the indexed_documents table (PRD 5.2): a row means fully indexed."""

from __future__ import annotations

import os
from datetime import UTC

from rag.db.sqlite_manager import SQLiteManager
from rag.ports import IndexedDocument


class SqliteIndexRegistry:
    def __init__(self, collection: str, db_dir: str | os.PathLike[str]) -> None:
        self._collection = collection
        self._db_dir = db_dir

    def _connect(self) -> SQLiteManager:
        return SQLiteManager(self._collection, self._db_dir)

    def exists(self, hash: str) -> bool:
        with self._connect() as conn:
            return conn.execute("SELECT 1 FROM indexed_documents WHERE hash = ?", (hash,)).fetchone() is not None

    def register(self, document: IndexedDocument) -> None:
        if document.indexed_at.tzinfo is None:
            raise ValueError("indexed_at must be timezone-aware")
        with self._connect() as conn:
            # Two jobs for the same hash can both finish (a reclaimed entry racing a slow one);
            # the first row wins and both report INDEXED.
            conn.execute(
                "INSERT INTO indexed_documents (hash, filename, object_path, chunk_count, indexed_at) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT(hash) DO NOTHING",
                (
                    document.hash,
                    document.filename,
                    document.object_path,
                    document.chunk_count,
                    document.indexed_at.astimezone(UTC).isoformat(),
                ),
            )

    def count(self) -> int:
        with self._connect() as conn:
            return conn.execute("SELECT COUNT(*) FROM indexed_documents").fetchone()[0]

    def clear(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM indexed_documents")
