"""SQLite access for the RAG service's own bookkeeping (the indexed_documents table)."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

BUSY_TIMEOUT_MS = 5000


class SQLiteManager:
    """Context manager around one connection to <db_dir>/<collection>.db.

        with SQLiteManager(collection, dir) as conn:
            conn.execute(...)

    The directory is created if needed. The block commits when it ends normally and rolls back
    when it raises; the connection is closed either way. The database uses WAL, so a reader never
    blocks the writer.
    """

    def __init__(self, collection: str, db_dir: str | os.PathLike[str]) -> None:
        self.path = Path(db_dir) / f"{collection}.db"
        self._conn: sqlite3.Connection | None = None

    def __enter__(self) -> sqlite3.Connection:
        if self._conn is not None:
            # Entering twice would overwrite the connection: the outer block would then never
            # commit or close the first one, and its writes would be lost.
            raise RuntimeError("SQLiteManager is already open; use a new manager for a second connection")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=BUSY_TIMEOUT_MS / 1000)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
        except BaseException:
            conn.close()  # e.g. "database is locked" while switching the mode: do not leak the handle
            raise
        self._conn = conn
        return conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        conn, self._conn = self._conn, None
        if conn is None:
            return False
        try:
            if exc_type is None:
                conn.commit()
            else:
                conn.rollback()
        finally:
            conn.close()
        return False
