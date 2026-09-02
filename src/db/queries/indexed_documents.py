from datetime import datetime, timezone
from sqlite3 import Row

from db.sqlite_manager import SQLiteManager


def _db(collection: str, db_dir: str | None) -> SQLiteManager:
    return SQLiteManager(collection, db_dir) if db_dir else SQLiteManager(collection)


def get_by_hash(collection: str, hash_id: str, db_dir: str | None = None) -> Row | None:
    db = _db(collection, db_dir)
    with db as conn:
        return conn.execute(
            """
            SELECT hash_id, object_path, chunk_count, indexed_at
            FROM indexed_documents
            WHERE hash_id = ?
            """,
            (hash_id,),
        ).fetchone()


def save_indexed(
    collection: str, hash_id: str, object_path: str, chunk_count: int, db_dir: str | None = None
) -> None:
    db = _db(collection, db_dir)
    with db as conn:
        conn.execute(
            """
            INSERT INTO indexed_documents (hash_id, object_path, chunk_count, indexed_at)
            VALUES (?, ?, ?, ?)
            """,
            (hash_id, object_path, chunk_count, datetime.now(timezone.utc).isoformat()),
        )
