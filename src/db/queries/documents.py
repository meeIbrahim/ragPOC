from pathlib import Path
from sqlite3 import Row
from typing import Optional
from core import utils
from db.sqlite_manager import SQLiteManager

def get_document_by_path(collection:str, document_path: Path) -> Row | None:
    db = SQLiteManager(collection)
    with db as conn:
        row = conn.execute(
            """
            SELECT document_id, sha256
            FROM documents
            WHERE path = ?
            """,
            (str(document_path),),
        ).fetchone()
        if row is not None:
            return row
        else:
            return None

def save_document(collection: str, document_path: Path, sha256: str) -> int:
   db = SQLiteManager(collection)
   with db as conn:
       conn.execute(
                   """
                   INSERT INTO documents (path, sha256)
                   VALUES (?, ?)
                   ON CONFLICT(path)
                   DO UPDATE SET
                       sha256 = excluded.sha256,
                       indexed_at = CURRENT_TIMESTAMP
                   """,
                   (str(document_path), sha256),
               )
       document_id = conn.execute(
                   """
                   SELECT document_id
                   FROM documents
                   WHERE path = ?
                   """,
                   (str(document_path),),
               ).fetchone()[0]
       return document_id


def needs_indexing(collection: str, document_path: Path) -> tuple[bool, Optional[int], str]:
    current_hash = utils.hash_document(document_path)
    row = get_document_by_path(collection, document_path)
    if row is None:
        return True, None, current_hash
    if row["sha256"] == current_hash:
        return False, row["document_id"], current_hash
    return True, row["document_id"], current_hash
