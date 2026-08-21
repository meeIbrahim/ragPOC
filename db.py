import sqlite3
import hashlib
from pathlib import Path
from typing import Optional

class DocDB:
    db_path: str

    def __init__(self, db_name:str) -> None:
        self.db_path = "./sqlite_db" + db_name
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    document_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path TEXT NOT NULL UNIQUE,
                    sha256 TEXT NOT NULL,
                    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """)

    def file_hash(self, path: Path) -> str:
        sha256 = hashlib.sha256()
        with path.open("rb") as f:
            while chunk := f.read(1024*1024)
                sha256.update(chunk)
        return sha256.hexdigest()


    def get_document(self ,path: Path):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT document_id, sha256
                FROM documents
                WHERE path = ?
                """,
                (str(path),),
            ).fetchone()
        return row

    def save_document(self,path: Path, sha256: str) -> int:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                        """
                        INSERT INTO documents (path, sha256)
                        VALUES (?, ?)
                        ON CONFLICT(path)
                        DO UPDATE SET
                            sha256 = excluded.sha256,
                            indexed_at = CURRENT_TIMESTAMP
                        """,
                        (str(path), sha256),
                    )
            document_id = conn.execute(
                        """
                        SELECT document_id
                        FROM documents
                        WHERE path = ?
                        """,
                        (str(path),),
                    ).fetchone()[0]
            return document_id

    def needs_indexing(self, path: Path) -> tuple[bool,Optional[int],str]:
        current_hash = self.file_hash(path)
        document = self.get_document(path)
        if document is None:
            return True, None, current_hash
        document_id, stored_hash = document
        if current_hash == stored_hash:
            return False, document_id, current_hash
        # File exists but its contents changed
        return True, document_id, current_hash
