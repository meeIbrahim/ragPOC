import logging

from rag.core import config_reader
from rag.db.sqlite_manager import SQLiteManager

logger = logging.getLogger(__name__)

# PRD 5.2. hash is the document identity (SHA-256 of the content, computed by Go). A row means
# the document is completely indexed.
_COLUMNS = {"hash", "filename", "object_path", "chunk_count", "indexed_at"}


class CreateSqLiteTables:
    desc = "Create SqLite Tables"

    def run(self):
        config = config_reader.get_config()
        manager = SQLiteManager(config.rag.collection, config.sqlite.dir)
        with manager as conn:
            self._indexed_documents_table(conn, manager.path)

    def _indexed_documents_table(self, conn, db_path):
        logger.info("Initializing indexed_documents table")
        existing = {row[1] for row in conn.execute("PRAGMA table_info(indexed_documents)")}
        if existing and existing != _COLUMNS:
            # CREATE TABLE IF NOT EXISTS would keep the old table and fail much later.
            raise RuntimeError(
                f"indexed_documents in {db_path} has the old schema (columns {sorted(existing)}); "
                f"delete {db_path} and run the migration again. It was only ever written by "
                "the retired Phase 0 prototype."
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS indexed_documents (
                hash TEXT PRIMARY KEY NOT NULL,
                filename TEXT NOT NULL,
                object_path TEXT NOT NULL,
                chunk_count INTEGER NOT NULL,
                indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
