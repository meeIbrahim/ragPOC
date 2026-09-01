from core import config_reader
from db.sqlite_manager import SQLiteManager
from transformers.utils import logging


logger = logging.get_logger(__name__)

class CreateSqLiteTables:
    desc = "Create SqLite Tables"
    def run(self):
        collection = config_reader.get_config().rag.collection
        db_dir = config_reader.get_config().sqlite.dir
        db = SQLiteManager(collection, db_dir)
        self._collection_table(db)
        self._storage_table(db)

    def _storage_table(self, db_manager: SQLiteManager):
        logger.info("Initializing ingestion_storage table")
        with db_manager as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_storage (
                document_hash TEXT PRIMARY KEY NOT NULL,
                object_path TEXT NOT NULL UNIQUE
                )
                """)

    def _collection_table(self, db_manager: SQLiteManager):
        logger.info("Initializing documents table")
        with db_manager as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    document_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path TEXT NOT NULL UNIQUE,
                    sha256 TEXT NOT NULL,
                    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
