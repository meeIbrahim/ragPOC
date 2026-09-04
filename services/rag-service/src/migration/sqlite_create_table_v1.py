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
        self._indexed_documents_table(db)

    def _indexed_documents_table(self, db_manager: SQLiteManager):
        logger.info("Initializing indexed_documents table")
        with db_manager as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS indexed_documents (
                    hash_id TEXT PRIMARY KEY NOT NULL,
                    object_path TEXT NOT NULL,
                    chunk_count INTEGER NOT NULL,
                    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
