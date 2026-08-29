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
        logger.info("Creating table documents if it doesnt exist")
        with db as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    document_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path TEXT NOT NULL UNIQUE,
                    sha256 TEXT NOT NULL,
                    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
