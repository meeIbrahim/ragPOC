import sqlite3
from pathlib import Path
from core import config_reader

DEFAULT_DB_DIR = config_reader.get_config().sqlite.dir
class SQLiteManager:

    db_parent_dir: Path
    db_path: Path
    collection: str

    def __init__(self, collection: str, db_parent_dir: str = DEFAULT_DB_DIR) -> None:
        self.db_parent_dir = Path(db_parent_dir)
        self.db_parent_dir.mkdir(parents=True, exist_ok=True)
        collection_path = Path(collection)

        if (
            collection_path.name != collection
            or
            collection in {".",".."}
        ):
            raise ValueError(f"Invalid Collection name: {collection}")

        self.collection = collection
        self.db_path = self.db_parent_dir / f"{collection}.db"

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def __enter__(self):
        self.connection = self.connect()
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.connection.close()
