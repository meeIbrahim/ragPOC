import logging

from core import qdrant_manager
from migration.minio_bucket_v1 import MinioCreateBucket
from migration.qdrant_collection_v1 import QdrantCollection
from migration.sqlite_create_table_v1 import CreateSqLiteTables

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

def run_migrations(migration):
    name = getattr(migration, "desc", migration.__name__)
    logger.info("Running migration: %s", name)
    try:
        migration().run()
        logger.info("Migration Completed: %s", name)
    except Exception:
        logger.exception("Migration Failed: %s", name)
        raise

def main():
    migrations = [
       CreateSqLiteTables,
       MinioCreateBucket,
       QdrantCollection
    ]
    for migration in migrations:
        run_migrations(migration)

    qdrant_manager.get_client().close()

if __name__ == "__main__":
    main()
