import logging

from migration.qdrant_collection_v1 import QdrantCollection
from migration.sqlite_create_table_v1 import CreateSqLiteTables
from rag.core import config_reader, logging_setup, qdrant_manager

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
    logging_setup.configure()
    config = config_reader.get_config()
    if config.unknown_keys:
        logger.warning("config has keys this service does not read: %s", ", ".join(config.unknown_keys))

    # The MinIO buckets are created by the Go service (`make go_migrate`), not here.
    migrations = [
        CreateSqLiteTables,
        QdrantCollection,
    ]
    try:
        for migration in migrations:
            run_migrations(migration)
    finally:
        qdrant_manager.get_client().close()


if __name__ == "__main__":
    main()
