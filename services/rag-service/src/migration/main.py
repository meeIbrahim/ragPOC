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


def _close_client() -> None:
    # Closing is housekeeping: its failure must not change the outcome of the migrations.
    try:
        qdrant_manager.close_client()
    except Exception:
        logger.warning("closing the Qdrant client failed", exc_info=True)


def main() -> int:
    """Run every migration. Returns the process exit code: 0 on success, 1 on any failure."""
    logging_setup.configure()
    try:
        config = config_reader.get_config()
    except config_reader.ConfigError as exc:
        logger.error("%s", exc)
        return 1
    config_reader.warn_unknown_keys(config, logger)

    # The MinIO buckets are created by the Go service (`make go_migrate`), not here.
    migrations = [
        CreateSqLiteTables,
        QdrantCollection,
    ]
    try:
        for migration in migrations:
            run_migrations(migration)
    except Exception:
        # run_migrations already logged the traceback; this line adds where it was pointed.
        logger.error(
            "Migrations aborted (qdrant.url=%s, sqlite.dir=%s). Fix the cause and run them again; "
            "finished steps are safe to repeat.",
            config.qdrant.url,
            config.sqlite.dir,
        )
        return 1
    finally:
        _close_client()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
