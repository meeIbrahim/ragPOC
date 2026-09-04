import logging

from core import config_reader
from storage import minio_storage

logger = logging.getLogger(__name__)


class MinioCreateBucket:
    desc = "Create Minio Bucket for ingestion docs"
    def run(self):
        minio_config = config_reader.get_config().minio
        logger.info(f"Creating Bucket {minio_config.bucket}")
        minio_storage.initialize_bucket(minio_config.bucket)
