from core import config_reader
from huggingface_hub.utils import logging
from storage import minio_storage

logger = logging.get_logger(__name__)


class MinioCreateBucket:
    desc = "Create Minio Bucket for ingestion docs"
    def run(self):
        minio_config = config_reader.get_config().minio
        logger.info(f"Creating Bucket {minio_config.bucket}")
        minio_storage.initialize_bucket(minio_config.bucket)
