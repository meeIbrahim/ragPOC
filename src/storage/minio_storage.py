from huggingface_hub.utils import logging
from minio import Minio
from core import config_reader

_minio_config = config_reader.get_config().minio
_BUCKET=_minio_config.bucket
_URL=_minio_config.url
_ACCESS_KEY=_minio_config.access_key
_SECRET_KEY=_minio_config.secret_key
_logger = logging.get_logger(__name__)

_client = Minio(
    _URL,
    _ACCESS_KEY,
    _SECRET_KEY,
    secure=False
)


def initialize_bucket(bucket: str):
    _logger.info(f"Initializing Bucket: {bucket}")
    if not _client.bucket_exists(bucket):
        _logger.info(f"Creating Bucket: {bucket}")
        _client.make_bucket(bucket)
    else:
        print(f"Bucket already created: {bucket}")
