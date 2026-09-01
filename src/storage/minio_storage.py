import logging

from minio import Minio
from core import config_reader

_minio_config = config_reader.get_config().minio
_BUCKET=_minio_config.bucket
_URL=_minio_config.url
_ACCESS_KEY=_minio_config.access_key
_SECRET_KEY=_minio_config.secret_key
_logger = logging.getLogger(__name__)

_client = Minio(
    _URL,
    _ACCESS_KEY,
    _SECRET_KEY,
    secure=False
)
