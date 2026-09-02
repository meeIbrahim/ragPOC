import redis

from core import config_reader

_config = config_reader.get_config().redis
_client = redis.Redis(host=_config.host, port=_config.port, decode_responses=True)


def get_client() -> redis.Redis:
    return _client
