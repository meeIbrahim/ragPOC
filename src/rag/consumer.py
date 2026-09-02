import logging

from core import config_reader
from core.redis_manager import get_client
from rag import ingest

logger = logging.getLogger(__name__)

CONSUMER_NAME = "rag-worker-1"


def _ensure_group(client, stream: str, group: str) -> None:
    try:
        client.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _handle(client, redis_config, message_id, fields: dict) -> None:
    hash_id = fields.get("hash_id")
    object_path = fields.get("object_path")
    try:
        ingest.ingest_document(hash_id, object_path)
    except Exception:
        logger.exception("Failed to ingest %s (%s), will retry on redelivery", hash_id, object_path)
        return
    client.xack(redis_config.stream, redis_config.consumer_group, message_id)


def run() -> None:
    redis_config = config_reader.get_config().redis
    client = get_client()
    _ensure_group(client, redis_config.stream, redis_config.consumer_group)

    logger.info("Consumer started: stream=%s group=%s", redis_config.stream, redis_config.consumer_group)

    # Reclaim and reprocess this consumer's own pending entries from a prior crash
    # before serving new messages — XREADGROUP with ">" never returns these.
    pending = client.xreadgroup(
        redis_config.consumer_group,
        CONSUMER_NAME,
        {redis_config.stream: "0"},
        count=100,
    )
    for _stream_name, messages in pending:
        for message_id, fields in messages:
            _handle(client, redis_config, message_id, fields)

    while True:
        entries = client.xreadgroup(
            redis_config.consumer_group,
            CONSUMER_NAME,
            {redis_config.stream: ">"},
            count=1,
            block=5000,
        )
        if not entries:
            continue
        for _stream_name, messages in entries:
            for message_id, fields in messages:
                _handle(client, redis_config, message_id, fields)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    run()
