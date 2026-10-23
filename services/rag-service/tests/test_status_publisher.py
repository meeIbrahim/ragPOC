"""Task 25 (T-P2): publishing status events to the status stream."""

import json

import jsonschema
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from rag.transport.status_event import Reason, StatusEvent
from rag.transport.status_publisher import StatusPublisher
from support import FIXTURE_DIR

HASH = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
STATUS_SCHEMA = json.loads((FIXTURE_DIR.parent / "status_event.schema.json").read_text())


class FakeRedis:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def xadd(self, name, fields, **kwargs):
        self.calls.append((name, fields, kwargs))
        if self.error:
            raise self.error
        return "1700000000000-0"


def test_publishes_the_entry_fields_to_the_status_stream():
    redis = FakeRedis()
    StatusPublisher(redis, "ingest.status", 324000).publish(StatusEvent.indexed(HASH, 2, chunk_count=7))

    (name, fields, kwargs), = redis.calls
    assert name == "ingest.status"
    assert set(fields) == {"event_type", "payload"}
    assert fields["event_type"] == "IngestionStatus"
    payload = json.loads(fields["payload"])
    assert (payload["status"], payload["attempts"], payload["chunk_count"]) == ("INDEXED", 2, 7)


def test_the_stream_is_capped_approximately_like_the_ingest_stream():
    redis = FakeRedis()
    StatusPublisher(redis, "ingest.status", 324000).publish(StatusEvent.indexed(HASH, 0))
    assert redis.calls[0][2] == {"maxlen": 324000, "approximate": True}


def test_returns_the_entry_id():
    assert StatusPublisher(FakeRedis(), "s", 10).publish(StatusEvent.indexed(HASH, 0)) == "1700000000000-0"


def test_the_published_payload_validates_against_the_schema():
    redis = FakeRedis()
    publisher = StatusPublisher(redis, "s", 10)
    publisher.publish(StatusEvent.failed(HASH, 1, retryable=True, reason=Reason.DEPENDENCY_UNAVAILABLE, detail="qdrant timed out"))
    publisher.publish(StatusEvent.failed(HASH, 2, retryable=False, reason=Reason.POISON_MESSAGE))
    for _, fields, _ in redis.calls:
        assert jsonschema.Draft202012Validator(STATUS_SCHEMA).is_valid(json.loads(fields["payload"]))


def test_a_redis_error_reaches_the_caller_so_it_can_leave_the_entry_pending():
    publisher = StatusPublisher(FakeRedis(error=RedisConnectionError("down")), "s", 10)
    with pytest.raises(RedisConnectionError):
        publisher.publish(StatusEvent.indexed(HASH, 0))


@pytest.mark.parametrize("maxlen", [0, -1])
def test_a_non_positive_maxlen_is_rejected(maxlen):
    with pytest.raises(ValueError):
        StatusPublisher(FakeRedis(), "s", maxlen)


def test_an_empty_stream_name_is_rejected():
    with pytest.raises(ValueError):
        StatusPublisher(FakeRedis(), "", 10)
