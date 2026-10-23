"""Task 27a (T-PR1..PR4 and the consumer end to end): behaviour the fakes assume, checked against
a real Redis 7.x. Skipped unless REDIS_URL is set:

    docker run -d -p 16379:6379 redis:7.4
    REDIS_URL=redis://localhost:16379 uv run pytest -m integration
"""

import json
import os
import threading
import time
import uuid
from datetime import timedelta

import pytest
import redis as redis_lib
from redis.exceptions import ResponseError

from rag.ingest.outcome import Failed, Indexed
from rag.transport.status_event import Reason
from rag.transport.status_publisher import StatusPublisher
from rag.transport.stream_consumer import ConsumerSettings, StreamConsumer
from support import FIXTURE_DIR

pytestmark = pytest.mark.integration

REDIS_URL = os.environ.get("REDIS_URL")
HASH = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


@pytest.fixture
def r():
    if not REDIS_URL:
        pytest.skip("REDIS_URL is not set")
    client = redis_lib.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        client.ping()
    except redis_lib.RedisError as exc:
        pytest.skip(f"redis not reachable at {REDIS_URL}: {exc}")
    yield client
    client.close()


@pytest.fixture
def streams(r):
    suffix = uuid.uuid4().hex[:8]
    names = (f"test.ingest.{suffix}", f"test.status.{suffix}")
    yield names
    r.delete(*names)


def ingest_payload(attempts=0) -> str:
    document = json.loads((FIXTURE_DIR / "ingest_event.json").read_text()) | {"attempts": attempts}
    return json.dumps(document)


def add_ingest(r, stream, attempts=0) -> str:
    return r.xadd(stream, {"event_type": "IngestionRequest", "payload": ingest_payload(attempts)})


def settings(stream, **changes):
    base = dict(
        stream=stream, group="rag-it", min_idle=timedelta(milliseconds=300), recovery_interval=timedelta(milliseconds=200),
        poison_threshold=3, cleanup_idle=timedelta(milliseconds=400), job_timeout=timedelta(seconds=5),
        read_block=timedelta(milliseconds=100), retry_delay=0.02, publish_attempts=3,
    )
    return ConsumerSettings(**(base | changes))


class Processor:
    def __init__(self, outcome=Indexed(0), fail_first=0):
        self.outcome = outcome
        self.calls = []
        self.fail_first = fail_first

    def process(self, event, deadline):
        self.calls.append(event.attempts)
        if len(self.calls) <= self.fail_first:
            raise RuntimeError("bad")
        return self.outcome


class NoWatchdog:
    def arm(self):
        pass

    def disarm(self):
        pass


def make(r, streams, processor=None, name=None, **changes):
    ingest, status = streams
    return StreamConsumer(
        r, settings(ingest, **changes), StatusPublisher(r, status, 1000), processor or Processor(), NoWatchdog(),
        name=name,
    )


def wait_for(condition, timeout=10.0):
    end = time.time() + timeout
    while time.time() < end:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


class Running:
    """Runs a consumer in a thread and stops it afterwards."""

    def __init__(self, consumer):
        self.consumer = consumer
        self.thread = threading.Thread(target=consumer.run, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self.consumer

    def __exit__(self, *exc):
        self.consumer.stop()
        self.thread.join(5)


# ---- T-PR1: XAUTOCLAIM

def test_xautoclaim_without_justid_returns_entries_with_fields_and_bumps_the_count(r, streams):
    ingest, _ = streams
    r.xgroup_create(ingest, "g", id="0", mkstream=True)
    entry_id = add_ingest(r, ingest)
    r.xreadgroup("g", "dead", {ingest: ">"}, count=1)
    time.sleep(0.15)

    response = r.xautoclaim(ingest, "g", "me", 100, start_id="0-0", count=1, justid=False)
    assert len(response) == 3  # [next cursor, entries, deleted ids] on Redis 7
    next_id, entries, deleted = response
    assert next_id == "0-0" and deleted == []
    (claimed_id, fields), = entries
    assert claimed_id == entry_id
    assert fields["event_type"] == "IngestionRequest" and json.loads(fields["payload"])["hash"] == HASH


def test_the_delivery_count_rises_from_1_to_2_after_a_claim(r, streams):
    ingest, _ = streams
    r.xgroup_create(ingest, "g", id="0", mkstream=True)
    entry_id = add_ingest(r, ingest)
    r.xreadgroup("g", "dead", {ingest: ">"}, count=1)
    time.sleep(0.15)

    def deliveries():
        (item,) = r.xpending_range(ingest, "g", min=entry_id, max=entry_id, count=1)
        return item["times_delivered"]

    assert deliveries() == 1
    r.xautoclaim(ingest, "g", "me", 100, start_id="0-0", count=1)
    assert deliveries() == 2


def test_xautoclaim_reports_entries_deleted_from_the_stream(r, streams):
    ingest, _ = streams
    r.xgroup_create(ingest, "g", id="0", mkstream=True)
    entry_id = add_ingest(r, ingest)
    r.xreadgroup("g", "dead", {ingest: ">"}, count=1)
    r.xdel(ingest, entry_id)  # what stream trimming does to a pending entry
    time.sleep(0.15)
    _, entries, deleted = r.xautoclaim(ingest, "g", "me", 100, start_id="0-0", count=1)
    assert entries == [] and deleted == [entry_id]


# ---- T-PR2: XPENDING

def test_xpending_range_for_one_id_reports_times_delivered_and_nothing_for_an_unknown_id(r, streams):
    ingest, _ = streams
    r.xgroup_create(ingest, "g", id="0", mkstream=True)
    entry_id = add_ingest(r, ingest)
    r.xreadgroup("g", "c", {ingest: ">"}, count=1)
    assert r.xpending_range(ingest, "g", min=entry_id, max=entry_id, count=1)[0]["times_delivered"] == 1
    assert r.xpending_range(ingest, "g", min="1-1", max="1-1", count=1) == []


# ---- T-PR3: XINFO CONSUMERS and cleanup

def test_xinfo_consumers_reports_idle_and_pending_and_cleanup_uses_them(r, streams):
    ingest, _ = streams
    consumer = make(r, streams, name="me")
    consumer.ensure_group()
    r.xgroup_createconsumer(ingest, "rag-it", "idle-empty")
    r.xgroup_createconsumer(ingest, "rag-it", "me")
    add_ingest(r, ingest)
    r.xreadgroup("rag-it", "busy", {ingest: ">"}, count=1)  # busy now holds one pending entry
    time.sleep(0.6)
    r.xgroup_createconsumer(ingest, "rag-it", "fresh")

    info = {c["name"]: c for c in r.xinfo_consumers(ingest, "rag-it")}
    assert info["busy"]["pending"] == 1 and info["idle-empty"]["pending"] == 0
    assert info["idle-empty"]["idle"] >= 500 and info["fresh"]["idle"] < 400

    consumer._cleanup_consumers()
    remaining = {c["name"] for c in r.xinfo_consumers(ingest, "rag-it")}
    assert remaining == {"me", "busy", "fresh"}  # only the idle consumer with nothing pending went
    assert r.xpending(ingest, "rag-it")["pending"] == 1  # busy's entry is intact


# ---- T-PR4: BUSYGROUP

def test_creating_the_group_twice_is_a_busygroup_error_that_ensure_group_swallows(r, streams):
    ingest, _ = streams
    consumer = make(r, streams)
    r.xgroup_create(ingest, "rag-it", id="0", mkstream=True)
    with pytest.raises(ResponseError, match="BUSYGROUP"):
        r.xgroup_create(ingest, "rag-it", id="0", mkstream=True)
    consumer.ensure_group()  # must not raise


def test_the_group_starts_at_zero_so_entries_published_earlier_are_delivered(r, streams):
    ingest, status = streams
    add_ingest(r, ingest)  # published before any group exists
    with Running(make(r, streams)):
        assert wait_for(lambda: r.xlen(status) == 1)


# ---- hostile bytes (review round 1: a non-UTF-8 entry crashed the consumer forever)

@pytest.fixture
def prod(streams):
    """A client built exactly like the entry point builds it."""
    from rag.consumer import make_redis
    from rag.core import config_reader

    host_port = REDIS_URL.split("//", 1)[1].split("/")[0].split(":")
    config = config_reader.load()
    client = make_redis(config, host=host_port[0], port=int(host_port[1]))
    yield client
    client.close()


def add_raw(r, stream, payload: bytes) -> str:
    raw = redis_lib.Redis.from_url(REDIS_URL)  # bytes in, no decoding
    try:
        return raw.xadd(stream, {b"event_type": b"IngestionRequest", b"payload": payload}).decode()
    finally:
        raw.close()


def test_a_non_utf8_entry_is_acked_without_crashing_on_the_read_path(r, streams, prod):
    ingest, status = streams
    add_raw(r, ingest, b"\xff\xfe not json")
    consumer = StreamConsumer(prod, settings(ingest), StatusPublisher(prod, status, 1000), Processor(), NoWatchdog())
    consumer.ensure_group()
    consumer.read_once()  # must not raise UnicodeDecodeError
    assert r.xpending(ingest, "rag-it")["pending"] == 0
    assert r.xlen(status) == 0  # an entry that is not an event produces no status


def test_a_non_utf8_entry_is_acked_without_crashing_on_the_reclaim_path(r, streams, prod):
    ingest, status = streams
    r.xgroup_create(ingest, "rag-it", id="0", mkstream=True)
    add_raw(r, ingest, b"\xff\xfe")
    prod.xreadgroup("rag-it", "dead", {ingest: ">"}, count=1)  # the plain `r` client would choke on it
    time.sleep(0.4)
    consumer = StreamConsumer(prod, settings(ingest), StatusPublisher(prod, status, 1000), Processor(), NoWatchdog())
    consumer.recover_once()  # must not raise
    assert r.xpending(ingest, "rag-it")["pending"] == 0


# ---- renewing a held entry (XCLAIM ... JUSTID)

def test_renewing_an_entry_resets_its_idle_time_but_not_its_delivery_count(r, streams):
    ingest, _ = streams
    r.xgroup_create(ingest, "rag-it", id="0", mkstream=True)
    entry_id = add_ingest(r, ingest)
    r.xreadgroup("rag-it", "me", {ingest: ">"}, count=1)
    time.sleep(0.4)
    before = r.xpending_range(ingest, "rag-it", min=entry_id, max=entry_id, count=1)[0]
    assert before["time_since_delivered"] >= 400

    make(r, streams, name="me")._renew(entry_id)

    after = r.xpending_range(ingest, "rag-it", min=entry_id, max=entry_id, count=1)[0]
    assert after["times_delivered"] == before["times_delivered"] == 1  # JUSTID: not a new delivery
    assert after["time_since_delivered"] < 200  # idle was reset: nobody may reclaim it yet


# ---- the poison boundary, exactly (K=3: the 4th delivery)

def reclaim_with(r, streams, extra_claims):
    ingest, status = streams
    r.xgroup_create(ingest, "rag-it", id="0", mkstream=True)
    entry_id = add_ingest(r, ingest, attempts=1)
    r.xreadgroup("rag-it", "dead", {ingest: ">"}, count=1)  # delivery 1
    for _ in range(extra_claims):
        r.xclaim(ingest, "rag-it", "dead", 0, [entry_id])  # deliveries 2, 3, ...
    time.sleep(0.4)
    processor = Processor()
    make(r, streams, processor).recover_once()  # the consumer's own claim is one more delivery
    return processor, [json.loads(f["payload"]) for _, f in r.xrange(status)]


def test_the_third_delivery_is_still_processed(r, streams):
    processor, statuses = reclaim_with(r, streams, extra_claims=1)  # read + 1 claim + ours = 3
    assert processor.calls == [1]
    assert [s["status"] for s in statuses] == ["INDEXED"]


def test_the_fourth_delivery_is_poison(r, streams):
    processor, statuses = reclaim_with(r, streams, extra_claims=2)  # read + 2 claims + ours = 4
    assert processor.calls == []
    assert [(s["reason"], s["retryable"], s["attempts"]) for s in statuses] == [("POISON_MESSAGE", False, 1)]


# ---- the consumer end to end

def test_a_new_entry_is_processed_published_and_acknowledged(r, streams):
    ingest, status = streams
    add_ingest(r, ingest, attempts=2)
    with Running(make(r, streams, Processor(Indexed(5)))):
        assert wait_for(lambda: r.xlen(status) == 1)
        assert wait_for(lambda: r.xpending(ingest, "rag-it")["pending"] == 0)
    (_, fields), = r.xrange(status)
    assert fields["event_type"] == "IngestionStatus"
    payload = json.loads(fields["payload"])
    assert (payload["status"], payload["attempts"], payload["chunk_count"], payload["hash"]) == ("INDEXED", 2, 5, HASH)


def test_the_consumer_name_is_new_per_instance_and_registered_in_the_group(r, streams):
    ingest, _ = streams
    first, second = make(r, streams), make(r, streams)
    assert first.name != second.name
    first.ensure_group()
    r.xgroup_createconsumer(ingest, "rag-it", first.name)
    assert first.name in {c["name"] for c in r.xinfo_consumers(ingest, "rag-it")}


def test_an_entry_a_dead_consumer_left_pending_is_recovered_after_min_idle(r, streams):
    ingest, status = streams
    r.xgroup_create(ingest, "rag-it", id="0", mkstream=True)
    add_ingest(r, ingest, attempts=1)
    r.xreadgroup("rag-it", "crashed-process", {ingest: ">"}, count=1)  # read, then the process died

    with Running(make(r, streams, Processor(Indexed(0)))):
        assert wait_for(lambda: r.xlen(status) == 1)
        assert wait_for(lambda: r.xpending(ingest, "rag-it")["pending"] == 0)
    assert json.loads(r.xrange(status)[0][1]["payload"])["attempts"] == 1
    # the dead consumer has no pending entries left, so cleanup removes it
    assert wait_for(lambda: "crashed-process" not in {c["name"] for c in r.xinfo_consumers(ingest, "rag-it")}, 5), (
        "the dead consumer, with nothing pending any more, was not cleaned up"
    )


def test_a_poison_entry_is_reported_with_the_echoed_attempt_and_acked(r, streams):
    ingest, status = streams
    r.xgroup_create(ingest, "rag-it", id="0", mkstream=True)
    entry_id = add_ingest(r, ingest, attempts=2)
    r.xreadgroup("rag-it", "dead", {ingest: ">"}, count=1)
    for _ in range(3):  # XCLAIM raises the delivery count: 1 read + 3 claims = 4, and ours makes 5
        r.xclaim(ingest, "rag-it", "dead", 0, [entry_id])

    processor = Processor()
    with Running(make(r, streams, processor)):
        assert wait_for(lambda: r.xlen(status) == 1)
        assert wait_for(lambda: r.xpending(ingest, "rag-it")["pending"] == 0)
    payload = json.loads(r.xrange(status)[0][1]["payload"])
    assert (payload["status"], payload["retryable"], payload["reason"], payload["attempts"]) == ("FAILED", False, "POISON_MESSAGE", 2)
    assert processor.calls == []  # never processed again


# ---- the group disappears under a running consumer (found by reviewing the Go worker)

def test_the_consumer_keeps_working_after_its_group_and_stream_are_deleted(r, streams):
    ingest, status = streams
    with Running(make(r, streams)):
        add_ingest(r, ingest)
        assert wait_for(lambda: r.xlen(status) == 1)
        r.delete(ingest)  # what a Redis restart without the group in its snapshot amounts to
        add_ingest(r, ingest)
        assert wait_for(lambda: r.xlen(status) == 2), "the consumer never recovered from the lost group"


# ---- a backlog left by a crash is recovered entry by entry

def test_a_backlog_of_pending_entries_is_recovered_one_at_a_time_and_all_reported(r, streams):
    ingest, status = streams
    r.xgroup_create(ingest, "rag-it", id="0", mkstream=True)
    ids = [add_ingest(r, ingest, attempts=i) for i in range(4)]
    for _ in ids:  # all four are pending under a dead consumer, as after a crash with a backlog
        r.xreadgroup("rag-it", "dead", {ingest: ">"}, count=1)

    processor = Processor(Failed(True, Reason.INTERNAL, "x"))
    with Running(make(r, streams, processor)):
        assert wait_for(lambda: r.xlen(status) == 4)
    attempts = sorted(json.loads(fields["payload"])["attempts"] for _, fields in r.xrange(status))
    assert attempts == [0, 1, 2, 3]
