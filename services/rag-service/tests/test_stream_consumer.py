"""Task 27 (T-P4..T-P9, T-P12): the ingest StreamConsumer against a scripted fake Redis that
records every call in order."""

import json
import re
import threading
from datetime import timedelta

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from rag.ingest.errors import TransientError
from rag.ingest.outcome import Failed, Indexed
from rag.transport.status_event import Reason
from rag.transport.status_publisher import StatusPublisher
from rag.transport.stream_consumer import ConsumerSettings, StreamConsumer, new_consumer_name
from support import FIXTURE_DIR

STREAM, GROUP, STATUS_STREAM = "ingest.docs", "rag-pdf-fixed", "ingest.status"
HASH = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
SETTINGS = ConsumerSettings(
    stream=STREAM,
    group=GROUP,
    min_idle=timedelta(seconds=10),
    recovery_interval=timedelta(seconds=2),
    poison_threshold=3,
    cleanup_idle=timedelta(seconds=15),
    job_timeout=timedelta(seconds=5),
    read_block=timedelta(seconds=2),
    retry_delay=0.001,
    publish_attempts=3,
)


def payload(attempts=0, **changes) -> str:
    document = json.loads((FIXTURE_DIR / "ingest_event.json").read_text()) | {"attempts": attempts} | changes
    return json.dumps(document)


def entry(entry_id="1-0", attempts=0, **changes):
    return (entry_id, {"event_type": "IngestionRequest", "payload": payload(attempts, **changes)})


class Crashed(BaseException):
    """The process going away: not an Exception, so nothing may swallow it."""


class FakeRedis:
    """Pops scripted responses per command; every call lands in one ordered log."""

    def __init__(self):
        self.log = []
        self.group_errors = []
        self.reads = []
        self.claims = []
        self.xadd_errors = []
        self.ack_errors = []
        self.pending = {}  # entry id -> times_delivered
        self.pending_error = None
        self.consumers = []
        self.xadd_calls = []
        self.kwargs = {}

    def _next(self, queue, default):
        if queue:
            item = queue.pop(0)
            if isinstance(item, BaseException):
                raise item
            if callable(item):
                return item()
            return item
        return default

    def xgroup_create(self, name, groupname, id="$", mkstream=False, **kw):
        self.log.append(f"group_create:{name}:{groupname}:{id}:{mkstream}")
        self._next(self.group_errors, None)

    def xreadgroup(self, groupname, consumername, streams, count=None, block=None, **kw):
        self.log.append(f"read:{consumername}:{list(streams.values())[0]}:count={count}:block={block}")
        self.kwargs["read"] = {"count": count, "block": block, "streams": streams}
        return self._next(self.reads, [])

    def xautoclaim(self, name, groupname, consumername, min_idle_time, start_id="0-0", count=None, justid=False):
        self.log.append(f"claim:{consumername}:idle={min_idle_time}:start={start_id}:count={count}:justid={justid}")
        self.kwargs["claim"] = {"justid": justid, "count": count, "min_idle": min_idle_time}
        return self._next(self.claims, ["0-0", []])

    def xpending_range(self, name, groupname, min, max, count, consumername=None, idle=None):
        self.log.append(f"pending:{min}")
        if self.pending_error:
            raise self.pending_error
        if min not in self.pending:
            return []
        return [{"message_id": min, "consumer": "x", "time_since_delivered": 0, "times_delivered": self.pending[min]}]

    def xack(self, name, groupname, *ids):
        self.log.append(f"ack:{','.join(ids)}")
        self._next(self.ack_errors, None)
        return len(ids)

    def xinfo_consumers(self, name, groupname):
        self.log.append("consumers")
        return list(self.consumers)

    def xgroup_delconsumer(self, name, groupname, consumername):
        self.log.append(f"delconsumer:{consumername}")
        return 0

    def xadd(self, name, fields, **kw):
        self.log.append(f"xadd:{name}")
        self._next(self.xadd_errors, None)
        self.xadd_calls.append((name, fields, kw))  # only a publish that worked
        return "9-0"


class FakeWatchdog:
    def __init__(self, log):
        self.log = log
        self.armed = False

    def arm(self):
        self.armed = True
        self.log.append("watchdog:arm")

    def disarm(self):
        self.armed = False
        self.log.append("watchdog:disarm")


class Processor:
    def __init__(self, redis, outcome=Indexed(0)):
        self.redis = redis
        self.outcome = outcome
        self.events = []
        self.deadlines = []

    def process(self, event, deadline):
        self.redis.log.append(f"process:{event.attempts}")
        self.events.append(event)
        self.deadlines.append(deadline)
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


def build(redis=None, outcome=Indexed(0), settings=SETTINGS, sleeps=None):
    redis = redis or FakeRedis()
    watchdog = FakeWatchdog(redis.log)
    processor = Processor(redis, outcome)
    sleeps = sleeps if sleeps is not None else []

    def sleep(seconds):
        sleeps.append(seconds)
        return not consumer._stop.is_set()

    consumer = StreamConsumer(
        redis, settings, StatusPublisher(redis, STATUS_STREAM, 1000), processor, watchdog,
        name="host/ingest/test", sleep=sleep,
    )
    return consumer, redis, processor, watchdog, sleeps


def published(redis):
    return [json.loads(fields["payload"]) for _, fields, _ in redis.xadd_calls]


# ---- consumer name (L19)

def test_the_name_has_the_kind_and_is_new_for_every_process():
    pattern = re.compile(r"^.+/ingest/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$")
    first, second = new_consumer_name(), new_consumer_name()
    assert pattern.match(first) and pattern.match(second) and first != second


# ---- the group (T-P4)

def test_the_group_is_created_at_the_start_of_the_stream_and_the_stream_is_created():
    consumer, redis, *_ = build()
    consumer.ensure_group()
    assert redis.log == [f"group_create:{STREAM}:{GROUP}:0:True"]


def test_an_existing_group_is_not_an_error():
    consumer, redis, *_ = build()
    redis.group_errors = [ResponseError("BUSYGROUP Consumer Group name already exists")]
    consumer.ensure_group()


def test_any_other_group_error_is_raised():
    consumer, redis, *_ = build()
    redis.group_errors = [ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")]
    with pytest.raises(ResponseError, match="WRONGTYPE"):
        consumer.ensure_group()


def test_run_retries_the_group_until_redis_answers():
    consumer, redis, _, _, sleeps = build()
    redis.group_errors = [RedisConnectionError("down"), RedisConnectionError("down")]
    redis.reads = [stop_read(consumer)]
    consumer.run()
    assert [e for e in redis.log if e.startswith("group_create")] == [f"group_create:{STREAM}:{GROUP}:0:True"] * 3
    assert len(sleeps) >= 2


def drive(consumer, redis, steps):
    """Run the loop; each scripted read response is consumed in order and the last one stops it."""
    redis.reads = list(steps) + [stop_read(consumer)]
    consumer.run()


def stop_read(consumer):
    """A scripted read whose turn stops the consumer and fails like a dropped connection, so
    run() ends cleanly: the loop logs the error, backs off, and then sees the stop flag."""

    def action():
        consumer.stop()
        raise RedisConnectionError("stop")

    return action


# ---- reading new entries

def test_a_new_entry_is_read_with_greater_than_one_at_a_time():
    consumer, redis, processor, *_ = build()
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    consumer.read_once()
    assert redis.kwargs["read"]["streams"] == {STREAM: ">"}
    assert redis.kwargs["read"]["count"] == 1  # one job at a time per process
    assert len(processor.events) == 1


def test_the_block_never_exceeds_the_recovery_interval():
    # Otherwise a single-process deployment would never reach its recovery pass (PRD 5.3).
    consumer, redis, *_ = build(settings=ConsumerSettings(**{**SETTINGS.__dict__, "read_block": timedelta(seconds=30)}))
    consumer.read_once()
    assert redis.kwargs["read"]["block"] == 2000


def test_a_sub_millisecond_block_is_clamped_up():
    # redis-py would send BLOCK 0, which waits forever.
    consumer, redis, *_ = build(settings=ConsumerSettings(**{**SETTINGS.__dict__, "recovery_interval": timedelta(microseconds=500)}))
    consumer.read_once()
    assert redis.kwargs["read"]["block"] >= 1


def test_nothing_to_read_is_not_an_error():
    consumer, redis, processor, *_ = build()
    redis.reads = [None]
    consumer.read_once()
    assert processor.events == []


# ---- ack order (T-P5, L17)

def test_the_status_is_published_before_the_entry_is_acknowledged():
    consumer, redis, *_ = build()
    redis.reads = [[[STREAM, [entry("5-0")]]]]
    consumer.read_once()
    steps = [e for e in redis.log if e.startswith(("process", "xadd", "ack"))]
    assert steps == ["process:0", f"xadd:{STATUS_STREAM}", "ack:5-0"]


def test_a_failed_publish_leaves_the_entry_pending_after_a_few_tries():
    consumer, redis, _, _, sleeps = build()
    redis.xadd_errors = [RedisConnectionError("down")] * 3
    redis.reads = [[[STREAM, [entry("5-0")]]]]
    consumer.read_once()
    assert len([e for e in redis.log if e.startswith("xadd")]) == 3
    assert not any(e.startswith("ack") for e in redis.log)
    assert len(sleeps) >= 2


def test_a_publish_that_works_on_a_later_try_is_acknowledged():
    consumer, redis, *_ = build()
    redis.xadd_errors = [RedisConnectionError("blip")]
    redis.reads = [[[STREAM, [entry("5-0")]]]]
    consumer.read_once()
    assert "ack:5-0" in redis.log
    assert len(published(redis)) == 1


def test_a_failed_ack_is_not_fatal():
    consumer, redis, *_ = build()
    redis.ack_errors = [RedisConnectionError("down")]
    redis.reads = [[[STREAM, [entry("5-0")]]]]
    consumer.read_once()  # must not raise; the entry is redelivered later and Go's CAS dedupes


# ---- attempts are echoed, never kept (T-P6, L12)

@pytest.mark.parametrize("attempts", [0, 1, 2, 5])
@pytest.mark.parametrize(
    "outcome",
    [Indexed(12), Failed(True, Reason.DEPENDENCY_UNAVAILABLE, "x"), Failed(False, Reason.UNSUPPORTED_TYPE, None), Failed(True, Reason.JOB_TIMEOUT, None)],
    ids=["indexed", "retryable", "permanent", "timeout"],
)
def test_attempts_are_echoed_unchanged(attempts, outcome):
    consumer, redis, *_ = build(outcome=outcome)
    redis.reads = [[[STREAM, [entry("1-0", attempts=attempts)]]]]
    consumer.read_once()
    (status,) = published(redis)
    assert status["attempts"] == attempts
    assert status["hash"] == HASH


def test_the_outcome_maps_to_the_status_fields():
    consumer, redis, *_ = build(outcome=Failed(True, Reason.CORRUPT_FILE, "short detail"))
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    consumer.read_once()
    (status,) = published(redis)
    assert (status["status"], status["retryable"], status["reason"], status["detail"]) == ("FAILED", True, "CORRUPT_FILE", "short detail")

    consumer, redis, *_ = build(outcome=Indexed(12))
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    consumer.read_once()
    (status,) = published(redis)
    assert (status["status"], status["chunk_count"]) == ("INDEXED", 12)


def test_an_unclassified_error_in_the_job_is_a_retryable_internal_failure():
    consumer, redis, *_ = build(outcome=ValueError("contains document text"))
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    consumer.read_once()
    (status,) = published(redis)
    assert (status["status"], status["retryable"], status["reason"]) == ("FAILED", True, "INTERNAL")
    assert "document text" not in json.dumps(status)


def test_a_typed_transient_error_keeps_its_reason():
    consumer, redis, *_ = build(outcome=TransientError(Reason.DEPENDENCY_UNAVAILABLE, "qdrant down"))
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    consumer.read_once()
    assert published(redis)[0]["reason"] == "DEPENDENCY_UNAVAILABLE"


# ---- unparseable and unexpected entries are acknowledged, never retried (T-P7)

@pytest.mark.parametrize(
    "fields",
    [
        {"event_type": "IngestionRequest", "payload": "{"},
        {"event_type": "IngestionRequest", "payload": "[]"},
        {"event_type": "IngestionRequest"},
        {"event_type": "SomethingElse", "payload": payload()},
        {"payload": payload()},
        {},
        {"event_type": "IngestionRequest", "payload": "x" * (2 * 1024 * 1024)},
        {"event_type": "IngestionRequest", "payload": payload(schema_version=2)},
    ],
    ids=["bad-json", "not-object", "no-payload", "wrong-type", "no-type", "empty", "oversized", "future-version"],
)
def test_an_entry_that_can_never_be_processed_is_acked_without_running_a_job(fields):
    consumer, redis, processor, *_ = build()
    redis.reads = [[[STREAM, [("7-0", fields)]]]]
    consumer.read_once()
    assert processor.events == []
    assert redis.xadd_calls == []
    assert "ack:7-0" in redis.log


# ---- recovery: claims one at a time, without JUSTID (T-P9)

def test_recovery_claims_without_justid_one_entry_at_a_time():
    consumer, redis, processor, *_ = build()
    redis.claims = [["0-0", [entry("3-0")]], ["0-0", []]]
    consumer.recover_once()
    claim = redis.kwargs["claim"]
    assert claim["justid"] is False  # the delivery count must increment and the payload is needed
    assert claim["count"] == 1  # min_idle protects one job; a claimed batch would outlive it
    assert claim["min_idle"] == 10_000
    assert len(processor.events) == 1


def test_recovery_keeps_claiming_until_nothing_is_stale():
    consumer, redis, processor, *_ = build()
    redis.pending = {"1-0": 2, "2-0": 2, "3-0": 2}
    redis.claims = [["0-0", [entry("1-0")]], ["0-0", [entry("2-0")]], ["0-0", [entry("3-0")]], ["0-0", []]]
    consumer.recover_once()
    assert len(processor.events) == 3
    assert [e for e in redis.log if e.startswith("ack")] == ["ack:1-0", "ack:2-0", "ack:3-0"]


def test_recovery_follows_the_cursor_past_entries_that_are_not_idle():
    # One XAUTOCLAIM call scans only a few pending entries; a stale one further down must not be
    # starved behind entries that belong to a busy consumer.
    consumer, redis, processor, *_ = build()
    redis.pending = {"9-0": 2}
    redis.claims = [["4-0", []], ["8-0", []], ["0-0", [entry("9-0")]], ["0-0", []]]
    consumer.recover_once()
    starts = [re.search(r"start=([^:]+)", e).group(1) for e in redis.log if e.startswith("claim")]
    assert starts[:3] == ["0-0", "4-0", "8-0"]
    assert len(processor.events) == 1


def test_recovery_is_bounded_per_pass_so_new_entries_are_still_read():
    consumer, redis, processor, *_ = build()
    redis.claims = [["0-0", [entry(f"{i}-0")]] for i in range(1, 40)]
    redis.pending = {f"{i}-0": 2 for i in range(1, 40)}
    consumer.recover_once()
    assert len(processor.events) == SETTINGS.max_recovery_jobs


def test_a_failing_claimed_entry_does_not_block_the_next_one():
    # The Go worker lost events by abandoning the rest of a batch after one failure. Here one
    # entry's publish fails; its successor must still be handled and acknowledged.
    consumer, redis, processor, *_ = build()
    redis.pending = {"1-0": 2, "2-0": 2}
    redis.claims = [["0-0", [entry("1-0")]], ["0-0", [entry("2-0")]], ["0-0", []]]
    redis.xadd_errors = [RedisConnectionError("x")] * 3  # all three tries for the first entry
    consumer.recover_once()
    assert len(processor.events) == 2
    acks = [e for e in redis.log if e.startswith("ack")]
    assert acks == ["ack:2-0"]


def test_deleted_entries_reported_by_xautoclaim_are_logged(caplog):
    consumer, redis, *_ = build()
    redis.claims = [["0-0", [], ["4-0", "5-0"]]]
    with caplog.at_level("ERROR"):
        consumer.recover_once()
    assert "4-0" in caplog.text and "5-0" in caplog.text


def test_a_claimed_entry_with_no_fields_was_deleted_and_is_acknowledged():
    consumer, redis, processor, *_ = build()
    redis.claims = [["0-0", [("6-0", None)]], ["0-0", []]]
    consumer.recover_once()
    assert processor.events == [] and "ack:6-0" in redis.log


# ---- poison (L14)

def test_a_reclaimed_entry_over_the_threshold_is_reported_as_poison_and_acked():
    consumer, redis, processor, *_ = build()
    redis.pending = {"1-0": 4}  # delivered 4 times: over the threshold of 3
    redis.claims = [["0-0", [entry("1-0", attempts=2)]], ["0-0", []]]
    consumer.recover_once()
    assert processor.events == []  # never processed again
    (status,) = published(redis)
    assert (status["status"], status["retryable"], status["reason"], status["attempts"]) == ("FAILED", False, "POISON_MESSAGE", 2)
    assert status["hash"] == HASH
    steps = [e for e in redis.log if e.startswith(("xadd", "ack"))]
    assert steps == [f"xadd:{STATUS_STREAM}", "ack:1-0"]


def test_an_entry_at_exactly_the_threshold_is_still_processed():
    consumer, redis, processor, *_ = build()
    redis.pending = {"1-0": 3}
    redis.claims = [["0-0", [entry("1-0")]], ["0-0", []]]
    consumer.recover_once()
    assert len(processor.events) == 1


def test_a_poison_entry_is_never_dropped_unless_its_status_was_published():
    consumer, redis, processor, *_ = build()
    redis.pending = {"1-0": 9}
    redis.claims = [["0-0", [entry("1-0")]], ["0-0", []]]
    redis.xadd_errors = [RedisConnectionError("down")] * 3
    consumer.recover_once()
    assert not any(e.startswith("ack") for e in redis.log)  # still pending: nothing is lost


def test_an_unparseable_reclaimed_entry_is_acked_without_a_status():
    consumer, redis, processor, *_ = build()
    redis.pending = {"1-0": 9}
    redis.claims = [["0-0", [("1-0", {"event_type": "IngestionRequest", "payload": "{"})]], ["0-0", []]]
    consumer.recover_once()
    assert redis.xadd_calls == [] and "ack:1-0" in redis.log


def test_when_the_delivery_count_cannot_be_read_the_entry_stays_pending():
    # Acking could drop a good event; processing blindly could loop a poison one.
    consumer, redis, processor, *_ = build()
    redis.pending_error = RedisConnectionError("down")
    redis.claims = [["0-0", [entry("1-0")]], ["0-0", []]]
    consumer.recover_once()
    assert processor.events == []
    assert not any(e.startswith("ack") for e in redis.log)


def test_a_new_entry_is_never_checked_for_poison():
    consumer, redis, processor, *_ = build()
    redis.pending = {"1-0": 99}
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    consumer.read_once()
    assert len(processor.events) == 1
    assert not any(e.startswith("pending") for e in redis.log)


# ---- consumer cleanup (T-P8, E8)

def test_cleanup_deletes_only_idle_consumers_without_pending_entries():
    consumer, redis, *_ = build()
    redis.consumers = [
        {"name": "host/ingest/test", "pending": 0, "idle": 999_999},  # me
        {"name": "dead-empty", "pending": 0, "idle": 20_000},  # delete
        {"name": "dead-busy", "pending": 2, "idle": 999_999},  # holds entries: keep
        {"name": "fresh", "pending": 0, "idle": 14_999},  # not idle long enough: keep
        {"name": "exactly", "pending": 0, "idle": 15_000},  # idle must EXCEED the threshold: keep
    ]
    consumer.recover_once()
    assert [e for e in redis.log if e.startswith("delconsumer")] == ["delconsumer:dead-empty"]


def test_cleanup_runs_after_the_claims_in_every_recovery_pass():
    consumer, redis, *_ = build()
    consumer.recover_once()
    kinds = [e.split(":")[0] for e in redis.log]
    assert kinds.index("claim") < kinds.index("consumers")


# ---- the job: deadline and watchdog (T-P11, T-P12)

def test_the_watchdog_wraps_exactly_the_job():
    consumer, redis, _, watchdog, _ = build()
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    consumer.read_once()
    steps = [e for e in redis.log if e.startswith(("watchdog", "process", "xadd", "ack"))]
    assert steps == ["watchdog:arm", "process:0", "watchdog:disarm", f"xadd:{STATUS_STREAM}", "ack:1-0"]


def test_the_watchdog_is_disarmed_even_when_the_job_takes_the_process_down():
    consumer, redis, _, watchdog, _ = build(outcome=Crashed())
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    with pytest.raises(Crashed):
        consumer.read_once()
    assert watchdog.armed is False
    assert redis.xadd_calls == [] and not any(e.startswith("ack") for e in redis.log)


def test_the_job_gets_a_deadline_of_job_timeout():
    consumer, redis, processor, *_ = build()
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    consumer.read_once()
    assert 4.9 < processor.deadlines[0].remaining() <= 5.0


# ---- the run loop

def test_the_loop_recovers_once_at_start_then_reads():
    consumer, redis, *_ = build()
    drive(consumer, redis, [])
    kinds = [e.split(":")[0] for e in redis.log]
    assert kinds[:3] == ["group_create", "claim", "consumers"]
    assert "read" in kinds


def test_recovery_repeats_every_interval():
    clock = {"now": 0.0}
    consumer, redis, *_ = build()
    consumer._clock = lambda: clock["now"]

    def advance(*_):
        clock["now"] += 1.0

    redis.reads = [[]] * 6 + [stop_read(consumer)]
    original = redis.xreadgroup

    def read(*a, **k):
        advance()
        return original(*a, **k)

    redis.xreadgroup = read
    consumer.run()
    claims = len([e for e in redis.log if e.startswith("claim")])
    assert claims >= 3  # at start, then at 2s and 4s of a 7s run


def test_a_read_error_backs_off_and_the_loop_continues():
    consumer, redis, processor, _, sleeps = build()
    drive(consumer, redis, [RedisConnectionError("reset"), [[STREAM, [entry("1-0")]]]])
    assert len(processor.events) == 1
    assert sleeps and sleeps[0] == SETTINGS.retry_delay


def test_stop_ends_the_loop_between_jobs():
    consumer, redis, processor, *_ = build()
    consumer.stop()
    consumer.run()
    assert processor.events == []


# ---- a vanished group is created again (found by reviewing the Go worker)

def test_the_group_is_recreated_when_a_read_reports_nogroup():
    consumer, redis, processor, *_ = build()
    drive(consumer, redis, [ResponseError("NOGROUP No such key or consumer group"), [[STREAM, [entry("1-0")]]]])
    assert len([e for e in redis.log if e.startswith("group_create")]) == 2
    assert len(processor.events) == 1


def test_the_group_is_recreated_when_a_claim_reports_nogroup():
    consumer, redis, *_ = build()
    redis.claims = [ResponseError("NOGROUP No such key or consumer group")]
    drive(consumer, redis, [])
    assert len([e for e in redis.log if e.startswith("group_create")]) >= 2


def test_other_response_errors_are_not_mistaken_for_a_lost_group():
    consumer, redis, *_ = build()
    drive(consumer, redis, [ResponseError("OOM command not allowed")])
    assert len([e for e in redis.log if e.startswith("group_create")]) == 1
