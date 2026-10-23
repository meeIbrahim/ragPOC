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
    call_timeout=1.0,
)


def payload(attempts=0, **changes) -> str:
    document = json.loads((FIXTURE_DIR / "ingest_event.json").read_text()) | {"attempts": attempts} | changes
    return json.dumps(document)


def entry(entry_id="1-0", attempts=0, **changes):
    return (entry_id, {"event_type": "IngestionRequest", "payload": payload(attempts, **changes)})


class RunawayLoop(BaseException):
    """A consumer loop that never stopped. A BaseException, so no handler under test swallows it."""


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
        self.renew_errors = []
        self.pending = {}  # entry id -> times_delivered (a claimed entry defaults to 2)
        self.gone = set()  # ids another consumer acked between our claim and the XPENDING
        self.pending_owner = "host/ingest/test"
        self.pending_error = None
        self.pending_errors = []  # raised once each, one per XPENDING call
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
        self.read_calls = getattr(self, "read_calls", 0) + 1
        if self.read_calls > 5000:
            raise RunawayLoop("the consumer loop never stopped")
        self.log.append(f"read:{consumername}:{list(streams.values())[0]}:count={count}:block={block}")
        self.kwargs["read"] = {"count": count, "block": block, "streams": streams}
        return self._next(self.reads, [])

    def xautoclaim(self, name, groupname, consumername, min_idle_time, start_id="0-0", count=None, justid=False):
        self.log.append(f"claim:{consumername}:idle={min_idle_time}:start={start_id}:count={count}:justid={justid}")
        self.kwargs["claim"] = {"justid": justid, "count": count, "min_idle": min_idle_time}
        return self._next(self.claims, ["0-0", []])

    def xpending_range(self, name, groupname, min, max, count, consumername=None, idle=None):
        self.log.append(f"pending:{min}")
        if self.pending_errors:
            raise self.pending_errors.pop(0)
        if self.pending_error:
            raise self.pending_error
        if min in self.gone:
            return []
        return [{
            "message_id": min, "consumer": self.pending_owner, "time_since_delivered": 0,
            "times_delivered": self.pending.get(min, 2),
        }]

    def xclaim(self, name, groupname, consumername, min_idle_time, message_ids, **kw):
        self.log.append(f"renew:{','.join(message_ids)}:justid={kw.get('justid')}")
        self._next(self.renew_errors, None)
        return list(message_ids)

    def xack(self, name, groupname, *ids):
        self.ack_calls = getattr(self, "ack_calls", 0) + 1
        if self.ack_calls > 5000:
            raise RunawayLoop("the consumer kept acknowledging and never stopped")
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
        self.xadd_total = getattr(self, "xadd_total", 0) + 1
        if self.xadd_total > 5000:
            raise RunawayLoop("the consumer kept publishing and never stopped")
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
        if len(sleeps) > 5000:
            raise RunawayLoop("the consumer kept sleeping and never stopped")
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


def test_an_entry_whose_delivery_count_cannot_be_read_does_not_block_the_next_one():
    # The Go worker lost events by abandoning the rest of a batch after one failure. An entry
    # that cannot be judged stays pending alone; its successor is still claimed and handled.
    consumer, redis, processor, *_ = build()
    redis.pending_errors = [RedisConnectionError("blip")]
    redis.claims = [["0-0", [entry("1-0")]], ["0-0", [entry("2-0")]], ["0-0", []]]
    consumer.recover_once()
    assert [e.attempts for e in processor.events] == [0]
    assert [e for e in redis.log if e.startswith("ack")] == ["ack:2-0"]


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
    assert claims == 4  # at t=0, then at 2s, 4s and 6s of the run


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


# =====================================================================================
# Review round 1: hostile entries, bounded retries, pausing when publishing is down
# =====================================================================================

from rag.ingest.errors import IngestError  # noqa: E402
from rag.ingest.outcome import run_job  # noqa: E402


# ---- the delivery count must belong to an entry we still own

def test_an_entry_acked_by_someone_else_after_the_claim_is_skipped():
    consumer, redis, processor, *_ = build()
    redis.gone = {"1-0"}
    redis.claims = [["0-0", [entry("1-0")]], ["0-0", []]]
    consumer.recover_once()
    assert processor.events == [] and not any(e.startswith("ack") for e in redis.log)


def test_an_entry_owned_by_another_consumer_is_skipped():
    consumer, redis, processor, *_ = build()
    redis.pending_owner = "someone-else"
    redis.claims = [["0-0", [entry("1-0")]], ["0-0", []]]
    consumer.recover_once()
    assert processor.events == []


# ---- nothing unexpected inside one entry's handling may take the loop down

def test_a_malformed_entry_shape_does_not_kill_the_loop():
    consumer, redis, processor, *_ = build()
    drive(consumer, redis, [[[STREAM, [("1-0", 5)]]], [[STREAM, [entry("2-0")]]]])  # fields is an int
    assert [e.attempts for e in processor.events] == [0]  # the next entry was still handled
    assert "ack:2-0" in redis.log


def test_a_decode_error_from_the_client_is_retried_not_fatal():
    # redis-py raises UnicodeDecodeError (a ValueError) while decoding a reply with bad bytes.
    consumer, redis, processor, _, sleeps = build()
    drive(consumer, redis, [UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"), [[STREAM, [entry("1-0")]]]])
    assert len(processor.events) == 1 and sleeps


def test_a_decode_error_during_a_claim_does_not_kill_the_loop():
    consumer, redis, processor, *_ = build()
    redis.claims = [UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")]
    drive(consumer, redis, [[[STREAM, [entry("1-0")]]]])
    assert len(processor.events) == 1  # reading carried on after the failed recovery pass
    assert "consumers" in redis.log  # idle-consumer cleanup still ran after the failed claim


def test_a_process_exit_inside_the_job_is_still_not_swallowed():
    consumer, redis, *_ = build(outcome=Crashed())
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    with pytest.raises(Crashed):
        consumer.run()


# ---- redis-py's own retries must not be what bounds publishing

def test_publishing_stops_retrying_when_the_entry_would_become_reclaimable():
    # The entry's idle time starts at its delivery. A new publish attempt may only start while a
    # whole call_timeout still fits before min_idle, or a second consumer could reclaim and re-run it.
    settings = ConsumerSettings(**{**SETTINGS.__dict__, "min_idle": timedelta(seconds=10), "call_timeout": 3.0, "publish_attempts": 5})
    now = {"t": 0.0}
    redis = FakeRedis()
    redis.xadd_errors = [RedisConnectionError("down")] * 20
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    consumer, *_ = build(redis, settings=settings)
    consumer._clock = lambda: now["t"]

    def sleep(seconds):
        now["t"] += 4.0
        return True

    consumer._sleep = sleep
    consumer.read_once()
    attempts = len([e for e in redis.log if e.startswith("xadd")])
    # Attempts start at t=0 and t=4. At t=8 the clock alone is still before min_idle (10s), but
    # a whole 3s call would end at 11s: too late. Without the call_timeout term this is 3.
    assert attempts == 2


def test_the_first_publish_attempt_is_always_made():
    settings = ConsumerSettings(**{**SETTINGS.__dict__, "min_idle": timedelta(seconds=1), "call_timeout": 60.0})
    consumer, redis, *_ = build(settings=settings)
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    consumer.read_once()
    assert len(published(redis)) == 1 and "ack:1-0" in redis.log


# ---- when only publishing fails, stop reading and claiming (counts must not grow)

def pause_script(failures):
    consumer, redis, processor, _, sleeps = build()
    redis.xadd_errors = [RedisConnectionError("OOM command not allowed when used memory > 'maxmemory'")] * failures
    return consumer, redis, processor, sleeps


def test_a_publish_that_keeps_failing_pauses_reading_until_it_works():
    consumer, redis, processor, sleeps = pause_script(5)  # 3 tries, then 2 more while held
    drive(consumer, redis, [[[STREAM, [entry("1-0")]]], [[STREAM, [entry("2-0")]]]])
    log = redis.log
    first_fail = log.index(f"xadd:{STATUS_STREAM}")
    ack_first = log.index("ack:1-0")
    between = log[first_fail:ack_first]
    assert not any(e.startswith(("read", "claim")) for e in between), between
    assert len(published(redis)) == 2  # the held outcome for 1-0, then the next entry's
    assert log.index("ack:1-0") < log.index("process:0", log.index("ack:1-0") + 1)


def test_the_held_entry_is_not_processed_again_while_waiting():
    consumer, redis, processor, _ = pause_script(4)
    drive(consumer, redis, [[[STREAM, [entry("1-0")]]]])
    assert len(processor.events) == 1  # the job ran once; only its status was retried


def test_recovery_claims_nothing_while_a_status_is_held():
    consumer, redis, processor, _ = pause_script(3)
    redis.claims = [["0-0", [entry("1-0")]], ["0-0", [entry("2-0")]]]
    redis.pending = {"1-0": 2, "2-0": 2}
    consumer.recover_once()
    assert len(processor.events) == 1  # 1-0 ran and its publish failed; 2-0 was not even claimed
    assert len([e for e in redis.log if e.startswith("claim")]) == 1


def test_a_held_poison_report_is_retried_before_anything_else():
    consumer, redis, processor, _ = pause_script(4)
    redis.pending = {"1-0": 9}
    redis.claims = [["0-0", [entry("1-0", attempts=1)]], ["0-0", []]]
    drive(consumer, redis, [])
    statuses = published(redis)
    assert len(statuses) == 1 and statuses[0]["reason"] == "POISON_MESSAGE" and statuses[0]["attempts"] == 1
    assert "ack:1-0" in redis.log and processor.events == []


def test_stop_ends_the_pause():
    consumer, redis, processor, _ = pause_script(10_000)
    redis.reads = [[[STREAM, [entry("1-0")]]]]
    original = consumer._sleep
    calls = {"n": 0}

    def sleep(seconds):
        calls["n"] += 1
        if calls["n"] > 8:
            consumer.stop()
        return original(seconds)

    consumer._sleep = sleep
    consumer.run()  # must return although publishing never works
    assert not any(e.startswith("ack") for e in redis.log)


# ---- a bare IngestError keeps its reason

def test_a_base_ingest_error_keeps_its_reason_and_is_retryable():
    class Raises:
        def process(self, event, deadline):
            raise IngestError(Reason.EMPTY_TEXT, "no text")

    assert run_job(Raises(), None, None) == Failed(True, Reason.EMPTY_TEXT, "no text")


# ---- review round 2

def test_the_poison_report_counts_its_budget_from_the_moment_it_starts():
    settings = ConsumerSettings(**{**SETTINGS.__dict__, "min_idle": timedelta(seconds=10), "call_timeout": 3.0, "publish_attempts": 5})
    now = {"t": 100.0}  # a large clock value: a budget measured from the epoch would be wrong
    redis = FakeRedis()
    redis.xadd_errors = [RedisConnectionError("down")] * 20
    redis.pending = {"1-0": 9}
    redis.claims = [["0-0", [entry("1-0")]], ["0-0", []]]
    consumer, *_ = build(redis, settings=settings)
    consumer._clock = lambda: now["t"]

    def sleep(seconds):
        now["t"] += 4.0
        return True

    consumer._sleep = sleep
    consumer.recover_once()
    assert len([e for e in redis.log if e.startswith("xadd")]) == 2  # same arithmetic as for a job


def test_an_unexpected_error_while_retrying_the_held_status_does_not_end_the_process():
    consumer, redis, processor, _ = pause_script(3)
    redis.xadd_errors.append(ValueError("unexpected reply shape"))  # raised by the first held retry
    drive(consumer, redis, [[[STREAM, [entry("1-0")]]]])
    assert len(published(redis)) == 1 and "ack:1-0" in redis.log  # it recovered and published


def test_an_error_while_acking_the_held_entry_does_not_end_the_process():
    consumer, redis, processor, _ = pause_script(3)
    redis.ack_errors = [ValueError("unexpected ack reply")]
    drive(consumer, redis, [[[STREAM, [entry("1-0")]]]])  # must return; the entry is redelivered later


def test_every_retry_of_a_held_status_renews_the_entry_without_touching_its_count():
    # Held longer than min_idle another consumer could reclaim and re-run it. XCLAIM ... JUSTID
    # resets the idle time, does not raise the delivery count, and is not refused at maxmemory.
    consumer, redis, *_ = pause_script(5)
    drive(consumer, redis, [[[STREAM, [entry("1-0")]]]])
    renewals = [e for e in redis.log if e.startswith("renew")]
    assert len(renewals) >= 2 and set(renewals) == {"renew:1-0:justid=True"}


def test_a_failing_renewal_does_not_stop_the_status_from_being_published():
    consumer, redis, *_ = pause_script(3)
    redis.renew_errors = [RedisConnectionError("down")] * 5
    drive(consumer, redis, [[[STREAM, [entry("1-0")]]]])
    assert "ack:1-0" in redis.log


def test_the_warning_for_a_status_that_is_still_held_is_throttled(caplog):
    consumer, redis, *_ = pause_script(200)
    with caplog.at_level("WARNING"):
        drive(consumer, redis, [[[STREAM, [entry("1-0")]]]])
    warnings = [r for r in caplog.records if "still cannot be published" in r.getMessage()]
    assert 1 <= len(warnings) <= 20  # ~190 failed retries, a line every 30th


def test_a_hostname_with_undecodable_bytes_gives_a_name_that_survives_redis(monkeypatch):
    # Redis stores the name as sent with encoding_errors="replace"; a surrogate in it would come
    # back as "?" and the owner check would then skip every reclaimed entry forever.
    monkeypatch.setattr("rag.transport.stream_consumer.socket.gethostname", lambda: "h\udcff")
    name = new_consumer_name()
    assert name == name.encode("utf-8").decode("utf-8")  # round-trips unchanged
    assert name.startswith("h") and "/ingest/" in name
