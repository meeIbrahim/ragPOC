"""The ingest stream consumer (PRD 5.3).

One consumer, one job at a time, in one thread. It reads new entries with `>`, runs the
processor, publishes a status event, and only then acknowledges the entry. Entries another
(dead) consumer left pending come back through a recovery pass.

Design rules, several of them learned from the Go status worker:
- Claim and run ONE reclaimed entry at a time. `min_idle` is configured above
  job_timeout + watchdog_grace, which protects one running job; a claimed batch of ten would
  wait far longer than min_idle for its turn and could be reclaimed by someone else mid-wait.
- An entry is acknowledged only after its status event was published (L17). A publish that
  keeps failing leaves the entry pending; redelivery is safe because the center's CAS ignores
  duplicates.
- A poison entry is acknowledged only after its POISON_MESSAGE status was published, so a Redis
  outage never turns into a silently dropped event.
- A failing entry never holds back the next one.
- A consumer group that vanished (Redis restarted without it in its snapshot) is created again.
- When only publishing fails (Redis at maxmemory refuses XADD but still serves reads and claims),
  stop reading and claiming and retry the held status until it goes through. Reading on would
  run jobs whose results cannot be reported, and every later claim would add a delivery to
  entries that are blameless, until they were reported as poison.
- No single entry may take the loop down: an unexpected error is logged and the loop carries on.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from redis.exceptions import RedisError, ResponseError

from rag.core.config_reader import Config
from rag.ingest.outcome import Failed, Indexed, Outcome, Processor, run_job
from rag.transport.status_event import INGEST_EVENT_TYPE, IngestEvent, InvalidEventError, Reason, StatusEvent
from rag.transport.status_publisher import StatusPublisher
from rag.transport.watchdog import Deadline

logger = logging.getLogger(__name__)

# An ingest event is a few hundred bytes. Anything this large is not one, and is not worth parsing.
MAX_PAYLOAD_BYTES = 1024 * 1024
# One XAUTOCLAIM call only scans a few pending entries; follow its cursor, but not forever.
MAX_CLAIM_PAGES = 100
_MAX_LOGGED = 200


def new_consumer_name() -> str:
    """`<host>/ingest/<uuid>`, new on every process start (L19): a restarted process never
    drains a previous name's pending list; those entries return only through the recovery pass."""
    host = socket.gethostname() or "unknown"
    # gethostname() decodes odd bytes with surrogateescape. The Redis client sends the name with
    # encoding_errors="replace", so a surrogate would come back as "?" and the owner check would
    # then never match: replace it here, once, so the name is the same on both sides.
    host = host.encode("utf-8", "replace").decode("utf-8")
    return f"{host}/ingest/{uuid.uuid4()}"


def socket_timeout_for(call_timeout: float, read_block: float) -> float:
    """The Redis read timeout: call_timeout, but always longer than the longest BLOCK, or a quiet
    stream would look like a dead connection."""
    return max(call_timeout, read_block + 5)


@dataclass(frozen=True)
class ConsumerSettings:
    stream: str
    group: str
    min_idle: timedelta
    recovery_interval: timedelta
    poison_threshold: int
    cleanup_idle: timedelta
    job_timeout: timedelta
    read_block: timedelta = timedelta(seconds=2)
    retry_delay: float = 1.0
    publish_attempts: int = 3
    # Seconds one publish attempt may take at most (connect + read timeout). A new attempt only
    # starts while a whole one still fits before min_idle, or another consumer could reclaim and
    # re-run the entry meanwhile.
    call_timeout: float = 60.0
    # Jobs run from one recovery pass before the loop goes back to reading new entries.
    max_recovery_jobs: int = 10

    @classmethod
    def from_config(cls, config: Config) -> ConsumerSettings:
        consumer = config.rag.consumer
        call_timeout = config.rag.call_timeout.total_seconds()
        # One attempt that has to reconnect can spend the connect timeout and then the read
        # timeout, so that sum, not call_timeout alone, is what a publish attempt may cost.
        attempt_bound = call_timeout + socket_timeout_for(call_timeout, cls.read_block.total_seconds())
        return cls(
            stream=config.redis.stream,
            group=config.redis.consumer_group,
            min_idle=consumer.min_idle,
            recovery_interval=consumer.recovery_interval,
            poison_threshold=consumer.poison_threshold,
            cleanup_idle=consumer.consumer_cleanup_idle,
            job_timeout=consumer.job_timeout,
            call_timeout=attempt_bound,
        )


def _ms(delta: timedelta) -> int:
    return int(delta.total_seconds() * 1000)


def _clip(value: Any) -> str:
    text = str(value)
    return text if len(text) <= _MAX_LOGGED else text[:_MAX_LOGGED] + "..."


def _is_no_group(exc: Exception) -> bool:
    return isinstance(exc, ResponseError) and "NOGROUP" in str(exc)


class StreamConsumer:
    def __init__(
        self,
        redis: Any,
        settings: ConsumerSettings,
        publisher: StatusPublisher,
        processor: Processor,
        watchdog: Any,
        *,
        name: str | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], bool] | None = None,
    ) -> None:
        self._redis = redis
        self._settings = settings
        self._publisher = publisher
        self._processor = processor
        self._watchdog = watchdog
        self.name = name or new_consumer_name()
        self._clock = clock
        # (entry id, status) whose publish failed: no reading or claiming until it goes through.
        self._held: tuple[str, StatusEvent] | None = None
        self._held_failures = 0
        self._stop = threading.Event()
        # sleep(seconds) -> True when the full wait happened, False when stop() cut it short.
        self._sleep = sleep or (lambda seconds: not self._stop.wait(seconds))

    def stop(self) -> None:
        """Ends the loop between jobs. A job already running is not interrupted (PRD 5.3: no
        graceful drain; an abandoned entry is reclaimed after min_idle)."""
        self._stop.set()

    # ---- group

    def ensure_group(self) -> None:
        """XGROUP CREATE at ID 0 with MKSTREAM, so entries published before the group existed are
        still delivered and the stream is created when needed. An existing group is fine."""
        try:
            self._redis.xgroup_create(self._settings.stream, self._settings.group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def _ensure_group_until_ready(self) -> bool:
        while not self._stop.is_set():
            try:
                self.ensure_group()
                return True
            except RedisError as exc:
                logger.error("cannot create the consumer group; retrying: %s", _clip(exc))
                if not self._sleep(self._settings.retry_delay):
                    return False
        return False

    # ---- loop

    def run(self) -> None:
        """Blocks until stop(). Redis errors are logged and retried, never fatal."""
        if not self._ensure_group_until_ready():
            return
        logger.info("ingest consumer %s started", self.name)

        next_recovery = 0.0  # recover once right at start
        while not self._stop.is_set():
            if self._held is not None:
                self._guarded(self._retry_held)  # an unexpected error here must not end the process
                if self._held is not None:
                    self._sleep(self._settings.retry_delay)
                continue
            if self._clock() >= next_recovery:
                self._guarded(self.recover_once)
                next_recovery = self._clock() + self._settings.recovery_interval.total_seconds()
                if self._held is not None:
                    continue  # recovery just failed to publish: retry that before reading anything
            self._guarded(self.read_once)
        logger.info("ingest consumer %s stopped", self.name)

    def _guarded(self, step: Callable[[], None]) -> None:
        try:
            step()
        except RedisError as exc:
            if _is_no_group(exc):
                # Redis lost the group (restart without it in a snapshot, deleted stream). Created
                # at ID 0 again: replayed entries are safe because the center ignores duplicates.
                logger.warning("the consumer group disappeared; creating it again")
                self._ensure_group_until_ready()
                return
            logger.error("redis error; backing off: %s", _clip(exc))
            self._sleep(self._settings.retry_delay)
        except Exception:
            # Not a Redis error: a reply that could not be decoded, an entry of an unexpected
            # shape. Crashing would restart into the same entry again and again; log and carry on.
            logger.exception("unexpected error while handling the stream; backing off")
            self._sleep(self._settings.retry_delay)

    # ---- new entries

    def read_once(self) -> None:
        """Read at most one new entry and process it."""
        # BLOCK must not outlast the recovery interval, or a single-process deployment would
        # never reach its recovery pass. redis-py turns BLOCK 0 into "wait forever", so clamp up.
        block = max(1, min(_ms(self._settings.read_block), _ms(self._settings.recovery_interval)))
        response = self._redis.xreadgroup(
            self._settings.group, self.name, {self._settings.stream: ">"}, count=1, block=block
        )
        for _stream, entries in response or []:
            for entry_id, fields in entries:
                event = self._parse(entry_id, fields)
                if event is not None:
                    self._handle(entry_id, event)

    # ---- recovery

    def recover_once(self) -> None:
        """Take over entries a dead consumer left pending, one at a time, then delete consumers
        that are gone. Raises the claim error (if any) after cleanup so the loop can notice a
        vanished group."""
        claim_error: Exception | None = None
        try:
            self._claim_and_process()
        except Exception as exc:
            claim_error = exc
        self._cleanup_consumers()
        if claim_error is not None:
            raise claim_error

    def _claim_and_process(self) -> None:
        for _ in range(self._settings.max_recovery_jobs):
            claimed = self._claim_one()
            if claimed is None:
                return
            entry_id, fields = claimed
            event = self._parse(entry_id, fields)
            if event is None:
                continue
            try:
                pending = self._pending_info(entry_id)
            except RedisError as exc:
                # Neither ack (it may be a good event) nor process (it may be poison): it stays
                # pending and is reclaimed on a later pass.
                logger.error("cannot read the delivery count of %s; leaving it pending: %s", entry_id, _clip(exc))
                continue
            if pending is None or pending[1] != self.name:
                # Acked by its original owner, or claimed by someone else, since our claim.
                logger.warning("entry %s is no longer pending under this consumer; skipping it", entry_id)
                continue
            deliveries = pending[0]
            if deliveries > self._settings.poison_threshold:
                self._report_poison(entry_id, event, deliveries)
            else:
                self._handle(entry_id, event)
            if self._held is not None:
                return  # publishing is down: claim nothing more, the counts must not grow

    def _claim_one(self) -> tuple[str, dict[str, str] | None] | None:
        """XAUTOCLAIM with count=1 and no JUSTID: the delivery count must increment and the
        payload is needed. One call scans only a few pending entries, so follow the cursor."""
        cursor = "0-0"
        for _ in range(MAX_CLAIM_PAGES):
            response = self._redis.xautoclaim(
                self._settings.stream,
                self._settings.group,
                self.name,
                _ms(self._settings.min_idle),
                start_id=cursor,
                count=1,
                justid=False,
            )
            next_cursor, entries = response[0], response[1]
            deleted = response[2] if len(response) > 2 else []
            if deleted:
                logger.error(
                    "pending ingest entries no longer exist in the stream; those events are lost: %s", deleted
                )
            if entries:
                return entries[0]
            if next_cursor == "0-0":
                return None
            cursor = next_cursor
        return None

    def _pending_info(self, entry_id: str) -> tuple[int, str] | None:
        """(times delivered, owning consumer) of a pending entry, or None when it is not pending."""
        pending = self._redis.xpending_range(
            self._settings.stream, self._settings.group, min=entry_id, max=entry_id, count=1
        )
        if not pending:
            return None
        return int(pending[0]["times_delivered"]), pending[0]["consumer"]

    def _report_poison(self, entry_id: str, event: IngestEvent, deliveries: int) -> None:
        """An entry delivered too often: report it as a permanent failure, echoing the attempt
        number from the event (L12). It is acknowledged only if the report was published."""
        logger.error("poison ingest entry %s delivered %d times (threshold %d)", entry_id, deliveries, self._settings.poison_threshold)
        status = StatusEvent.failed(
            event.hash,
            event.attempts,
            retryable=False,
            reason=Reason.POISON_MESSAGE,
            detail=f"delivered {deliveries} times, threshold {self._settings.poison_threshold}",
        )
        self._finish(entry_id, status, started=self._clock())

    def _cleanup_consumers(self) -> None:
        """Delete consumers other than this one with no pending entries that have been idle past
        the threshold. A consumer with pending entries is never deleted: Redis would drop its
        entries silently."""
        try:
            consumers = self._redis.xinfo_consumers(self._settings.stream, self._settings.group)
            threshold = _ms(self._settings.cleanup_idle)
            for consumer in consumers:
                if consumer["name"] == self.name or consumer["pending"] != 0 or consumer["idle"] <= threshold:
                    continue
                self._redis.xgroup_delconsumer(self._settings.stream, self._settings.group, consumer["name"])
                logger.info("deleted idle consumer %s", consumer["name"])
        except RedisError as exc:
            logger.error("cleaning up idle consumers failed: %s", _clip(exc))

    # ---- one entry

    def _parse(self, entry_id: str, fields: dict[str, str] | None) -> IngestEvent | None:
        """The event, or None after acknowledging an entry that can never be processed: retrying
        the same bytes cannot succeed, and an unacked entry would be redelivered forever."""
        if fields is None:
            logger.error("entry %s no longer exists in the stream", entry_id)
        elif fields.get("event_type") != INGEST_EVENT_TYPE:
            logger.warning("discarding entry %s with event_type %s", entry_id, _clip(fields.get("event_type")))
        else:
            raw = fields.get("payload")
            if not isinstance(raw, (str, bytes)):
                logger.warning("discarding entry %s without a text payload", entry_id)
            elif len(raw) > MAX_PAYLOAD_BYTES:
                logger.warning("discarding entry %s: payload of %d bytes is not an ingest event", entry_id, len(raw))
            else:
                try:
                    return IngestEvent.parse(raw)
                except InvalidEventError as exc:
                    logger.warning("discarding invalid ingest entry %s: %s", entry_id, exc)
        self._ack(entry_id)
        return None

    def _handle(self, entry_id: str, event: IngestEvent) -> bool:
        """Run the job, publish its status, acknowledge. Returns whether the entry was finished."""
        started = self._clock()  # the entry's idle time starts just before this
        deadline = Deadline.after(self._settings.job_timeout.total_seconds())
        self._watchdog.arm()
        try:
            outcome = run_job(self._processor, event, deadline)
        finally:
            self._watchdog.disarm()
        return self._finish(entry_id, self._status_for(event, outcome), started)

    def _finish(self, entry_id: str, status: StatusEvent, started: float) -> bool:
        """Publish the status, then acknowledge. When the publish cannot be done the entry and its
        status are held: nothing else is read or claimed until they go through."""
        if self._publish(status, started):
            self._ack(entry_id)
            return True
        self._held = (entry_id, status)
        self._held_failures = 0
        return False

    def _retry_held(self) -> bool:
        """One more try at the held status. The job already ran and is not run again."""
        assert self._held is not None
        entry_id, status = self._held
        self._renew(entry_id)
        try:
            self._publisher.publish(status)
        except RedisError as exc:
            self._held_failures += 1
            if self._held_failures == 1 or self._held_failures % 30 == 0:  # not a line per second
                logger.warning(
                    "status for %s still cannot be published (%d tries): %s", entry_id, self._held_failures, _clip(exc)
                )
            return False
        self._held = None
        self._held_failures = 0
        logger.info("the held status for %s was published; resuming", entry_id)
        self._ack(entry_id)
        return True

    def _renew(self, entry_id: str) -> None:
        """Keep a held entry from becoming reclaimable: XCLAIM ... JUSTID resets its idle time,
        does not add a delivery, and (unlike XADD) is not refused when Redis is at maxmemory. It
        is the idle-time renewal of backlog B11, done only for the entry being held. Best effort."""
        try:
            self._redis.xclaim(self._settings.stream, self._settings.group, self.name, 0, [entry_id], justid=True)
        except Exception as exc:
            logger.debug("could not renew %s: %s", entry_id, _clip(exc))

    @staticmethod
    def _status_for(event: IngestEvent, outcome: Outcome) -> StatusEvent:
        # attempts is echoed from the event, never counted here (L12).
        if isinstance(outcome, Indexed):
            return StatusEvent.indexed(event.hash, event.attempts, chunk_count=outcome.chunk_count)
        assert isinstance(outcome, Failed)
        return StatusEvent.failed(
            event.hash, event.attempts, retryable=outcome.retryable, reason=outcome.reason, detail=outcome.detail
        )

    def _publish(self, status: StatusEvent, started: float) -> bool:
        """Publish with a few quick retries. False means the caller must hold the entry.

        The first attempt always happens. A further one only starts while a whole call_timeout
        still fits before min_idle, counted from `started`: past that the entry could be
        reclaimed and run again by another consumer, which is the one thing min_idle is for."""
        attempts = self._settings.publish_attempts
        reclaimable_at = started + self._settings.min_idle.total_seconds()
        for attempt in range(attempts):
            try:
                self._publisher.publish(status)
                return True
            except RedisError as exc:
                logger.warning("publishing the status event failed (try %d of %d): %s", attempt + 1, attempts, _clip(exc))
            if attempt + 1 >= attempts or not self._sleep(self._settings.retry_delay):
                break
            if self._clock() + self._settings.call_timeout >= reclaimable_at:
                logger.warning("no time left to retry the publish before the entry becomes reclaimable")
                break
        logger.error("could not publish the status event for %s; holding the entry", status.hash)
        return False

    def _ack(self, entry_id: str) -> None:
        try:
            self._redis.xack(self._settings.stream, self._settings.group, entry_id)
        except RedisError as exc:
            logger.error("ack of %s failed; the entry will be redelivered: %s", entry_id, _clip(exc))
