"""Task 26a (T-P10, T-P11): the job deadline, the watchdog, and the typed failures that turn an
exception into a status outcome in one place."""

import logging
import threading
import time

import pytest

from rag.ingest.errors import PermanentError, TransientError
from rag.ingest.outcome import Failed, Indexed, run_job
from rag.transport.status_event import Reason
from rag.transport.watchdog import Deadline, Watchdog


class FakeClock:
    def __init__(self, now: float = 100.0):
        self.now = now

    def __call__(self) -> float:
        return self.now


# ---- Deadline (T-P10)

def test_deadline_passes_before_it_expires():
    clock = FakeClock()
    deadline = Deadline.after(5, clock=clock)
    deadline.check()
    clock.now += 4.999
    deadline.check()
    assert 0 < deadline.remaining() < 0.01


def test_deadline_raises_a_retryable_job_timeout_once_it_passes():
    clock = FakeClock()
    deadline = Deadline.after(5, clock=clock)
    clock.now += 5
    with pytest.raises(TransientError) as excinfo:
        deadline.check()
    assert excinfo.value.reason is Reason.JOB_TIMEOUT


def test_deadline_stays_expired():
    clock = FakeClock()
    deadline = Deadline.after(1, clock=clock)
    clock.now += 10
    for _ in range(3):
        with pytest.raises(TransientError):
            deadline.check()
    assert deadline.remaining() == 0
    assert deadline.expired


def test_deadline_is_not_expired_before_time():
    assert not Deadline.after(60).expired


# ---- typed failures

def test_failures_carry_a_reason_and_accept_it_as_a_plain_string():
    assert PermanentError("CORRUPT_FILE").reason is Reason.CORRUPT_FILE
    assert TransientError(Reason.DEPENDENCY_UNAVAILABLE, "qdrant timed out").detail == "qdrant timed out"


def test_an_unknown_reason_is_a_programming_error():
    with pytest.raises(ValueError):
        PermanentError("OUT_OF_COFFEE")


# ---- run_job: the one place that classifies a failure (PRD 5.2)

class Raises:
    def __init__(self, exc):
        self.exc = exc

    def process(self, event, deadline):
        raise self.exc


class Returns:
    def __init__(self, outcome):
        self.outcome = outcome

    def process(self, event, deadline):
        return self.outcome


def test_an_outcome_passes_through():
    assert run_job(Returns(Indexed(3)), None, None) == Indexed(3)
    failed = Failed(True, Reason.INTERNAL, "x")
    assert run_job(Returns(failed), None, None) == failed


def test_a_permanent_error_is_a_non_retryable_failure():
    assert run_job(Raises(PermanentError(Reason.EMPTY_TEXT, "no text")), None, None) == Failed(False, Reason.EMPTY_TEXT, "no text")


def test_a_transient_error_is_a_retryable_failure():
    assert run_job(Raises(TransientError(Reason.JOB_TIMEOUT)), None, None) == Failed(True, Reason.JOB_TIMEOUT, None)


def test_anything_uncaught_is_a_retryable_internal_failure_without_leaking_its_message(caplog):
    secret = "contents of the document: hunter2"
    with caplog.at_level(logging.ERROR):
        outcome = run_job(Raises(ValueError(secret)), None, None)
    assert outcome == Failed(True, Reason.INTERNAL, "ValueError")
    assert secret not in (outcome.detail or "")
    assert "ValueError" in caplog.text  # the traceback is logged for operators


def test_a_job_that_asks_the_process_to_stop_is_not_swallowed():
    class Stop(BaseException):
        pass

    with pytest.raises(Stop):
        run_job(Raises(Stop()), None, None)


# ---- Watchdog (T-P11)

def test_watchdog_exits_the_process_when_a_job_runs_past_its_limit(monkeypatch):
    written = []
    monkeypatch.setattr("rag.transport.watchdog.os.write", lambda fd, data: written.append((fd, data)))
    fired = threading.Event()
    codes = []
    watchdog = Watchdog(0.05, exit_fn=lambda code: (codes.append(code), fired.set()))
    watchdog.arm()
    assert fired.wait(2), "the watchdog never fired"
    assert codes == [1]
    # The last words go to fd 2 directly: logging takes a lock the stuck main thread may hold.
    assert written and written[0][0] == 2 and b"watchdog" in written[0][1].lower()


def test_watchdog_does_not_fire_when_disarmed_in_time():
    fired = threading.Event()
    watchdog = Watchdog(0.1, exit_fn=lambda code: fired.set())
    watchdog.arm()
    watchdog.disarm()
    assert not fired.wait(0.4)


def test_disarming_a_watchdog_that_was_never_armed_is_harmless():
    Watchdog(1, exit_fn=lambda code: None).disarm()


def test_arming_twice_restarts_the_timer_and_fires_once():
    fired = []
    done = threading.Event()
    watchdog = Watchdog(0.15, exit_fn=lambda code: (fired.append(code), done.set()))
    watchdog.arm()
    time.sleep(0.1)
    watchdog.arm()  # the first timer must not fire at 0.15s
    time.sleep(0.1)
    assert fired == []
    assert done.wait(2)
    time.sleep(0.1)
    assert fired == [1]


def test_watchdog_can_be_armed_again_after_firing_or_disarming():
    fired = threading.Event()
    watchdog = Watchdog(0.05, exit_fn=lambda code: fired.set())
    watchdog.arm()
    watchdog.disarm()
    watchdog.arm()
    assert fired.wait(2)


def test_the_watchdog_thread_is_a_daemon_so_it_never_keeps_the_process_alive():
    watchdog = Watchdog(30, exit_fn=lambda code: None)
    watchdog.arm()
    try:
        timers = [t for t in threading.enumerate() if isinstance(t, threading.Timer)]
        assert timers and all(t.daemon for t in timers)
    finally:
        watchdog.disarm()


def test_a_non_positive_timeout_is_rejected():
    with pytest.raises(ValueError):
        Watchdog(0, exit_fn=lambda code: None)


# ---- races and robustness (review round 1)

def test_a_stale_timer_never_exits_the_process():
    # Timer.cancel() does nothing once the callback has started: a disarm (or re-arm) that loses
    # the race must still win, or the process exits although the job returned.
    fired = []
    watchdog = Watchdog(30, exit_fn=fired.append)
    watchdog.arm()
    stale = watchdog._generation
    watchdog.disarm()
    watchdog._fire(stale)
    assert fired == []


def test_a_timer_from_before_a_re_arm_is_stale():
    fired = []
    watchdog = Watchdog(30, exit_fn=fired.append)
    try:
        watchdog.arm()
        first = watchdog._generation
        watchdog.arm()
        watchdog._fire(first)
        assert fired == []
        watchdog._fire(watchdog._generation)
        assert fired == [1]
    finally:
        watchdog.disarm()


def test_the_process_still_exits_when_writing_the_last_words_fails(monkeypatch):
    def broken(fd, data):
        raise OSError("stderr is gone")

    monkeypatch.setattr("rag.transport.watchdog.os.write", broken)
    fired = []
    watchdog = Watchdog(30, exit_fn=fired.append)
    try:
        watchdog.arm()
        watchdog._fire(watchdog._generation)
    finally:
        watchdog.disarm()
    assert fired == [1]
