"""Task 26 (T-P3, T-P6, T-P10 for `slow`): the stub processor and its six modes."""

import threading

import pytest

from rag.ingest.errors import TransientError
from rag.ingest.outcome import Failed, Indexed, run_job
from rag.ingest.stub import Mode, StubProcessor
from rag.transport.status_event import Reason
from rag.transport.watchdog import Deadline
from support import ingest_event


class Crashed(BaseException):
    """Stands in for the process disappearing: os._exit never returns."""


class FakeTime:
    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def deadline(seconds=60.0):
    return Deadline.after(seconds)


def test_modes_parse_from_their_cli_names():
    assert [Mode(m) for m in ("indexed", "failed-retryable", "failed-permanent", "crash", "slow", "hang")] == list(Mode)


def test_an_unknown_mode_is_rejected():
    with pytest.raises(ValueError):
        Mode("explode")


def test_indexed_reports_success_with_no_chunks():
    assert run_job(StubProcessor(Mode.INDEXED), ingest_event(), deadline()) == Indexed(chunk_count=0)


def test_failed_retryable_is_a_retryable_internal_failure():
    outcome = run_job(StubProcessor(Mode.FAILED_RETRYABLE), ingest_event(), deadline())
    assert outcome == Failed(True, Reason.INTERNAL, "stub failed-retryable")


def test_failed_permanent_is_a_non_retryable_corrupt_file_failure():
    outcome = run_job(StubProcessor(Mode.FAILED_PERMANENT), ingest_event(), deadline())
    assert outcome == Failed(False, Reason.CORRUPT_FILE, "stub failed-permanent")


def test_crash_ends_the_process_with_code_1_and_returns_no_outcome():
    codes = []

    def exit_fn(code):
        codes.append(code)
        raise Crashed

    with pytest.raises(Crashed):
        run_job(StubProcessor(Mode.CRASH, exit_fn=exit_fn), ingest_event(), deadline())
    assert codes == [1]


# ---- slow: steps that never finish in time, so the deadline raises a retryable JOB_TIMEOUT

def test_slow_runs_steps_until_the_deadline_and_reports_job_timeout():
    fake = FakeTime()
    processor = StubProcessor(Mode.SLOW, sleep=fake.sleep, step_seconds=1.0)
    outcome = run_job(processor, ingest_event(), Deadline.after(5, clock=fake.clock))
    assert outcome == Failed(True, Reason.JOB_TIMEOUT, "job deadline passed")
    assert len(fake.sleeps) == 5  # it checked the deadline between steps, not only at the end


def test_slow_raises_a_transient_job_timeout_before_classification():
    fake = FakeTime()
    processor = StubProcessor(Mode.SLOW, sleep=fake.sleep, step_seconds=1.0)
    with pytest.raises(TransientError) as excinfo:
        processor.process(ingest_event(), Deadline.after(3, clock=fake.clock))
    assert excinfo.value.reason is Reason.JOB_TIMEOUT


def test_slow_with_an_already_expired_deadline_does_no_work():
    fake = FakeTime()
    processor = StubProcessor(Mode.SLOW, sleep=fake.sleep, step_seconds=1.0)
    outcome = run_job(processor, ingest_event(), Deadline.after(0, clock=fake.clock))
    assert outcome.reason is Reason.JOB_TIMEOUT and fake.sleeps == []


# ---- hang: a step that never returns, which only the watchdog can end

def test_hang_blocks_until_released_and_never_returns_an_outcome():
    release = threading.Event()
    processor = StubProcessor(Mode.HANG, hang=release)
    result = {}

    def work():
        try:
            result["outcome"] = processor.process(ingest_event(), deadline())
        except RuntimeError as exc:
            result["released"] = str(exc)

    thread = threading.Thread(target=work, daemon=True)
    thread.start()
    thread.join(0.3)
    assert thread.is_alive(), "hang returned on its own"
    release.set()
    thread.join(2)
    assert not thread.is_alive()
    assert "outcome" not in result and "released" in result


# ---- the event is the consumer's business: the stub must not need or alter it

@pytest.mark.parametrize("mode", [Mode.INDEXED, Mode.FAILED_RETRYABLE, Mode.FAILED_PERMANENT])
def test_the_outcome_does_not_depend_on_the_event(mode):
    first = run_job(StubProcessor(mode), ingest_event(attempts=0, filename="a.pdf"), deadline())
    second = run_job(StubProcessor(mode), ingest_event(attempts=2, filename="b.md"), deadline())
    assert first == second
