"""A stand-in for the real ingest service, used to prove the event loop end to end (Phase 0).

It does no parsing, embedding or indexing: each mode produces one scripted outcome, so the e2e
can drive every path the center service must handle. Phase 1 replaces it behind the same
`process(event, deadline)` call.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from enum import StrEnum

from rag.ingest.outcome import Failed, Indexed, Outcome
from rag.transport.status_event import IngestEvent, Reason
from rag.transport.watchdog import Deadline


class Mode(StrEnum):
    INDEXED = "indexed"
    FAILED_RETRYABLE = "failed-retryable"
    FAILED_PERMANENT = "failed-permanent"
    CRASH = "crash"
    SLOW = "slow"
    HANG = "hang"


class StubProcessor:
    def __init__(
        self,
        mode: Mode,
        *,
        exit_fn: Callable[[int], object] = os._exit,
        sleep: Callable[[float], object] = time.sleep,
        step_seconds: float = 1.0,
        hang: threading.Event | None = None,
    ) -> None:
        self._mode = Mode(mode)
        self._exit_fn = exit_fn
        self._sleep = sleep
        self._step_seconds = step_seconds
        # Never set in production: a `hang` job must only end through the watchdog.
        self._hang = hang if hang is not None else threading.Event()

    def process(self, event: IngestEvent, deadline: Deadline) -> Outcome:
        match self._mode:
            case Mode.INDEXED:
                return Indexed(chunk_count=0)
            case Mode.FAILED_RETRYABLE:
                return Failed(True, Reason.INTERNAL, "stub failed-retryable")
            case Mode.FAILED_PERMANENT:
                return Failed(False, Reason.CORRUPT_FILE, "stub failed-permanent")
            case Mode.CRASH:
                # After reading, before publishing or acknowledging: the entry stays pending.
                self._exit_fn(1)
                raise RuntimeError("crash mode: exit_fn returned")
            case Mode.SLOW:
                # Simulated steps that never finish in time. The deadline is checked between
                # steps, like the real pipeline does, and raises a retryable JOB_TIMEOUT.
                while True:
                    deadline.check()
                    self._sleep(self._step_seconds)
            case Mode.HANG:
                # A step that never returns. Only the watchdog ends it, by exiting the process.
                self._hang.wait()
                raise RuntimeError("hang mode: released")
        raise AssertionError(f"unhandled mode {self._mode}")  # pragma: no cover
