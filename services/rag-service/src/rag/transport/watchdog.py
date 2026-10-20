"""Time limits for one ingest job (PRD 5.3, L15).

Three layers bound a job: every external call has a client timeout; the job carries a Deadline
that is checked between steps and raises a retryable JOB_TIMEOUT; and the Watchdog kills the
process if a single step never returns at all, since nothing in-process can interrupt that.
`min_idle` (the time before another consumer may reclaim an entry) is configured above
job_timeout + watchdog_grace, so a live job is never reclaimed.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable

from rag.ingest.errors import TransientError
from rag.transport.status_event import Reason

logger = logging.getLogger(__name__)


class Deadline:
    """A point in time by which the job must finish."""

    def __init__(self, expires_at: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._expires_at = expires_at
        self._clock = clock

    @classmethod
    def after(cls, seconds: float, clock: Callable[[], float] = time.monotonic) -> Deadline:
        return cls(clock() + seconds, clock)

    def remaining(self) -> float:
        return max(0.0, self._expires_at - self._clock())

    @property
    def expired(self) -> bool:
        return self._clock() >= self._expires_at

    def check(self) -> None:
        """Call between steps and between embedding batches. Raises a retryable JOB_TIMEOUT."""
        if self.expired:
            raise TransientError(Reason.JOB_TIMEOUT, "job deadline passed")


class Watchdog:
    """Exits the process if it is armed for longer than `timeout` seconds.

    The consumer arms it before a job and disarms it in a finally block. When it fires it calls
    exit_fn(1): os._exit by default, which ends the process even if the main thread is stuck in
    a call that never returns. The container restarts under a new consumer name and the entry is
    reclaimed after min_idle, counting toward the poison threshold.
    """

    def __init__(self, timeout: float, exit_fn: Callable[[int], object] = os._exit) -> None:
        if timeout <= 0:
            raise ValueError("watchdog timeout must be > 0")
        self._timeout = timeout
        self._exit_fn = exit_fn
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None

    def arm(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            timer = threading.Timer(self._timeout, self._fire)
            timer.daemon = True  # must never keep the process alive on its own
            self._timer = timer
            timer.start()

    def disarm(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None

    def _fire(self) -> None:
        logger.error("watchdog: a job ran past %.0fs and did not return; exiting the process", self._timeout)
        for handler in logging.getLogger().handlers:
            handler.flush()  # os._exit skips the normal flush
        self._exit_fn(1)
