"""Time limits for one ingest job (PRD 5.3, L15).

Three layers bound a job: every external call has a client timeout; the job carries a Deadline
that is checked between steps and raises a retryable JOB_TIMEOUT; and the Watchdog kills the
process if a single step never returns at all, since nothing in-process can interrupt that.
`min_idle` (the time before another consumer may reclaim an entry) is configured above
job_timeout + watchdog_grace, so a live job is never reclaimed.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable

from rag.ingest.errors import TransientError
from rag.transport.status_event import Reason


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
        # Timer.cancel() does nothing once the callback has started, so a disarm (or re-arm) that
        # loses the race would still see the process exit. Every arm/disarm moves the generation
        # on, and a firing timer only acts if its generation is still the current one.
        self._generation = 0

    def arm(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._generation += 1
            timer = threading.Timer(self._timeout, self._fire, args=(self._generation,))
            timer.daemon = True  # must never keep the process alive on its own
            self._timer = timer
            timer.start()

    def disarm(self) -> None:
        with self._lock:
            self._generation += 1
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None

    def _fire(self, generation: int) -> None:
        with self._lock:
            if generation != self._generation:
                return  # disarmed or re-armed after this timer started: the job is not stuck
        try:
            # Straight to fd 2: the logging machinery takes a lock the stuck main thread may be
            # holding (a full stderr pipe), and os._exit must never wait for it.
            os.write(2, f"watchdog: a job ran past {self._timeout:.0f}s and did not return; exiting the process\n".encode())
        except OSError:
            pass
        finally:
            self._exit_fn(1)
