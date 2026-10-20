"""What one ingest attempt ends in, and the one place that classifies a failure."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

from rag.ingest.errors import PermanentError, TransientError
from rag.transport.status_event import IngestEvent, Reason
from rag.transport.watchdog import Deadline

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Indexed:
    chunk_count: int | None = None


@dataclass(frozen=True)
class Failed:
    retryable: bool
    reason: Reason
    detail: str | None = None


Outcome = Indexed | Failed


class Processor(Protocol):
    """Does the work for one ingest event. The stub implements it now; Phase 1's
    IngestService replaces it behind the same call."""

    def process(self, event: IngestEvent, deadline: Deadline) -> Outcome: ...


def run_job(processor: Processor, event: IngestEvent, deadline: Deadline) -> Outcome:
    """Run one job and return its outcome. Typed errors keep their classification; anything
    uncaught is a retryable INTERNAL failure (PRD 5.2). BaseException (a process exit, Ctrl-C)
    is deliberately not caught."""
    try:
        return processor.process(event, deadline)
    except PermanentError as exc:
        return Failed(False, exc.reason, exc.detail)
    except TransientError as exc:
        return Failed(True, exc.reason, exc.detail)
    except Exception as exc:
        logger.exception("ingest job failed with an unclassified error")
        # Only the exception type is reported: its message could contain document content.
        return Failed(True, Reason.INTERNAL, type(exc).__name__)
