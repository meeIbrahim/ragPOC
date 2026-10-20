"""Typed ingest failures.

Whatever does the work (the stub now, the real ingest service in Phase 1) raises one of these;
run_job() in rag.ingest.outcome is the single place that turns them into a status outcome
(PRD 5.2), so the retryable/permanent decision is not scattered through the pipeline.
"""

from __future__ import annotations

from rag.transport.status_event import Reason


class IngestError(Exception):
    """A classified ingest failure. detail is short text for logs and the status event, and must
    never contain document content."""

    def __init__(self, reason: Reason | str, detail: str | None = None) -> None:
        self.reason = Reason(reason)  # an unknown reason is a programming error: ValueError
        self.detail = detail
        super().__init__(f"{self.reason.value}: {detail}" if detail else self.reason.value)


class PermanentError(IngestError):
    """Retrying cannot help (unsupported type, corrupt file, no extractable text)."""


class TransientError(IngestError):
    """Retrying may help (a dependency is down, the job ran out of time)."""
