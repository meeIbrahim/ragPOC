"""The two stream events between the Go center service and this service.

- IngestEvent: Go -> Python on `ingest.docs` (PRD 4.1). This side only parses it.
- StatusEvent: Python -> Go on `ingest.status` (PRD 4.2). This side only produces it.

The machine-checkable contract is contracts/events/*.schema.json plus the fixtures next to it;
tests/test_contracts.py runs both against this module, and the Go tests run against the same
files. Parsing here is as strict as the schema: a malformed ingest event is the consumer's
problem to log and acknowledge, not to guess at.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

INGEST_EVENT_TYPE = "IngestionRequest"
STATUS_EVENT_TYPE = "IngestionStatus"
SCHEMA_VERSION = 1

# Integer fields are bounded to the int32 range, the same as the Go decoder and the schema.
MAX_INTEGER = 2**31 - 1
# detail is short free text for logs and never document content; a runaway message is cut.
MAX_DETAIL_LENGTH = 500

# RFC 3339 UTC as Go's time.Time emits it: no fraction when zero, otherwise 1 to 9 digits. The
# same pattern as the schemas. fullmatch, because "$" would also accept a trailing newline.
_CREATED_AT = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z")


class InvalidEventError(ValueError):
    """An event violates the contract. The message says which field and why."""


class Status(StrEnum):
    INDEXED = "INDEXED"
    FAILED = "FAILED"


class Reason(StrEnum):
    """Why ingestion failed (PRD 4.2). Every value must be known to the Go consumer."""

    UNSUPPORTED_TYPE = "UNSUPPORTED_TYPE"
    EMPTY_TEXT = "EMPTY_TEXT"
    CORRUPT_FILE = "CORRUPT_FILE"
    DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
    JOB_TIMEOUT = "JOB_TIMEOUT"
    POISON_MESSAGE = "POISON_MESSAGE"
    INTERNAL = "INTERNAL"


def _is_int(value: Any) -> bool:
    # bool is a subclass of int in Python; JSON true/false must not pass as a number.
    return isinstance(value, int) and not isinstance(value, bool)


def _check_attempts(attempts: Any) -> int:
    if not _is_int(attempts) or not 0 <= attempts <= MAX_INTEGER:
        raise InvalidEventError(f"attempts must be an integer between 0 and {MAX_INTEGER}, got {attempts!r}")
    return attempts


def _format_timestamp(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class IngestEvent:
    """A request from the center service to ingest one document."""

    id: str
    hash: str
    filename: str
    bucket_path: str
    size: int
    attempts: int
    created_at: datetime
    schema_version: int

    @staticmethod
    def parse_timestamp(text: Any) -> datetime:
        """Parse Go's created_at. Go prints up to nanoseconds; datetime keeps microseconds, so
        the extra digits are dropped."""
        if not isinstance(text, str) or _CREATED_AT.fullmatch(text) is None:
            raise InvalidEventError(f"created_at must be RFC 3339 UTC with a Z suffix, got {text!r}")
        try:
            return datetime.fromisoformat(text)
        except ValueError as exc:  # matches the pattern but is not a real date, e.g. 02-30
            raise InvalidEventError(f"created_at is not a valid timestamp: {text!r}") from exc

    @classmethod
    def parse(cls, raw: str | bytes) -> IngestEvent:
        try:
            document = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise InvalidEventError(f"payload is not valid JSON: {exc}") from exc
        if not isinstance(document, dict):
            raise InvalidEventError("payload must be a JSON object")

        def field_of(name: str) -> Any:
            if name not in document:
                raise InvalidEventError(f"{name} is required")
            return document[name]

        def text(name: str) -> str:
            value = field_of(name)
            if not isinstance(value, str) or value == "":
                raise InvalidEventError(f"{name} must be a non-empty string")
            return value

        version = field_of("schema_version")
        if not _is_int(version) or version != SCHEMA_VERSION:
            raise InvalidEventError(f"unsupported schema_version {version!r}")
        size = field_of("size")
        if not _is_int(size) or size < 0:
            raise InvalidEventError(f"size must be a non-negative integer, got {size!r}")

        return cls(
            id=text("id"),
            hash=text("hash"),
            filename=text("filename"),
            bucket_path=text("bucket_path"),
            size=size,
            attempts=_check_attempts(field_of("attempts")),
            created_at=cls.parse_timestamp(field_of("created_at")),
            schema_version=version,
        )


@dataclass(frozen=True)
class StatusEvent:
    """This service's report on one ingestion attempt. Build it with indexed() or failed()."""

    hash: str
    status: Status
    # The attempt number echoed from the ingest event, never one this service keeps (L12).
    attempts: int
    retryable: bool | None = None
    reason: Reason | None = None
    detail: str | None = None
    chunk_count: int | None = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not isinstance(self.hash, str) or self.hash == "":
            raise InvalidEventError("hash is required")
        _check_attempts(self.attempts)
        if self.created_at.tzinfo is None:
            raise InvalidEventError("created_at must be timezone-aware")
        if self.chunk_count is not None and (not _is_int(self.chunk_count) or self.chunk_count < 0):
            raise InvalidEventError(f"chunk_count must be a non-negative integer, got {self.chunk_count!r}")
        if self.status is Status.FAILED:
            if not isinstance(self.retryable, bool):
                raise InvalidEventError("retryable must be true or false when status is FAILED")
            if self.reason is None:
                raise InvalidEventError("reason is required when status is FAILED")

    @classmethod
    def indexed(
        cls,
        hash: str,
        attempts: int,
        *,
        chunk_count: int | None = None,
        id: str | None = None,
        created_at: datetime | None = None,
    ) -> StatusEvent:
        return cls(
            hash=hash,
            status=Status.INDEXED,
            attempts=attempts,
            chunk_count=chunk_count,
            **_identity(id, created_at),
        )

    @classmethod
    def failed(
        cls,
        hash: str,
        attempts: int,
        *,
        retryable: bool | None,
        reason: Reason | str | None,
        detail: str | None = None,
        id: str | None = None,
        created_at: datetime | None = None,
    ) -> StatusEvent:
        if reason is None:
            raise InvalidEventError("reason is required when status is FAILED")
        try:
            reason = Reason(reason)
        except ValueError as exc:
            raise InvalidEventError(f"unknown reason {reason!r}") from exc
        return cls(
            hash=hash,
            status=Status.FAILED,
            attempts=attempts,
            retryable=retryable,
            reason=reason,
            detail=None if detail is None else detail[:MAX_DETAIL_LENGTH],
            **_identity(id, created_at),
        )

    def to_dict(self) -> dict[str, Any]:
        document: dict[str, Any] = {
            "id": self.id,
            "hash": self.hash,
            "status": self.status.value,
            "attempts": self.attempts,
        }
        if self.retryable is not None:
            document["retryable"] = self.retryable
        if self.reason is not None:
            document["reason"] = self.reason.value
        if self.detail is not None:
            document["detail"] = self.detail
        if self.chunk_count is not None:
            document["chunk_count"] = self.chunk_count
        document["created_at"] = _format_timestamp(self.created_at)
        document["schema_version"] = SCHEMA_VERSION
        return document

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    def to_entry(self) -> dict[str, str]:
        """The fields of the stream entry that carries this event."""
        return {"event_type": STATUS_EVENT_TYPE, "payload": self.to_json()}


def _identity(id: str | None, created_at: datetime | None) -> dict[str, Any]:
    """Only the identity fields the caller set, so the dataclass defaults apply to the rest."""
    fields: dict[str, Any] = {}
    if id is not None:
        fields["id"] = id
    if created_at is not None:
        fields["created_at"] = created_at
    return fields
