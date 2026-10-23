"""The two stream events between the Go center service and this service.

- IngestEvent: Go -> Python on `ingest.docs` (PRD 4.1). This side only parses it.
- StatusEvent: Python -> Go on `ingest.status` (PRD 4.2). This side only produces it.

The machine-checkable contract is contracts/events/*.schema.json plus the fixtures next to it;
tests/test_contracts.py runs both against this module, and the Go tests run against the same
files. Parsing is strict: integer fields must be JSON integers (Go's producer only ever emits
integers; Go's status decoder is the lenient side and also takes 1.0). A malformed ingest event
is the consumer's problem to log and acknowledge, not to guess at.
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


# Untrusted values are repeated in error messages, which get logged: keep them short. The Go
# decoder caps its echoes the same way.
_MAX_ECHO = 64


def _clip(value: Any) -> str:
    """A bounded description of an untrusted value for an error message. It must never raise
    itself: that would replace the error it was written to report."""
    try:
        if isinstance(value, int) and not isinstance(value, bool) and value.bit_length() > 256:
            return f"<int of {value.bit_length()} bits>"  # repr() of a huge int can raise ValueError
        if isinstance(value, (str, bytes)) and len(value) > _MAX_ECHO:
            return repr(value[:_MAX_ECHO])[:-1] + "..."  # do not build the repr of a 10 MB string
        text = repr(value)
    except Exception:
        return f"<unprintable {type(value).__name__}>"
    return text if len(text) <= _MAX_ECHO else text[:_MAX_ECHO] + "..."


def _is_int(value: Any) -> bool:
    # bool is a subclass of int in Python; JSON true/false must not pass as a number.
    return isinstance(value, int) and not isinstance(value, bool)


def _check_attempts(attempts: Any) -> int:
    if not _is_int(attempts) or not 0 <= attempts <= MAX_INTEGER:
        raise InvalidEventError(f"attempts must be an integer between 0 and {MAX_INTEGER}, got {_clip(attempts)}")
    return attempts


def _coerce(enum_type: type[StrEnum], value: Any, name: str) -> Any:
    try:
        return enum_type(value)
    except ValueError:
        raise InvalidEventError(f"unknown {name} {_clip(value)}") from None


def format_timestamp(moment: datetime) -> str:
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
            raise InvalidEventError(f"created_at must be RFC 3339 UTC with a Z suffix, got {_clip(text)}")
        try:
            return datetime.fromisoformat(text)
        except ValueError as exc:  # matches the pattern but is not a real date, e.g. 02-30
            raise InvalidEventError(f"created_at is not a valid timestamp: {_clip(text)}") from exc

    @classmethod
    def parse(cls, raw: str | bytes) -> IngestEvent:
        try:
            document = json.loads(raw)
        except (ValueError, RecursionError) as exc:
            # ValueError covers JSONDecodeError, UnicodeDecodeError and the plain ValueError
            # json.loads raises for an integer of more than 4300 digits. RecursionError (deeply
            # nested garbage) is not a ValueError. Neither may escape the consumer's "log and
            # acknowledge" handling.
            raise InvalidEventError(f"payload is not valid JSON: {type(exc).__name__}") from exc
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
            try:
                value.encode("utf-8")
            except UnicodeEncodeError:
                # A lone surrogate is schema-valid JSON, but SQLite cannot store it and Go reads
                # it back as U+FFFD, so the value would not match across the two services.
                raise InvalidEventError(f"{name} must be valid unicode text") from None
            return value

        version = field_of("schema_version")
        if not _is_int(version) or version != SCHEMA_VERSION:
            raise InvalidEventError(f"unsupported schema_version {_clip(version)}")
        size = field_of("size")
        if not _is_int(size) or size < 0:
            raise InvalidEventError(f"size must be a non-negative integer, got {_clip(size)}")

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
        # The public constructor is as strict as the factories: a plain "FAILED" string would
        # otherwise skip the FAILED checks and only crash later, in to_json().
        # The dataclass is frozen, so coerced values are written with object.__setattr__.
        if not isinstance(self.hash, str) or self.hash == "":
            raise InvalidEventError("hash is required")
        object.__setattr__(self, "status", _coerce(Status, self.status, "status"))
        if self.reason is not None:
            object.__setattr__(self, "reason", _coerce(Reason, self.reason, "reason"))
        _check_attempts(self.attempts)
        if not isinstance(self.id, str) or self.id == "":
            raise InvalidEventError("id must be a non-empty string")
        if not isinstance(self.created_at, datetime):
            raise InvalidEventError(f"created_at must be a datetime, got {_clip(self.created_at)}")
        # Aware means utcoffset() is not None: a tzinfo that returns None is treated as naive by
        # Python and astimezone() would silently read it as host-local time. Convert once, here,
        # so nothing that passes the constructor can fail later in to_json().
        try:
            if self.created_at.utcoffset() is None:
                raise InvalidEventError("created_at must be timezone-aware")
            created_at = self.created_at.astimezone(UTC)
        except InvalidEventError:
            raise
        except Exception as exc:  # a broken tzinfo, or a moment that overflows when moved to UTC
            raise InvalidEventError(f"created_at cannot be converted to UTC: {type(exc).__name__}") from exc
        object.__setattr__(self, "created_at", created_at)
        if self.retryable is not None and not isinstance(self.retryable, bool):
            raise InvalidEventError(f"retryable must be true or false, got {_clip(self.retryable)}")
        if self.detail is not None:
            if not isinstance(self.detail, str):
                raise InvalidEventError(f"detail must be text, got {_clip(self.detail)}")
            if len(self.detail) > MAX_DETAIL_LENGTH:
                object.__setattr__(self, "detail", self.detail[:MAX_DETAIL_LENGTH])
        if self.chunk_count is not None and (not _is_int(self.chunk_count) or not 0 <= self.chunk_count <= MAX_INTEGER):
            raise InvalidEventError(f"chunk_count must be an integer between 0 and {MAX_INTEGER}, got {_clip(self.chunk_count)}")
        if self.status is Status.FAILED:
            if self.retryable is None:
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
        return cls(
            hash=hash,
            status=Status.FAILED,
            attempts=attempts,
            retryable=retryable,
            reason=reason,
            detail=detail,
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
        document["created_at"] = format_timestamp(self.created_at)
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
