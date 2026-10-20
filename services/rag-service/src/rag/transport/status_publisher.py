"""Publishes status events to the status stream the Go center service reads."""

from __future__ import annotations

from typing import Any

from rag.transport.status_event import StatusEvent


class StatusPublisher:
    """XADD one StatusEvent. Errors from Redis reach the caller: the consumer must know the
    publish failed so it leaves the ingest entry pending instead of acknowledging it."""

    def __init__(self, redis: Any, stream: str, maxlen: int) -> None:
        if not stream:
            raise ValueError("status stream name must not be empty")
        if maxlen < 1:
            raise ValueError("status stream maxlen must be >= 1")
        self._redis = redis
        self._stream = stream
        self._maxlen = maxlen

    def publish(self, event: StatusEvent) -> str:
        """Returns the stream entry id. The stream is trimmed approximately, to the same cap as
        the ingest stream (config [redis].stream_max_len)."""
        return self._redis.xadd(self._stream, event.to_entry(), maxlen=self._maxlen, approximate=True)
