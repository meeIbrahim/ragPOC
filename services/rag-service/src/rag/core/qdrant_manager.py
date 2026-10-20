"""The process-wide Qdrant client."""

from __future__ import annotations

import functools

from qdrant_client import QdrantClient

from rag.core.config_reader import get_config


@functools.cache
def get_client() -> QdrantClient:
    """One client per process, bounded by [rag].call_timeout. Creating it makes no request:
    check_compatibility=False skips the version probe the constructor would otherwise send."""
    config = get_config()
    return QdrantClient(
        url=config.qdrant.url,
        timeout=int(config.rag.call_timeout.total_seconds()),
        check_compatibility=False,
    )
