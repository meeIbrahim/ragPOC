"""The process-wide Qdrant client."""

from __future__ import annotations

import functools

from qdrant_client import QdrantClient

from rag.core.config_reader import Config, get_config


def build_client(config: Config) -> QdrantClient:
    """A client bounded by [rag].call_timeout. Creating it makes no request:
    check_compatibility=False skips the version probe the constructor would otherwise send."""
    return QdrantClient(
        url=config.qdrant.url,
        timeout=int(config.rag.call_timeout.total_seconds()),
        check_compatibility=False,
    )


@functools.cache
def get_client() -> QdrantClient:
    """One client per process, for the process-wide config."""
    return build_client(get_config())


def close_client() -> None:
    """Close the process-wide client if one was built. It never builds one just to close it, and
    it drops the closed client from the cache so a later get_client() builds a fresh one."""
    if get_client.cache_info().currsize:
        client = get_client()
        get_client.cache_clear()
        client.close()
