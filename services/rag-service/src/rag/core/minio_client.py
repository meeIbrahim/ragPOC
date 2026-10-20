"""The process-wide MinIO client.

Go owns both buckets and creates them in `make go_migrate` (PRD L5), so this module only builds
a client. It deliberately has no bucket-creating helper.
"""

from __future__ import annotations

import functools

import urllib3
from minio import Minio

from rag.core.config_reader import get_config

# minio's default is 5 retries with backoff; a failing dependency should surface quickly so the
# ingest job can report DEPENDENCY_UNAVAILABLE and let the center retry the attempt.
_RETRIES = 2


@functools.cache
def get_client() -> Minio:
    """One client per process. Every request is bounded by [rag].call_timeout (connect and
    read). Creating the client makes no request."""
    config = get_config()
    timeout = config.rag.call_timeout.total_seconds()
    http = urllib3.PoolManager(
        timeout=urllib3.Timeout(connect=timeout, read=timeout),
        retries=urllib3.Retry(total=_RETRIES, backoff_factor=0.2, status_forcelist=(500, 502, 503, 504)),
    )
    return Minio(
        config.minio.url,
        access_key=config.minio.access_key,
        secret_key=config.minio.secret_key,
        secure=False,  # plain HTTP, like the Go client
        http_client=http,
    )
