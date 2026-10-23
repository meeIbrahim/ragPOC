"""The process-wide MinIO client.

Go owns both buckets and creates them in `make go_migrate` (PRD L5), so this module only builds
a client. It deliberately has no bucket-creating helper.
"""

from __future__ import annotations

import functools

import urllib3
from minio import Minio

from rag.core.config_reader import Config, get_config

# minio's default is 5 retries with backoff; a failing dependency should surface quickly so the
# ingest job can report DEPENDENCY_UNAVAILABLE and let the center retry the attempt.
_RETRIES = 2


def build_client(config: Config) -> Minio:
    """A client for the given config. Creating it makes no request.

    What [rag].call_timeout bounds: the connect, and each socket read (an idle timeout). It does
    NOT bound a whole call: a download that trickles one byte per read is never cut off, and
    Retry(total=2) allows three tries, so one call can take about three times the timeout.
    The ingest job's deadline and watchdog (job_timeout) are what bound total time. A retried
    request that keeps failing with a 5xx raises urllib3's MaxRetryError, not minio's S3Error,
    so callers mapping failures to DEPENDENCY_UNAVAILABLE must catch the urllib3 errors too.
    PUT is retried by urllib3's defaults; POST is not."""
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


@functools.cache
def get_client() -> Minio:
    """One client per process, for the process-wide config."""
    return build_client(get_config())
