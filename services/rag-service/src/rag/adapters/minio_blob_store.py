"""BlobStore over the MinIO ingestion bucket (PRD L5: Go owns the bucket, Python only reads)."""

from __future__ import annotations

import logging

import urllib3
from minio import Minio
from minio.error import MinioException, S3Error

from rag.ingest.errors import PermanentError, TransientError
from rag.transport.status_event import Reason

logger = logging.getLogger(__name__)

_READ_CHUNK = 64 * 1024


class MinioBlobStore:
    def __init__(self, client: Minio, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    def get(self, key: str, max_bytes: int) -> bytes:
        """Read the whole object, but never more than max_bytes: the event's size is only what
        the producer claimed, so the cap is enforced on the bytes actually read."""
        response = None
        try:
            response = self._client.get_object(self._bucket, key)
            parts: list[bytes] = []
            total = 0
            for part in response.stream(_READ_CHUNK):
                total += len(part)
                if total > max_bytes:
                    raise PermanentError(Reason.UNSUPPORTED_TYPE, f"file too large (over {max_bytes} bytes)")
                parts.append(part)
            return b"".join(parts)
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                # Go writes the object before it publishes the event, so a missing object is a
                # bug or a race; retrying is bounded by max_attempts (spec A1).
                raise TransientError(Reason.INTERNAL, "object missing") from None
            raise TransientError(Reason.DEPENDENCY_UNAVAILABLE, f"minio: {exc.code}") from None
        except (MinioException, urllib3.exceptions.HTTPError, OSError) as exc:
            # minio's retries end in urllib3's MaxRetryError, not S3Error (see core.minio_client).
            logger.warning("MinIO read failed: %s", type(exc).__name__)
            raise TransientError(Reason.DEPENDENCY_UNAVAILABLE, f"minio: {type(exc).__name__}") from None
        finally:
            if response is not None:
                response.close()
                response.release_conn()
