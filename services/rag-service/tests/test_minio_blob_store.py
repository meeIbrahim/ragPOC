"""R2 MinioBlobStore: bounded read and error mapping (spec D8/D13, A1, E8, E9)."""

import os
import uuid

import pytest
import urllib3
from minio.error import S3Error, ServerError

from rag.adapters.minio_blob_store import MinioBlobStore
from rag.ingest.errors import PermanentError, TransientError
from rag.transport.status_event import Reason


class FakeResponse:
    def __init__(self, data: bytes, chunk: int = 4):
        self.data = data
        self.chunk = chunk
        self.closed = False
        self.released = False

    def stream(self, amt):
        for i in range(0, len(self.data), self.chunk):
            yield self.data[i : i + self.chunk]

    def close(self):
        self.closed = True

    def release_conn(self):
        self.released = True


class FakeMinio:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.requests = []

    def get_object(self, bucket, key):
        self.requests.append((bucket, key))
        if self.error:
            raise self.error
        return self.response


def s3_error(code):
    return S3Error(None, code, "msg", "/r", "req", "host", bucket_name="b", object_name="k")


def test_reads_the_object_from_the_ingestion_bucket():
    response = FakeResponse(b"hello world")
    client = FakeMinio(response)
    assert MinioBlobStore(client, "ingestion").get("aa/bb/h", max_bytes=100) == b"hello world"
    assert client.requests == [("ingestion", "aa/bb/h")]
    assert response.closed and response.released


def test_exactly_max_bytes_is_allowed():
    assert MinioBlobStore(FakeMinio(FakeResponse(b"12345678")), "b").get("k", max_bytes=8) == b"12345678"


def test_more_than_max_bytes_is_a_permanent_too_large_failure_and_stops_reading():
    response = FakeResponse(b"x" * 100)
    with pytest.raises(PermanentError) as excinfo:
        MinioBlobStore(FakeMinio(response), "b").get("k", max_bytes=10)
    assert excinfo.value.reason is Reason.UNSUPPORTED_TYPE
    assert "too large" in excinfo.value.detail
    assert response.closed and response.released


def test_a_missing_object_is_a_retryable_internal_failure():
    with pytest.raises(TransientError) as excinfo:
        MinioBlobStore(FakeMinio(error=s3_error("NoSuchKey")), "b").get("k", 10)
    assert excinfo.value.reason is Reason.INTERNAL
    assert excinfo.value.detail == "object missing"


@pytest.mark.parametrize(
    "error",
    [
        s3_error("NoSuchBucket"),
        ServerError("bad gateway", 502),
        urllib3.exceptions.MaxRetryError(None, "/k", "too many 503 error responses"),
        urllib3.exceptions.ProtocolError("connection reset"),
        urllib3.exceptions.ReadTimeoutError(None, "/k", "read timed out"),
        ConnectionRefusedError(61, "refused"),
    ],
    ids=lambda e: type(e).__name__,
)
def test_dependency_failures_are_retryable(error):
    with pytest.raises(TransientError) as excinfo:
        MinioBlobStore(FakeMinio(error=error), "b").get("k", 10)
    assert excinfo.value.reason is Reason.DEPENDENCY_UNAVAILABLE


def test_a_failure_mid_stream_still_releases_the_connection():
    class Broken(FakeResponse):
        def stream(self, amt):
            yield b"ab"
            raise urllib3.exceptions.ProtocolError("reset")

    response = Broken(b"")
    with pytest.raises(TransientError):
        MinioBlobStore(FakeMinio(response), "b").get("k", 10)
    assert response.closed and response.released


# ---- integration: the throwaway or compose MinIO (MINIO_URL, e.g. localhost:19000)


@pytest.mark.integration
def test_round_trip_against_a_real_minio():
    from minio import Minio

    url = os.environ.get("MINIO_URL")
    if not url:
        pytest.skip("set MINIO_URL (and MINIO_ACCESS_KEY / MINIO_SECRET_KEY)")
    client = Minio(url, os.environ["MINIO_ACCESS_KEY"], os.environ["MINIO_SECRET_KEY"], secure=False)
    bucket = "p1-it-" + uuid.uuid4().hex[:8]
    client.make_bucket(bucket)  # test setup only: the service itself never creates buckets
    try:
        import io

        client.put_object(bucket, "aa/bb/hash", io.BytesIO(b"payload"), 7)
        store = MinioBlobStore(client, bucket)
        assert store.get("aa/bb/hash", 7) == b"payload"
        with pytest.raises(PermanentError):
            store.get("aa/bb/hash", 6)
        with pytest.raises(TransientError) as excinfo:
            store.get("aa/bb/missing", 10)
        assert excinfo.value.detail == "object missing"
    finally:
        for obj in client.list_objects(bucket, recursive=True):
            client.remove_object(bucket, obj.object_name)
        client.remove_bucket(bucket)
