"""R8 `python -m rag.tools.ingest_file`: upload, schema-valid event, --wait (spec A10, E23)."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import jsonschema
import pytest
import redis
from minio.error import S3Error

from rag.tools import ingest_file
from rag.transport.status_event import IngestEvent

SCHEMA = json.loads((Path(__file__).resolve().parents[3] / "contracts" / "events" / "ingest_event.schema.json").read_text())
NOW = datetime(2026, 10, 23, 7, 30, 15, 123456, tzinfo=UTC)


class FakeMinio:
    def __init__(self, error=None):
        self.error = error
        self.puts = []

    def put_object(self, bucket, key, data, length):
        if self.error:
            raise self.error
        self.puts.append((bucket, key, data.read(), length))


class FakeRedis:
    """XREVRANGE / XADD / XREAD on in-memory streams. `replies` are status entries that appear
    on the status stream after the XADD."""

    def __init__(self, status=(), replies=(), error=None):
        self.streams = {"ingest.status": list(status), "ingest.docs": []}
        self.replies = list(replies)
        self.error = error
        self.reads = []

    def xrevrange(self, name, max="+", min="-", count=None):
        if self.error:
            raise self.error
        return list(reversed(self.streams[name]))[:count]

    def xadd(self, name, fields, maxlen=None, approximate=True):
        entry_id = f"{len(self.streams[name]) + 100}-0"
        self.streams[name].append((entry_id, fields))
        self.streams["ingest.status"].extend(self.replies)
        self.replies = []
        return entry_id

    def xread(self, streams, count=None, block=None):
        (name, last), = streams.items()
        self.reads.append(last)
        entries = self.streams[name]
        ids = [e[0] for e in entries]
        start = ids.index(last) + 1 if last in ids else 0
        new = entries[start:]
        return [[name, new]] if new else []


def status(entry_id, hash, outcome="INDEXED", **extra):
    payload = {"id": "s", "hash": hash, "status": outcome, "attempts": 0, "schema_version": 1,
               "created_at": "2026-10-23T07:30:16Z", **extra}
    return (entry_id, {"event_type": "IngestionStatus", "payload": json.dumps(payload)})


@pytest.fixture
def doc(tmp_path):
    path = tmp_path / "Guide One.md"
    path.write_bytes(b"# Guide\n\nhello\n")
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def run(rag_config, path, minio=None, redis_client=None, wait=None, clock=None):
    return ingest_file.run(
        path,
        config=rag_config,
        minio=minio or FakeMinio(),
        redis_client=redis_client or FakeRedis(),
        wait=wait,
        now=lambda: NOW,
        clock=clock or (lambda: 0.0),
    )


def test_object_key_matches_the_go_layout():
    assert ingest_file.object_key("ab" + "cd" + "e" * 60) == f"ab/cd/abcd{'e' * 60}"


def test_uploads_to_the_ingestion_bucket_under_the_content_hash(rag_config, doc):
    path, digest = doc
    minio = FakeMinio()
    assert run(rag_config, path, minio=minio) == 0
    assert minio.puts == [("ingestion-bucket", f"{digest[:2]}/{digest[2:4]}/{digest}", path.read_bytes(), 15)]


def test_publishes_a_schema_valid_ingest_event(rag_config, doc):
    path, digest = doc
    client = FakeRedis()
    run(rag_config, path, redis_client=client)
    ((_, fields),) = client.streams["ingest.docs"]
    assert fields["event_type"] == "IngestionRequest"
    payload = json.loads(fields["payload"])
    jsonschema.Draft202012Validator(SCHEMA).validate(payload)
    event = IngestEvent.parse(fields["payload"])
    assert (event.hash, event.filename, event.size, event.attempts) == (digest, "Guide One.md", 15, 0)
    assert event.bucket_path == ingest_file.object_key(digest)
    assert payload["created_at"] == "2026-10-23T07:30:15.123456Z"


def test_prints_the_hash(rag_config, doc, capsys):
    path, digest = doc
    run(rag_config, path)
    assert digest in capsys.readouterr().out


def test_wait_returns_0_on_indexed_and_ignores_older_and_other_entries(rag_config, doc, capsys):
    path, digest = doc
    client = FakeRedis(
        status=[status("5-0", digest, "FAILED", retryable=False, reason="CORRUPT_FILE")],  # before our XADD
        replies=[status("200-0", "f" * 64, "FAILED", retryable=True, reason="INTERNAL"), status("201-0", digest)],
    )
    assert run(rag_config, path, redis_client=client, wait=10) == 0
    assert client.reads[0] == "5-0"  # read only what arrived after the XADD
    assert '"INDEXED"' in capsys.readouterr().out


def test_wait_returns_1_on_failed(rag_config, doc):
    path, digest = doc
    client = FakeRedis(replies=[status("201-0", digest, "FAILED", retryable=False, reason="UNSUPPORTED_TYPE")])
    assert run(rag_config, path, redis_client=client, wait=10) == 1


def test_wait_times_out_with_exit_3_and_a_hint_E23(rag_config, doc, capsys):
    path, _ = doc
    ticks = iter([0.0, 5.0, 11.0, 12.0])
    assert run(rag_config, path, wait=10, clock=lambda: next(ticks)) == 3
    assert "is rag-consumer running?" in capsys.readouterr().err


def test_an_empty_status_stream_waits_from_the_start(rag_config, doc):
    path, digest = doc
    client = FakeRedis(replies=[status("201-0", digest)])
    assert run(rag_config, path, redis_client=client, wait=10) == 0
    assert client.reads[0] == "0-0"


def test_a_minio_failure_is_one_line_and_exit_2(rag_config, doc, capsys):
    path, _ = doc
    error = S3Error(None, "AccessDenied", "denied", "/r", "req", "host")
    assert run(rag_config, path, minio=FakeMinio(error=error)) == 2
    err = capsys.readouterr().err
    assert "MinIO" in err and "localhost:9000" in err and "Traceback" not in err


def test_a_redis_failure_is_one_line_and_exit_2(rag_config, doc, capsys):
    path, _ = doc
    client = FakeRedis(error=redis.exceptions.ConnectionError("refused"))
    assert run(rag_config, path, redis_client=client) == 2
    err = capsys.readouterr().err
    assert "Redis" in err and "localhost:6379" in err


def test_a_missing_file_is_exit_2(rag_config, tmp_path, capsys):
    assert run(rag_config, tmp_path / "nope.pdf") == 2
    assert "nope.pdf" in capsys.readouterr().err
