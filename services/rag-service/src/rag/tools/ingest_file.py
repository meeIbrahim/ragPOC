"""Dev tool: ingest one file without the Go service (PRD 5.1, spec R8).

    python -m rag.tools.ingest_file <path> [--config PATH] [--wait SECONDS]

Hashes the file, uploads it to the ingestion bucket under the content-addressed key Go uses,
and XADDs a schema-valid ingest event with attempts=0. With --wait it follows the status stream
until that hash's status event arrives.

Exit codes: 0 published (or INDEXED with --wait), 1 FAILED, 2 usage or connection error,
3 --wait timed out.

POC test data only: indexed text is later sent to Groq when queried (PRD 5.4).
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path

import redis
import urllib3
from minio import Minio
from minio.error import MinioException

from rag.core import config_reader, minio_client
from rag.core.config_reader import Config
from rag.transport.status_event import IngestEvent, format_timestamp

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_TIMEOUT = 3

INGEST_EVENT_TYPE = "IngestionRequest"
SCHEMA_VERSION = 1
# How long one XREAD blocks before the --wait deadline is checked again.
_POLL_MS = 1000


def object_key(digest: str) -> str:
    """The ingestion bucket key Go's storage layer uses: aa/bb/<hash>."""
    return f"{digest[:2]}/{digest[2:4]}/{digest}"


def _last_status_id(client: redis.Redis, stream: str) -> str:
    newest = client.xrevrange(stream, "+", "-", count=1)
    return newest[0][0] if newest else "0-0"


def _wait(client: redis.Redis, stream: str, after: str, digest: str, seconds: float,
          clock: Callable[[], float]) -> int:
    deadline = clock() + seconds
    last = after
    while clock() < deadline:
        for _, entries in client.xread({stream: last}, count=100, block=_POLL_MS):
            for entry_id, fields in entries:
                last = entry_id
                try:
                    payload = json.loads(fields.get("payload", ""))
                except ValueError:
                    continue  # not ours to judge; the Go consumer drops malformed entries
                if not isinstance(payload, dict) or payload.get("hash") != digest:
                    continue
                print(json.dumps(payload, indent=2))
                return EXIT_OK if payload.get("status") == "INDEXED" else EXIT_FAILED
    print(f"no status for {digest} within {seconds:g}s: is rag-consumer running?", file=sys.stderr)
    return EXIT_TIMEOUT


def run(path: Path, config: Config, minio: Minio, redis_client: redis.Redis, wait: float | None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        clock: Callable[[], float] = time.monotonic) -> int:
    try:
        data = path.read_bytes()
    except OSError as exc:
        print(f"cannot read {path}: {exc.strerror}", file=sys.stderr)
        return EXIT_USAGE

    digest = hashlib.sha256(data).hexdigest()
    key = object_key(digest)
    payload = json.dumps({
        "id": str(uuid.uuid4()),
        "hash": digest,
        "filename": path.name,
        "bucket_path": key,
        "size": len(data),
        "attempts": 0,
        "created_at": format_timestamp(now()),
        "schema_version": SCHEMA_VERSION,
    })
    IngestEvent.parse(payload)  # the consumer's own validation: never publish what it would drop

    try:
        minio.put_object(config.minio.ingestion_bucket, key, io.BytesIO(data), len(data))
    except (MinioException, urllib3.exceptions.HTTPError, OSError) as exc:
        print(f"MinIO upload to {config.minio.url} failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_USAGE

    try:
        after = _last_status_id(redis_client, config.redis.status_stream)
        redis_client.xadd(
            config.redis.stream,
            {"event_type": INGEST_EVENT_TYPE, "payload": payload},
            maxlen=config.redis.stream_max_len,
            approximate=True,
        )
        print(digest)
        if wait is None:
            return EXIT_OK
        return _wait(redis_client, config.redis.status_stream, after, digest, wait, clock)
    except redis.exceptions.RedisError as exc:
        print(f"Redis at {config.redis.host}:{config.redis.port} failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_USAGE


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m rag.tools.ingest_file", description=__doc__.splitlines()[0])
    parser.add_argument("path", type=Path)
    parser.add_argument("--config", help="path to config.toml (default: $RAG_CONFIG, else the repo's config.toml)")
    parser.add_argument("--wait", type=float, metavar="SECONDS", help="wait this long for the status event")
    args = parser.parse_args(argv)
    try:
        config = config_reader.load(args.config)
    except config_reader.ConfigError as exc:
        print(exc, file=sys.stderr)
        return EXIT_USAGE
    timeout = config.rag.call_timeout.total_seconds()
    client = redis.Redis(
        host=config.redis.host,
        port=config.redis.port,
        decode_responses=True,
        protocol=2,
        socket_timeout=timeout + _POLL_MS / 1000,
        socket_connect_timeout=timeout,
    )
    return run(args.path, config, minio_client.build_client(config), client, args.wait)


if __name__ == "__main__":
    sys.exit(main())
