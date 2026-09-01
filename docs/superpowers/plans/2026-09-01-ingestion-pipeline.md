# Ingestion Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the event-driven PDF ingestion pipeline: a new Go central service (presigned upload + confirm + outbox) publishing to a Redis Stream, consumed by a reworked Python RAG worker that indexes into a networked Qdrant.

**Architecture:** Client → Go `/ingest/init` (presigned MinIO POST policy) → client uploads to MinIO directly → Go `/ingest/confirm` (server-side hash, dedup, DB insert, XADD with outbox fallback) → Redis Stream `ingest.docs` → Python RAG worker (XREADGROUP, dedup check, chunk/embed/upsert to Qdrant, XACK).

**Tech Stack:** Python 3.13 (uv) — existing `langchain`/`qdrant-client`/`minio`/`ollama` stack, + `redis` client. Go 1.22+ — stdlib `net/http`, `modernc.org/sqlite` (pure-Go, no cgo), `github.com/minio/minio-go/v7`, `github.com/redis/go-redis/v9`, `github.com/BurntSushi/toml`, `github.com/google/uuid`. Redis 7 and Qdrant, both via docker-compose.

**Spec:** `docs/superpowers/specs/2026-09-01-ingestion-pipeline-design.md`

## Global Constraints

- Go 1.22+ required (uses `net/http`'s method-prefixed mux patterns, e.g. `"POST /ingest/init"`, added in 1.22).
- Qdrant runs networked via docker (`qdrant/qdrant` image), not embedded/local-mode. Collection name `pdf-fixed`, HNSW config unchanged from existing `qdrant_collection_v1.py` (`m=4, ef_construct=100, full_scan_threshold=1`).
- Redis Stream name `ingest.docs`, consumer group `rag-pdf-fixed` (mirrors the Qdrant collection name — one group per strategy, per the deferred-routing note in the spec).
- Two independent SQLite files: Go's `./go_sqlite_db/documents.db`, RAG's existing `./sqlite_db/pdf-fixed.db`. No shared DB.
- No RPC/gRPC surface for ingestion — Redis Stream is the only Go→RAG path (spec §4.2). Existing `protos/rag/v1/rag.proto` / `src/api/server.py` are untouched.
- Presigned upload uses MinIO's POST policy (`PresignedPostPolicy`, multipart form) — matches the "upload via simple POST call" requirement literally, not a PUT-based presigned URL.
- Outbox pattern is mandatory on the Go confirm endpoint: a DB row is durable proof of "confirmed" even if the inline `XADD` fails; a background poller retries.
- Qdrant point IDs must be an unsigned integer or UUID (hard Qdrant constraint) — point id is `uuid5("{hash_id}:{chunk_id}")`, with `hash_id`/`chunk_id` carried as explicit payload fields.
- Query (`Ask`/`AskStream`) stays out of scope — not touched by any task here.

---

## Part A — Python RAG service

### Task 1: Fix regressions, network Qdrant, add infra containers

**Files:**
- Modify: `src/storage/minio_storage.py`
- Modify: `src/core/qdrant_manager.py`
- Modify: `config.toml`
- Modify: `docker-compose.yml`

**Interfaces:**
- Produces: `minio_storage.initialize_bucket(bucket: str) -> None`, `minio_storage.get_client() -> Minio`, `qdrant_manager.get_client() -> QdrantClient` (now networked).

- [ ] **Step 1: Restore `initialize_bucket` and expose a client getter in `minio_storage.py`**

```python
# src/storage/minio_storage.py
import logging

from minio import Minio
from core import config_reader

_minio_config = config_reader.get_config().minio
_BUCKET=_minio_config.bucket
_URL=_minio_config.url
_ACCESS_KEY=_minio_config.access_key
_SECRET_KEY=_minio_config.secret_key
_logger = logging.getLogger(__name__)

_client = Minio(
    _URL,
    _ACCESS_KEY,
    _SECRET_KEY,
    secure=False
)


def get_client() -> Minio:
    return _client


def initialize_bucket(bucket: str) -> None:
    if _client.bucket_exists(bucket):
        _logger.info("Bucket %s already exists", bucket)
        return
    _logger.info("Creating bucket %s", bucket)
    _client.make_bucket(bucket)
```

- [ ] **Step 2: Switch `qdrant_manager.py` to a networked client, drop the now-unused path-based delete helper**

```python
# src/core/qdrant_manager.py
from core import config_reader
from qdrant_client import QdrantClient

_client = QdrantClient(url=config_reader.get_config().qdrant.url)


def get_client() -> QdrantClient:
    return _client
```

- [ ] **Step 3: Update `config.toml`** — change `[qdrant]` from `dir` to `url`, add `[redis]` and `[go]` sections (the `[go]` section is read only by the Go service; Python's config loader ignores unknown top-level keys)

```toml
[minio]
url="localhost:9000"
access_key="minio_user"
secret_key="minio_password"
bucket="rag-pdf-source"

[sqlite]
dir="./sqlite_db"

[qdrant]
url="localhost:6333"

[server]
port=50051

[rag]
collection="pdf-fixed"
model="qwen3-embedding"

[redis]
host="localhost"
port=6379
stream="ingest.docs"
consumer_group="rag-pdf-fixed"

[go]
port=8080
sqlite_dir="./go_sqlite_db"
```

- [ ] **Step 4: Add `qdrant` and `redis` services to `docker-compose.yml`**

```yaml
services:
  minio:
    image: quay.io/minio/minio
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minio_user
      MINIO_ROOT_PASSWORD: minio_password
    volumes:
      - ./ingestion_docs/data:/data
    command: server /data --console-address ":9001"

  qdrant:
    image: qdrant/qdrant
    container_name: qdrant
    ports:
      - "6333:6333"
    volumes:
      - ./qdrant_data:/qdrant/storage

  redis:
    image: redis:7
    container_name: redis
    ports:
      - "6379:6379"
```

- [ ] **Step 5: Verify infra comes up and old regression is gone**

Run:
```bash
docker compose up -d
curl -sf http://localhost:6333/readyz
redis-cli -h localhost -p 6379 ping
```
Expected: `curl` succeeds (Qdrant ready), `redis-cli` prints `PONG`.

- [ ] **Step 6: Commit**

```bash
git add src/storage/minio_storage.py src/core/qdrant_manager.py config.toml docker-compose.yml
git commit -m "fix: restore bucket init, network qdrant, add redis/qdrant containers"
```

---

### Task 2: Add `RedisConfig` to `config_reader.py`

**Files:**
- Modify: `src/core/config_reader.py`
- Test: `tests/core/test_config_reader.py`

**Interfaces:**
- Produces: `RedisConfig(host: str, port: int, stream: str, consumer_group: str)`, `Config.redis: RedisConfig`, `QdrantConfig(url: str)`.

- [ ] **Step 1: Write the failing test**

```python
# tests/core/test_config_reader.py
import textwrap

from core import config_reader


def test_loads_redis_and_qdrant_url_config(tmp_path, monkeypatch):
    config_file = tmp_path / "config.toml"
    config_file.write_text(textwrap.dedent("""
        [minio]
        url="localhost:9000"
        access_key="minio_user"
        secret_key="minio_password"
        bucket="rag-pdf-source"

        [sqlite]
        dir="./sqlite_db"

        [qdrant]
        url="localhost:6333"

        [server]
        port=50051

        [rag]
        collection="pdf-fixed"
        model="qwen3-embedding"

        [redis]
        host="localhost"
        port=6379
        stream="ingest.docs"
        consumer_group="rag-pdf-fixed"
        """))
    monkeypatch.chdir(tmp_path)
    config_reader.get_config.cache_clear()

    config = config_reader.get_config()

    assert config.qdrant.url == "localhost:6333"
    assert config.redis.host == "localhost"
    assert config.redis.port == 6379
    assert config.redis.stream == "ingest.docs"
    assert config.redis.consumer_group == "rag-pdf-fixed"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_config_reader.py -v`
Expected: FAIL — `KeyError: 'redis'` or `AttributeError` (no `redis` field on `Config` yet).

- [ ] **Step 3: Implement**

```python
# src/core/config_reader.py
import os
import tomllib
from dataclasses import dataclass
from functools import lru_cache


CONFIG_FILE = "config.toml"


@dataclass(frozen=True)
class MinioConfig:
    url: str
    access_key: str
    secret_key: str
    bucket: str


@dataclass(frozen=True)
class SqliteConfig:
    dir: str


@dataclass(frozen=True)
class QdrantConfig:
    url: str


@dataclass(frozen=True)
class ServerConfig:
    port: int


@dataclass(frozen=True)
class RagConfig:
    collection: str
    model: str


@dataclass(frozen=True)
class RedisConfig:
    host: str
    port: int
    stream: str
    consumer_group: str


@dataclass(frozen=True)
class Config:
    minio: MinioConfig
    sqlite: SqliteConfig
    qdrant: QdrantConfig
    server: ServerConfig
    rag: RagConfig
    redis: RedisConfig


def _load_config() -> Config:
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Missing config file: {CONFIG_FILE}")

    with open(CONFIG_FILE, "rb") as f:
        data = tomllib.load(f)

    return Config(
        minio=MinioConfig(**data["minio"]),
        sqlite=SqliteConfig(**data["sqlite"]),
        qdrant=QdrantConfig(**data["qdrant"]),
        server=ServerConfig(**data["server"]),
        rag=RagConfig(**data["rag"]),
        redis=RedisConfig(**data["redis"]),
    )


@lru_cache(maxsize=1)
def get_config() -> Config:
    return _load_config()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core/test_config_reader.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/core/config_reader.py tests/core/test_config_reader.py
git commit -m "feat: add RedisConfig, switch QdrantConfig to url"
```

---

### Task 3: Replace `ingestion_storage`/`documents` tables with `indexed_documents`

**Files:**
- Modify: `src/migration/sqlite_create_table_v1.py`
- Remove: `src/db/queries/documents.py`
- Create: `src/db/queries/indexed_documents.py`
- Test: `tests/db/test_indexed_documents.py`

**Interfaces:**
- Produces: `indexed_documents.get_by_hash(collection: str, hash_id: str, db_dir: str | None = None) -> sqlite3.Row | None`, `indexed_documents.save_indexed(collection: str, hash_id: str, object_path: str, chunk_count: int, db_dir: str | None = None) -> None`.
- Consumed by: Task 6's `rag/ingest.py`.

- [ ] **Step 1: Write the failing test**

```python
# tests/db/test_indexed_documents.py
import pytest

from db.queries import indexed_documents
from db.sqlite_manager import SQLiteManager


@pytest.fixture
def db_dir(tmp_path):
    manager = SQLiteManager("pdf-fixed", str(tmp_path))
    with manager as conn:
        conn.execute("""
            CREATE TABLE indexed_documents (
                hash_id TEXT PRIMARY KEY NOT NULL,
                object_path TEXT NOT NULL,
                chunk_count INTEGER NOT NULL,
                indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    return str(tmp_path)


def test_get_by_hash_returns_none_when_absent(db_dir):
    assert indexed_documents.get_by_hash("pdf-fixed", "missing", db_dir=db_dir) is None


def test_save_then_get_round_trip(db_dir):
    indexed_documents.save_indexed("pdf-fixed", "abc123", "objects/abc.pdf", 3, db_dir=db_dir)

    row = indexed_documents.get_by_hash("pdf-fixed", "abc123", db_dir=db_dir)

    assert row["object_path"] == "objects/abc.pdf"
    assert row["chunk_count"] == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/db/test_indexed_documents.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'db.queries.indexed_documents'`

- [ ] **Step 3: Delete `src/db/queries/documents.py`, create `src/db/queries/indexed_documents.py`**

```python
# src/db/queries/indexed_documents.py
from datetime import datetime, timezone
from sqlite3 import Row

from db.sqlite_manager import SQLiteManager


def _db(collection: str, db_dir: str | None) -> SQLiteManager:
    return SQLiteManager(collection, db_dir) if db_dir else SQLiteManager(collection)


def get_by_hash(collection: str, hash_id: str, db_dir: str | None = None) -> Row | None:
    db = _db(collection, db_dir)
    with db as conn:
        return conn.execute(
            """
            SELECT hash_id, object_path, chunk_count, indexed_at
            FROM indexed_documents
            WHERE hash_id = ?
            """,
            (hash_id,),
        ).fetchone()


def save_indexed(
    collection: str, hash_id: str, object_path: str, chunk_count: int, db_dir: str | None = None
) -> None:
    db = _db(collection, db_dir)
    with db as conn:
        conn.execute(
            """
            INSERT INTO indexed_documents (hash_id, object_path, chunk_count, indexed_at)
            VALUES (?, ?, ?, ?)
            """,
            (hash_id, object_path, chunk_count, datetime.now(timezone.utc).isoformat()),
        )
```

- [ ] **Step 4: Replace the `ingestion_storage`/`documents` table creation with `indexed_documents` in the migration**

```python
# src/migration/sqlite_create_table_v1.py
from core import config_reader
from db.sqlite_manager import SQLiteManager
from transformers.utils import logging


logger = logging.get_logger(__name__)

class CreateSqLiteTables:
    desc = "Create SqLite Tables"
    def run(self):
        collection = config_reader.get_config().rag.collection
        db_dir = config_reader.get_config().sqlite.dir
        db = SQLiteManager(collection, db_dir)
        self._indexed_documents_table(db)

    def _indexed_documents_table(self, db_manager: SQLiteManager):
        logger.info("Initializing indexed_documents table")
        with db_manager as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS indexed_documents (
                    hash_id TEXT PRIMARY KEY NOT NULL,
                    object_path TEXT NOT NULL,
                    chunk_count INTEGER NOT NULL,
                    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/db/test_indexed_documents.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git rm src/db/queries/documents.py
git add src/db/queries/indexed_documents.py src/migration/sqlite_create_table_v1.py tests/db/test_indexed_documents.py
git commit -m "feat: replace path-keyed documents table with hash-keyed indexed_documents"
```

---

### Task 4: Add `download_to_tempfile` to `minio_storage.py`

**Files:**
- Modify: `src/storage/minio_storage.py`
- Test: `tests/storage/test_minio_storage.py`

**Interfaces:**
- Produces: `minio_storage.download_to_tempfile(object_path: str) -> Path`.
- Consumed by: Task 6's `rag/ingest.py`.

- [ ] **Step 1: Write the failing test**

```python
# tests/storage/test_minio_storage.py
from pathlib import Path
from unittest.mock import patch

from storage import minio_storage


@patch("storage.minio_storage._client")
def test_download_to_tempfile_fetches_object_and_returns_path(mock_client):
    def fake_fget(bucket, object_path, tmp_path):
        Path(tmp_path).write_bytes(b"%PDF-1.4 fake content")

    mock_client.fget_object.side_effect = fake_fget

    result = minio_storage.download_to_tempfile("some/object.pdf")

    assert result.exists()
    assert result.suffix == ".pdf"
    assert result.read_bytes().startswith(b"%PDF")
    result.unlink()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/storage/test_minio_storage.py -v`
Expected: FAIL — `AttributeError: module 'storage.minio_storage' has no attribute 'download_to_tempfile'`

- [ ] **Step 3: Implement**

```python
# src/storage/minio_storage.py
import logging
import os
import tempfile
from pathlib import Path

from minio import Minio
from core import config_reader

_minio_config = config_reader.get_config().minio
_BUCKET=_minio_config.bucket
_URL=_minio_config.url
_ACCESS_KEY=_minio_config.access_key
_SECRET_KEY=_minio_config.secret_key
_logger = logging.getLogger(__name__)

_client = Minio(
    _URL,
    _ACCESS_KEY,
    _SECRET_KEY,
    secure=False
)


def get_client() -> Minio:
    return _client


def initialize_bucket(bucket: str) -> None:
    if _client.bucket_exists(bucket):
        _logger.info("Bucket %s already exists", bucket)
        return
    _logger.info("Creating bucket %s", bucket)
    _client.make_bucket(bucket)


def download_to_tempfile(object_path: str) -> Path:
    suffix = Path(object_path).suffix or ".pdf"
    fd, tmp_path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    _client.fget_object(_BUCKET, object_path, tmp_path)
    return Path(tmp_path)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/storage/test_minio_storage.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/storage/minio_storage.py tests/storage/test_minio_storage.py
git commit -m "feat: add download_to_tempfile for stream-driven ingestion"
```

---

### Task 5: Rewrite `rag/ingest.py` for hash-based, stream-driven ingestion

**Files:**
- Modify: `src/rag/ingest.py`
- Test: `tests/rag/test_ingest.py`

**Interfaces:**
- Consumes: `indexed_documents.get_by_hash`/`save_indexed` (Task 3), `minio_storage.download_to_tempfile` (Task 4), existing `chunking.load_pdf_chunks`, `embeddings.embed`, `qdrant_manager.get_client`.
- Produces: `ingest.ingest_document(hash_id: str, object_path: str) -> int | None` (returns chunk count, or `None` if already indexed). `ingest._chunk_point_id(hash_id: str, chunk_index: int) -> str` (deterministic UUID5 string).
- Consumed by: Task 7's `rag/consumer.py`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/rag/test_ingest.py
from unittest.mock import MagicMock, patch

from rag import ingest


@patch("rag.ingest.qdrant_manager")
@patch("rag.ingest.indexed_documents")
@patch("rag.ingest.minio_storage")
@patch("rag.ingest.chunking")
@patch("rag.ingest.embeddings")
@patch("rag.ingest.config_reader")
def test_skips_already_indexed_document(
    mock_config_reader, mock_embeddings, mock_chunking, mock_minio_storage,
    mock_indexed_documents, mock_qdrant_manager,
):
    mock_config_reader.get_config.return_value.rag.collection = "pdf-fixed"
    mock_indexed_documents.get_by_hash.return_value = {"hash_id": "abc"}

    result = ingest.ingest_document("abc", "some/object/path.pdf")

    assert result is None
    mock_minio_storage.download_to_tempfile.assert_not_called()
    mock_qdrant_manager.get_client.assert_not_called()


@patch("rag.ingest.qdrant_manager")
@patch("rag.ingest.indexed_documents")
@patch("rag.ingest.minio_storage")
@patch("rag.ingest.chunking")
@patch("rag.ingest.embeddings")
@patch("rag.ingest.config_reader")
def test_indexes_new_document_with_deterministic_point_ids(
    mock_config_reader, mock_embeddings, mock_chunking, mock_minio_storage,
    mock_indexed_documents, mock_qdrant_manager,
):
    mock_config_reader.get_config.return_value.rag.collection = "pdf-fixed"
    mock_indexed_documents.get_by_hash.return_value = None

    fake_local_path = MagicMock()
    mock_minio_storage.download_to_tempfile.return_value = fake_local_path

    chunk = MagicMock()
    chunk.page_content = "some text"
    chunk.metadata = {"source": "doc.pdf", "page": 0}
    mock_chunking.load_pdf_chunks.return_value = [chunk]

    mock_embeddings.embed.return_value = [0.1, 0.2]

    result = ingest.ingest_document("abc123", "some/object/path.pdf")

    assert result == 1
    upsert_call = mock_qdrant_manager.get_client.return_value.upsert.call_args
    points = upsert_call.kwargs["points"]
    assert points[0].payload["hash_id"] == "abc123"
    assert points[0].payload["chunk_id"] == 0
    assert points[0].id == ingest._chunk_point_id("abc123", 0)

    mock_indexed_documents.save_indexed.assert_called_once_with(
        "pdf-fixed", "abc123", "some/object/path.pdf", 1
    )
    fake_local_path.unlink.assert_called_once_with(missing_ok=True)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/rag/test_ingest.py -v`
Expected: FAIL — current `ingest_document` signature takes a single `Path` argument, not `(hash_id, object_path)`.

- [ ] **Step 3: Implement**

```python
# src/rag/ingest.py
import uuid

from qdrant_client.models import PointStruct

from core import config_reader, qdrant_manager
from db.queries import indexed_documents
from rag import chunking, embeddings
from storage import minio_storage

POINT_ID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def _chunk_point_id(hash_id: str, chunk_index: int) -> str:
    return str(uuid.uuid5(POINT_ID_NAMESPACE, f"{hash_id}:{chunk_index}"))


def ingest_document(hash_id: str, object_path: str) -> int | None:
    collection = config_reader.get_config().rag.collection

    if indexed_documents.get_by_hash(collection, hash_id) is not None:
        print(f"Skipping already-indexed document: {hash_id}")
        return None

    local_path = minio_storage.download_to_tempfile(object_path)
    try:
        chunks = chunking.load_pdf_chunks(local_path)
        points = [
            PointStruct(
                id=_chunk_point_id(hash_id, chunk_index),
                vector=embeddings.embed(chunk.page_content),
                payload={
                    "text": chunk.page_content,
                    "source": chunk.metadata.get("source"),
                    "page": chunk.metadata.get("page"),
                    "hash_id": hash_id,
                    "chunk_id": chunk_index,
                    "object_path": object_path,
                },
            )
            for chunk_index, chunk in enumerate(chunks)
        ]

        qdrant_manager.get_client().upsert(collection_name=collection, points=points)
        indexed_documents.save_indexed(collection, hash_id, object_path, len(points))
        print(f"Indexed {len(points)} chunks: {hash_id}")
        return len(points)
    finally:
        local_path.unlink(missing_ok=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/rag/test_ingest.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/rag/ingest.py tests/rag/test_ingest.py
git commit -m "feat: rewrite ingest_document around hash_id/object_path, drop path-based dedup"
```

---

### Task 6: Redis consumer worker

**Files:**
- Create: `src/core/redis_manager.py`
- Create: `src/rag/consumer.py`
- Remove: `src/rag/pipeline.py`
- Modify: `Makefile`
- Modify: `pyproject.toml`
- Test: `tests/rag/test_consumer.py`

**Interfaces:**
- Consumes: `config_reader.get_config().redis`, `ingest.ingest_document` (Task 5).
- Produces: `consumer.run() -> None` (blocking loop), `consumer._handle(client, redis_config, message_id, fields) -> None`.

- [ ] **Step 1: Add the `redis` dependency**

```toml
# pyproject.toml — add to [project] dependencies list
    "redis>=5.0.0",
```

Run: `uv sync`

- [ ] **Step 2: Add `src/core/redis_manager.py`**

```python
# src/core/redis_manager.py
import redis

from core import config_reader

_config = config_reader.get_config().redis
_client = redis.Redis(host=_config.host, port=_config.port, decode_responses=True)


def get_client() -> redis.Redis:
    return _client
```

- [ ] **Step 3: Write the failing test for message handling**

```python
# tests/rag/test_consumer.py
from unittest.mock import MagicMock, patch

from rag import consumer


def test_handle_acks_after_successful_ingest():
    client = MagicMock()
    redis_config = MagicMock(stream="ingest.docs", consumer_group="rag-pdf-fixed")

    with patch("rag.consumer.ingest") as mock_ingest:
        mock_ingest.ingest_document.return_value = 3
        consumer._handle(client, redis_config, "1-0", {"hash_id": "abc", "object_path": "objects/abc.pdf"})

    mock_ingest.ingest_document.assert_called_once_with("abc", "objects/abc.pdf")
    client.xack.assert_called_once_with("ingest.docs", "rag-pdf-fixed", "1-0")


def test_handle_does_not_ack_on_ingest_failure():
    client = MagicMock()
    redis_config = MagicMock(stream="ingest.docs", consumer_group="rag-pdf-fixed")

    with patch("rag.consumer.ingest") as mock_ingest:
        mock_ingest.ingest_document.side_effect = RuntimeError("boom")
        consumer._handle(client, redis_config, "1-0", {"hash_id": "abc", "object_path": "objects/abc.pdf"})

    client.xack.assert_not_called()
```

- [ ] **Step 4: Run test to verify it fails**

Run: `uv run pytest tests/rag/test_consumer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'rag.consumer'`

- [ ] **Step 5: Implement `src/rag/consumer.py`**

```python
# src/rag/consumer.py
import logging

from core import config_reader
from core.redis_manager import get_client
from rag import ingest

logger = logging.getLogger(__name__)

CONSUMER_NAME = "rag-worker-1"


def _ensure_group(client, stream: str, group: str) -> None:
    try:
        client.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _handle(client, redis_config, message_id, fields: dict) -> None:
    hash_id = fields.get("hash_id")
    object_path = fields.get("object_path")
    try:
        ingest.ingest_document(hash_id, object_path)
    except Exception:
        logger.exception("Failed to ingest %s (%s), will retry on redelivery", hash_id, object_path)
        return
    client.xack(redis_config.stream, redis_config.consumer_group, message_id)


def run() -> None:
    redis_config = config_reader.get_config().redis
    client = get_client()
    _ensure_group(client, redis_config.stream, redis_config.consumer_group)

    logger.info("Consumer started: stream=%s group=%s", redis_config.stream, redis_config.consumer_group)
    while True:
        entries = client.xreadgroup(
            redis_config.consumer_group,
            CONSUMER_NAME,
            {redis_config.stream: ">"},
            count=1,
            block=5000,
        )
        if not entries:
            continue
        for _stream_name, messages in entries:
            for message_id, fields in messages:
                _handle(client, redis_config, message_id, fields)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    run()
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/rag/test_consumer.py -v`
Expected: PASS

- [ ] **Step 7: Remove obsolete `pipeline.py`, update `Makefile`**

```bash
git rm src/rag/pipeline.py
```

```makefile
# Makefile
.PHONY: generate run_migrations run_worker up down

# gRPC
generate:
	uv run python -m grpc_tools.protoc \
		-I./protos \
		--python_out=./src/generated \
		--pyi_out=./src/generated \
		--grpc_python_out=./src/generated \
		./protos/rag/v1/rag.proto

run_migrations: up
	uv run src/migration/main.py
	docker compose down

run_worker:
	uv run python -m rag.consumer

up:
	docker compose up -d

down:
	docker compose down
```

- [ ] **Step 8: Commit**

```bash
git add src/core/redis_manager.py src/rag/consumer.py tests/rag/test_consumer.py Makefile pyproject.toml uv.lock
git commit -m "feat: add redis stream consumer worker, drop manual pipeline driver"
```

---

## Part B — Go central service

All Go code lives under `services/go-ingest/`. Module name: `ragingest`.

### Task 7: Go module skeleton + config loader

**Files:**
- Create: `services/go-ingest/go.mod`
- Create: `services/go-ingest/internal/config/config.go`
- Test: `services/go-ingest/internal/config/config_test.go`

**Interfaces:**
- Produces: `config.Load(path string) (*Config, error)`, `Config{Minio MinioConfig, Redis RedisConfig, Go GoConfig}`.

- [ ] **Step 1: Initialize the module**

Run:
```bash
mkdir -p services/go-ingest
cd services/go-ingest
go mod init ragingest
go get github.com/BurntSushi/toml
```

- [ ] **Step 2: Write the failing test**

```go
// services/go-ingest/internal/config/config_test.go
package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadParsesMinioRedisAndGoSections(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	contents := `
[minio]
url="localhost:9000"
access_key="minio_user"
secret_key="minio_password"
bucket="rag-pdf-source"

[redis]
host="localhost"
port=6379
stream="ingest.docs"
consumer_group="rag-pdf-fixed"

[go]
port=8080
sqlite_dir="./go_sqlite_db"
`
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.Minio.Bucket != "rag-pdf-source" {
		t.Errorf("Minio.Bucket = %q, want rag-pdf-source", cfg.Minio.Bucket)
	}
	if cfg.Redis.Port != 6379 {
		t.Errorf("Redis.Port = %d, want 6379", cfg.Redis.Port)
	}
	if cfg.Go.Port != 8080 {
		t.Errorf("Go.Port = %d, want 8080", cfg.Go.Port)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd services/go-ingest && go test ./internal/config/...`
Expected: FAIL — `config.go` doesn't exist yet.

- [ ] **Step 4: Implement**

```go
// services/go-ingest/internal/config/config.go
package config

import "github.com/BurntSushi/toml"

type MinioConfig struct {
	URL       string `toml:"url"`
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Bucket    string `toml:"bucket"`
}

type RedisConfig struct {
	Host          string `toml:"host"`
	Port          int    `toml:"port"`
	Stream        string `toml:"stream"`
	ConsumerGroup string `toml:"consumer_group"`
}

type GoConfig struct {
	Port      int    `toml:"port"`
	SqliteDir string `toml:"sqlite_dir"`
}

type Config struct {
	Minio MinioConfig `toml:"minio"`
	Redis RedisConfig `toml:"redis"`
	Go    GoConfig    `toml:"go"`
}

func Load(path string) (*Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd services/go-ingest && go test ./internal/config/...`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add services/go-ingest/go.mod services/go-ingest/go.sum services/go-ingest/internal/config
git commit -m "feat(go-ingest): module skeleton with toml config loader"
```

---

### Task 8: SQLite document registry

**Files:**
- Create: `services/go-ingest/internal/db/db.go`
- Create: `services/go-ingest/internal/db/documents.go`
- Test: `services/go-ingest/internal/db/documents_test.go`

**Interfaces:**
- Produces: `db.Open(path string) (*sql.DB, error)`, `db.Document{HashID, ObjectPath, Status string; PublishedAt sql.NullTime; ConfirmedAt time.Time}`, `db.GetByHash(conn *sql.DB, hashID string) (*Document, error)`, `db.Insert(conn *sql.DB, hashID, objectPath string) error`, `db.MarkPublished(conn *sql.DB, hashID string) error`, `db.UnpublishedHashes(conn *sql.DB) ([]Document, error)`.
- Consumed by: Task 11 (handlers), Task 12 (outbox).

- [ ] **Step 1: Add the sqlite driver dependency**

Run: `cd services/go-ingest && go get modernc.org/sqlite`

- [ ] **Step 2: Write the failing test**

```go
// services/go-ingest/internal/db/documents_test.go
package db

import "testing"

func TestInsertGetAndOutboxSweep(t *testing.T) {
	conn, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	if _, err := GetByHash(conn, "missing"); err != nil {
		t.Fatalf("GetByHash on empty table: %v", err)
	}
	if got, _ := GetByHash(conn, "missing"); got != nil {
		t.Fatalf("GetByHash on empty table = %+v, want nil", got)
	}

	if err := Insert(conn, "abc123", "objects/abc.pdf"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	doc, err := GetByHash(conn, "abc123")
	if err != nil {
		t.Fatalf("GetByHash: %v", err)
	}
	if doc == nil || doc.ObjectPath != "objects/abc.pdf" {
		t.Fatalf("GetByHash = %+v, want object_path=objects/abc.pdf", doc)
	}
	if doc.PublishedAt.Valid {
		t.Fatalf("PublishedAt should start unset, got %v", doc.PublishedAt)
	}

	unpublished, err := UnpublishedHashes(conn)
	if err != nil {
		t.Fatalf("UnpublishedHashes: %v", err)
	}
	if len(unpublished) != 1 || unpublished[0].HashID != "abc123" {
		t.Fatalf("UnpublishedHashes = %+v, want one row for abc123", unpublished)
	}

	if err := MarkPublished(conn, "abc123"); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}

	unpublished, err = UnpublishedHashes(conn)
	if err != nil {
		t.Fatalf("UnpublishedHashes after publish: %v", err)
	}
	if len(unpublished) != 0 {
		t.Fatalf("UnpublishedHashes after publish = %+v, want none", unpublished)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd services/go-ingest && go test ./internal/db/...`
Expected: FAIL — package doesn't compile, no `Open`/`Insert`/etc.

- [ ] **Step 4: Implement**

```go
// services/go-ingest/internal/db/db.go
package db

import (
	"database/sql"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

func Open(path string) (*sql.DB, error) {
	if path != ":memory:" {
		if dir := filepath.Dir(path); dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
		}
	}

	conn, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(`
		CREATE TABLE IF NOT EXISTS documents (
			hash_id      TEXT PRIMARY KEY,
			object_path  TEXT NOT NULL,
			status       TEXT NOT NULL,
			published_at TIMESTAMP,
			confirmed_at TIMESTAMP NOT NULL
		)
	`); err != nil {
		return nil, err
	}
	return conn, nil
}
```

```go
// services/go-ingest/internal/db/documents.go
package db

import (
	"database/sql"
	"time"
)

type Document struct {
	HashID      string
	ObjectPath  string
	Status      string
	PublishedAt sql.NullTime
	ConfirmedAt time.Time
}

func GetByHash(conn *sql.DB, hashID string) (*Document, error) {
	row := conn.QueryRow(
		`SELECT hash_id, object_path, status, published_at, confirmed_at FROM documents WHERE hash_id = ?`,
		hashID,
	)
	var d Document
	if err := row.Scan(&d.HashID, &d.ObjectPath, &d.Status, &d.PublishedAt, &d.ConfirmedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &d, nil
}

func Insert(conn *sql.DB, hashID, objectPath string) error {
	_, err := conn.Exec(
		`INSERT INTO documents (hash_id, object_path, status, confirmed_at) VALUES (?, ?, 'uploaded', ?)`,
		hashID, objectPath, time.Now().UTC(),
	)
	return err
}

func MarkPublished(conn *sql.DB, hashID string) error {
	_, err := conn.Exec(`UPDATE documents SET published_at = ? WHERE hash_id = ?`, time.Now().UTC(), hashID)
	return err
}

func UnpublishedHashes(conn *sql.DB) ([]Document, error) {
	rows, err := conn.Query(
		`SELECT hash_id, object_path, status, published_at, confirmed_at FROM documents WHERE published_at IS NULL`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var docs []Document
	for rows.Next() {
		var d Document
		if err := rows.Scan(&d.HashID, &d.ObjectPath, &d.Status, &d.PublishedAt, &d.ConfirmedAt); err != nil {
			return nil, err
		}
		docs = append(docs, d)
	}
	return docs, rows.Err()
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd services/go-ingest && go test ./internal/db/...`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add services/go-ingest/internal/db services/go-ingest/go.mod services/go-ingest/go.sum
git commit -m "feat(go-ingest): sqlite document registry with outbox query"
```

---

### Task 9: MinIO object store (presigned POST policy + server-side hash)

**Files:**
- Create: `services/go-ingest/internal/storage/storage.go`

**Interfaces:**
- Produces: `storage.ObjectStore` interface (`PresignedUploadPolicy(ctx, objectKey, expiry) (url string, fields map[string]string, err error)`, `HashObject(ctx, objectKey) (string, error)`), `storage.NewMinioStore(endpoint, accessKey, secretKey, bucket string) (*MinioStore, error)`.
- Consumed by: Task 11 (handlers) via the interface. `MinioStore` itself is exercised only by the manual end-to-end smoke test in Task 13 (real network calls to MinIO aren't unit-tested — noted, not a gap: the `ObjectStore` interface is what's unit-tested via handler tests with a fake).

- [ ] **Step 1: Add the minio dependency**

Run: `cd services/go-ingest && go get github.com/minio/minio-go/v7`

- [ ] **Step 2: Implement**

```go
// services/go-ingest/internal/storage/storage.go
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type ObjectStore interface {
	PresignedUploadPolicy(ctx context.Context, objectKey string, expiry time.Duration) (url string, fields map[string]string, err error)
	HashObject(ctx context.Context, objectKey string) (string, error)
}

type MinioStore struct {
	client *minio.Client
	bucket string
}

func NewMinioStore(endpoint, accessKey, secretKey, bucket string) (*MinioStore, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}
	return &MinioStore{client: client, bucket: bucket}, nil
}

func (s *MinioStore) PresignedUploadPolicy(ctx context.Context, objectKey string, expiry time.Duration) (string, map[string]string, error) {
	policy := minio.NewPostPolicy()
	if err := policy.SetBucket(s.bucket); err != nil {
		return "", nil, err
	}
	if err := policy.SetKey(objectKey); err != nil {
		return "", nil, err
	}
	if err := policy.SetExpires(time.Now().UTC().Add(expiry)); err != nil {
		return "", nil, err
	}

	u, formData, err := s.client.PresignedPostPolicy(ctx, policy)
	if err != nil {
		return "", nil, err
	}
	return u.String(), formData, nil
}

func (s *MinioStore) HashObject(ctx context.Context, objectKey string) (string, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return "", err
	}
	defer obj.Close()

	h := sha256.New()
	if _, err := io.Copy(h, obj); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cd services/go-ingest && go build ./...`
Expected: succeeds with no errors.

- [ ] **Step 4: Commit**

```bash
git add services/go-ingest/internal/storage services/go-ingest/go.mod services/go-ingest/go.sum
git commit -m "feat(go-ingest): minio object store (presigned POST policy, server-side hash)"
```

---

### Task 10: Redis stream publisher

**Files:**
- Create: `services/go-ingest/internal/stream/stream.go`
- Test: `services/go-ingest/internal/stream/stream_test.go`

**Interfaces:**
- Produces: `stream.Publisher` interface (`Publish(ctx, hashID, objectPath, contentType string) error`), `stream.NewRedisPublisher(addr, streamName string) *RedisPublisher`.
- Consumed by: Task 11 (handlers), Task 12 (outbox).

- [ ] **Step 1: Add dependencies**

Run:
```bash
cd services/go-ingest
go get github.com/redis/go-redis/v9
go get github.com/alicebob/miniredis/v2
```

- [ ] **Step 2: Write the failing test**

```go
// services/go-ingest/internal/stream/stream_test.go
package stream

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestPublishAddsEntryToStream(t *testing.T) {
	mr := miniredis.RunT(t)
	publisher := NewRedisPublisher(mr.Addr(), "ingest.docs")

	if err := publisher.Publish(context.Background(), "abc123", "objects/abc.pdf", "application/pdf"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	entries, err := client.XRange(context.Background(), "ingest.docs", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d stream entries, want 1", len(entries))
	}
	if entries[0].Values["hash_id"] != "abc123" {
		t.Errorf("hash_id = %v, want abc123", entries[0].Values["hash_id"])
	}
	if entries[0].Values["object_path"] != "objects/abc.pdf" {
		t.Errorf("object_path = %v, want objects/abc.pdf", entries[0].Values["object_path"])
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd services/go-ingest && go test ./internal/stream/...`
Expected: FAIL — package doesn't compile, no `NewRedisPublisher`.

- [ ] **Step 4: Implement**

```go
// services/go-ingest/internal/stream/stream.go
package stream

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Publisher interface {
	Publish(ctx context.Context, hashID, objectPath, contentType string) error
}

type RedisPublisher struct {
	client *redis.Client
	stream string
}

func NewRedisPublisher(addr, streamName string) *RedisPublisher {
	return &RedisPublisher{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		stream: streamName,
	}
}

func (p *RedisPublisher) Publish(ctx context.Context, hashID, objectPath, contentType string) error {
	return p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.stream,
		Values: map[string]interface{}{
			"hash_id":      hashID,
			"object_path":  objectPath,
			"content_type": contentType,
		},
	}).Err()
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd services/go-ingest && go test ./internal/stream/...`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add services/go-ingest/internal/stream services/go-ingest/go.mod services/go-ingest/go.sum
git commit -m "feat(go-ingest): redis stream publisher"
```

---

### Task 11: HTTP handlers (`/ingest/init`, `/ingest/confirm`)

**Files:**
- Create: `services/go-ingest/internal/handlers/ingest.go`
- Test: `services/go-ingest/internal/handlers/ingest_test.go`

**Interfaces:**
- Consumes: `db.GetByHash/Insert/MarkPublished` (Task 8), `storage.ObjectStore` (Task 9), `stream.Publisher` (Task 10).
- Produces: `handlers.IngestHandler{DB *sql.DB, Storage storage.ObjectStore, Publisher stream.Publisher}`, `Init(w, r)`, `Confirm(w, r)`.
- Consumed by: Task 12 (`main.go`).

- [ ] **Step 1: Add the uuid dependency**

Run: `cd services/go-ingest && go get github.com/google/uuid`

- [ ] **Step 2: Write the failing tests**

```go
// services/go-ingest/internal/handlers/ingest_test.go
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ragingest/internal/db"
)

var errObjectNotFound = errors.New("object not found")

type fakeStore struct {
	hash string
	err  error
}

func (f *fakeStore) PresignedUploadPolicy(ctx context.Context, objectKey string, expiry time.Duration) (string, map[string]string, error) {
	return "http://minio.local/upload", map[string]string{"key": objectKey}, nil
}

func (f *fakeStore) HashObject(ctx context.Context, objectKey string) (string, error) {
	return f.hash, f.err
}

type fakePublisher struct {
	calls int
	err   error
}

func (f *fakePublisher) Publish(ctx context.Context, hashID, objectPath, contentType string) error {
	f.calls++
	return f.err
}

func newTestHandler(t *testing.T, store *fakeStore, pub *fakePublisher) *IngestHandler {
	t.Helper()
	conn, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &IngestHandler{DB: conn, Storage: store, Publisher: pub}
}

func TestInitReturnsUploadURLAndObjectPath(t *testing.T) {
	h := newTestHandler(t, &fakeStore{}, &fakePublisher{})

	req := httptest.NewRequest(http.MethodPost, "/ingest/init", bytes.NewBufferString(`{"filename":"doc.pdf"}`))
	rec := httptest.NewRecorder()

	h.Init(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp initResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.UploadURL == "" || resp.ObjectPath == "" {
		t.Fatalf("resp = %+v, want non-empty upload_url and object_path", resp)
	}
}

func TestConfirmInsertsAndPublishesNewDocument(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(t, &fakeStore{hash: "abc123"}, pub)

	req := httptest.NewRequest(http.MethodPost, "/ingest/confirm", bytes.NewBufferString(`{"object_path":"objects/abc.pdf"}`))
	rec := httptest.NewRecorder()

	h.Confirm(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	if pub.calls != 1 {
		t.Fatalf("publisher called %d times, want 1", pub.calls)
	}

	doc, err := db.GetByHash(h.DB, "abc123")
	if err != nil || doc == nil {
		t.Fatalf("GetByHash: doc=%+v err=%v", doc, err)
	}
	if !doc.PublishedAt.Valid {
		t.Fatalf("PublishedAt should be set after successful publish, got %+v", doc.PublishedAt)
	}
}

func TestConfirmReturns404WhenObjectMissing(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(t, &fakeStore{err: errObjectNotFound}, pub)

	req := httptest.NewRequest(http.MethodPost, "/ingest/confirm", bytes.NewBufferString(`{"object_path":"objects/missing.pdf"}`))
	rec := httptest.NewRecorder()

	h.Confirm(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if pub.calls != 0 {
		t.Fatalf("publisher should not be called when object is missing, got %d calls", pub.calls)
	}
}

func TestConfirmRejectsDuplicateHash(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(t, &fakeStore{hash: "abc123"}, pub)
	if err := db.Insert(h.DB, "abc123", "objects/first.pdf"); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/ingest/confirm", bytes.NewBufferString(`{"object_path":"objects/dup.pdf"}`))
	rec := httptest.NewRecorder()

	h.Confirm(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	if pub.calls != 0 {
		t.Fatalf("publisher should not be called for a duplicate, got %d calls", pub.calls)
	}
}

func TestConfirmSurvivesPublishFailureViaOutbox(t *testing.T) {
	pub := &fakePublisher{err: context.DeadlineExceeded}
	h := newTestHandler(t, &fakeStore{hash: "abc123"}, pub)

	req := httptest.NewRequest(http.MethodPost, "/ingest/confirm", bytes.NewBufferString(`{"object_path":"objects/abc.pdf"}`))
	rec := httptest.NewRecorder()

	h.Confirm(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 even when publish fails (outbox picks it up)", rec.Code)
	}
	doc, err := db.GetByHash(h.DB, "abc123")
	if err != nil || doc == nil {
		t.Fatalf("GetByHash: doc=%+v err=%v", doc, err)
	}
	if doc.PublishedAt.Valid {
		t.Fatalf("PublishedAt should stay unset when publish failed, got %+v", doc.PublishedAt)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd services/go-ingest && go test ./internal/handlers/...`
Expected: FAIL — package doesn't compile, no `IngestHandler`.

- [ ] **Step 4: Implement**

```go
// services/go-ingest/internal/handlers/ingest.go
package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"ragingest/internal/db"
	"ragingest/internal/storage"
	"ragingest/internal/stream"
)

type IngestHandler struct {
	DB        *sql.DB
	Storage   storage.ObjectStore
	Publisher stream.Publisher
}

type initRequest struct {
	Filename string `json:"filename"`
}

type initResponse struct {
	UploadURL  string            `json:"upload_url"`
	Fields     map[string]string `json:"fields"`
	ObjectPath string            `json:"object_path"`
}

func (h *IngestHandler) Init(w http.ResponseWriter, r *http.Request) {
	var req initRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	objectKey := uuid.NewString()
	if req.Filename != "" {
		objectKey = objectKey + "-" + req.Filename
	}

	url, fields, err := h.Storage.PresignedUploadPolicy(r.Context(), objectKey, 15*time.Minute)
	if err != nil {
		http.Error(w, "failed to create upload url", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, initResponse{UploadURL: url, Fields: fields, ObjectPath: objectKey})
}

type confirmRequest struct {
	ObjectPath string `json:"object_path"`
}

type confirmResponse struct {
	HashID string `json:"hash_id"`
	Status string `json:"status"`
}

func (h *IngestHandler) Confirm(w http.ResponseWriter, r *http.Request) {
	var req confirmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ObjectPath == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	hashID, err := h.Storage.HashObject(r.Context(), req.ObjectPath)
	if err != nil {
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}

	existing, err := db.GetByHash(h.DB, hashID)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if existing != nil {
		writeJSON(w, http.StatusConflict, confirmResponse{HashID: hashID, Status: "duplicate"})
		return
	}

	if err := db.Insert(h.DB, hashID, req.ObjectPath); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Best-effort inline publish. If this fails, the row stays unpublished
	// and the outbox poller (Task 12) retries it — the client still gets a
	// success response because the DB row is the durable proof of confirm.
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := h.Publisher.Publish(ctx, hashID, req.ObjectPath, "application/pdf"); err == nil {
		_ = db.MarkPublished(h.DB, hashID)
	}

	writeJSON(w, http.StatusOK, confirmResponse{HashID: hashID, Status: "uploaded"})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd services/go-ingest && go test ./internal/handlers/...`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add services/go-ingest/internal/handlers services/go-ingest/go.mod services/go-ingest/go.sum
git commit -m "feat(go-ingest): init/confirm handlers with outbox-safe publish"
```

---

### Task 12: Outbox poller + `main.go` wiring

**Files:**
- Create: `services/go-ingest/internal/outbox/poller.go`
- Test: `services/go-ingest/internal/outbox/poller_test.go`
- Create: `services/go-ingest/main.go`

**Interfaces:**
- Consumes: `db.UnpublishedHashes/MarkPublished` (Task 8), `stream.Publisher` (Task 10), `config.Load` (Task 7), `handlers.IngestHandler` (Task 11).
- Produces: `outbox.Start(ctx, conn, publisher, interval)` (spawns a background goroutine), binary entrypoint `services/go-ingest/main.go`.

- [ ] **Step 1: Write the failing test**

```go
// services/go-ingest/internal/outbox/poller_test.go
package outbox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"ragingest/internal/db"
)

type countingPublisher struct {
	calls int32
	err   error
}

func (p *countingPublisher) Publish(ctx context.Context, hashID, objectPath, contentType string) error {
	atomic.AddInt32(&p.calls, 1)
	return p.err
}

func TestSweepPublishesUnpublishedRowsAndMarksThem(t *testing.T) {
	conn, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer conn.Close()
	if err := db.Insert(conn, "abc123", "objects/abc.pdf"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	pub := &countingPublisher{}
	sweep(context.Background(), conn, pub)

	if atomic.LoadInt32(&pub.calls) != 1 {
		t.Fatalf("publish calls = %d, want 1", pub.calls)
	}
	doc, err := db.GetByHash(conn, "abc123")
	if err != nil || doc == nil || !doc.PublishedAt.Valid {
		t.Fatalf("expected doc to be marked published, got %+v (err=%v)", doc, err)
	}
}

func TestSweepLeavesRowUnpublishedOnPublishError(t *testing.T) {
	conn, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer conn.Close()
	if err := db.Insert(conn, "abc123", "objects/abc.pdf"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	pub := &countingPublisher{err: context.DeadlineExceeded}
	sweep(context.Background(), conn, pub)

	doc, err := db.GetByHash(conn, "abc123")
	if err != nil || doc == nil {
		t.Fatalf("GetByHash: doc=%+v err=%v", doc, err)
	}
	if doc.PublishedAt.Valid {
		t.Fatalf("expected doc to stay unpublished after publish error, got %+v", doc.PublishedAt)
	}
}

func TestStartRunsSweepOnEachTick(t *testing.T) {
	conn, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer conn.Close()
	if err := db.Insert(conn, "abc123", "objects/abc.pdf"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	pub := &countingPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	Start(ctx, conn, pub, 10*time.Millisecond)
	defer cancel()

	deadline := time.After(500 * time.Millisecond)
	for {
		if atomic.LoadInt32(&pub.calls) >= 1 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("Start did not publish the pending row within 500ms")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/go-ingest && go test ./internal/outbox/...`
Expected: FAIL — package doesn't exist yet.

- [ ] **Step 3: Implement**

```go
// services/go-ingest/internal/outbox/poller.go
package outbox

import (
	"context"
	"database/sql"
	"log"
	"time"

	"ragingest/internal/db"
	"ragingest/internal/stream"
)

func Start(ctx context.Context, conn *sql.DB, publisher stream.Publisher, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweep(ctx, conn, publisher)
			}
		}
	}()
}

func sweep(ctx context.Context, conn *sql.DB, publisher stream.Publisher) {
	docs, err := db.UnpublishedHashes(conn)
	if err != nil {
		log.Printf("outbox: sweep query failed: %v", err)
		return
	}
	for _, d := range docs {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := publisher.Publish(pctx, d.HashID, d.ObjectPath, "application/pdf")
		cancel()
		if err != nil {
			log.Printf("outbox: publish retry failed for %s: %v", d.HashID, err)
			continue
		}
		if err := db.MarkPublished(conn, d.HashID); err != nil {
			log.Printf("outbox: mark published failed for %s: %v", d.HashID, err)
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd services/go-ingest && go test ./internal/outbox/...`
Expected: PASS

- [ ] **Step 5: Write `main.go`**

```go
// services/go-ingest/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"ragingest/internal/config"
	"ragingest/internal/db"
	"ragingest/internal/handlers"
	"ragingest/internal/outbox"
	"ragingest/internal/storage"
	"ragingest/internal/stream"
)

func main() {
	cfg, err := config.Load("../../config.toml")
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}

	conn, err := db.Open(cfg.Go.SqliteDir + "/documents.db")
	if err != nil {
		log.Fatalf("db open failed: %v", err)
	}
	defer conn.Close()

	minioStore, err := storage.NewMinioStore(cfg.Minio.URL, cfg.Minio.AccessKey, cfg.Minio.SecretKey, cfg.Minio.Bucket)
	if err != nil {
		log.Fatalf("minio client failed: %v", err)
	}

	publisher := stream.NewRedisPublisher(fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port), cfg.Redis.Stream)

	h := &handlers.IngestHandler{DB: conn, Storage: minioStore, Publisher: publisher}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest/init", h.Init)
	mux.HandleFunc("POST /ingest/confirm", h.Confirm)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	outbox.Start(ctx, conn, publisher, 5*time.Second)

	srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Go.Port), Handler: mux}
	go func() {
		log.Printf("go-ingest listening on :%d", cfg.Go.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
```

- [ ] **Step 6: Verify the whole module builds**

Run: `cd services/go-ingest && go build ./...`
Expected: succeeds with no errors.

- [ ] **Step 7: Commit**

```bash
git add services/go-ingest/internal/outbox services/go-ingest/main.go services/go-ingest/go.mod services/go-ingest/go.sum
git commit -m "feat(go-ingest): outbox poller and main entrypoint"
```

---

### Task 13: End-to-end manual smoke test

**Files:** none (verification only).

- [ ] **Step 1: Bring up infra and run migrations**

```bash
make up
uv run src/migration/main.py
```

- [ ] **Step 2: Start both services** (separate terminals)

```bash
cd services/go-ingest && go run .
```
```bash
make run_worker
```

- [ ] **Step 3: Drive the flow with curl**

```bash
curl -s -X POST localhost:8080/ingest/init -d '{"filename":"extreme_uz.pdf"}' | tee /tmp/init.json

# Using upload_url + fields from /tmp/init.json, POST the file as multipart form data
# (fields become form fields, file goes in the "file" field, per MinIO POST policy)
curl -s -X POST "$(jq -r .upload_url /tmp/init.json)" \
  $(jq -r '.fields | to_entries[] | "-F \(.key)=\(.value)"' /tmp/init.json) \
  -F "file=@ingestion_docs/extreme_uz.pdf"

curl -s -X POST localhost:8080/ingest/confirm \
  -d "{\"object_path\":\"$(jq -r .object_path /tmp/init.json)\"}"
```

- [ ] **Step 4: Confirm indexing happened**

Watch the `make run_worker` terminal for `Indexed N chunks: <hash>`. Then verify directly against Qdrant:

```bash
curl -s localhost:6333/collections/pdf-fixed | jq .result.points_count
```
Expected: count `> 0` after the confirm call above (was `0`/collection-empty beforehand).

- [ ] **Step 5: Confirm dedup works**

Repeat the `/ingest/confirm` call from Step 3 with the same `object_path` (same file → same hash).
Expected: `409` response with `{"status":"duplicate"}`, and `points_count` unchanged.

No commit for this task — it's a verification pass, not a code change.
