# RAG Ingestion Pipeline — Design Spec

Date: 2026-09-01
Status: Approved for implementation planning
Branch: `pdf`

## 1. Purpose

Transform the current single-process, manually-driven PDF ingestion prototype into
a POC-grade, event-driven ingestion pipeline with two services:

- A **Go central service** that clients talk to directly (presigned upload +
  confirm), owns document identity/dedup, and publishes ingestion events.
- A **Python RAG service** that consumes those events, indexes documents into
  Qdrant, and tracks what it has already indexed.

This spec covers **ingestion only**. Query (`Ask`/`AskStream` in
`protos/rag/v1/rag.proto`) is explicitly out of scope and left as-is (currently
unimplemented/broken — not touched by this work).

The design is meant to support, later, multiple RAG services with different
chunking/embedding strategies each owning their own Qdrant collection — this pass
implements exactly one strategy/collection, but avoids decisions that would be
expensive to unwind when a second one is added (see §7).

## 2. Current State (as of branch `pdf`, commit `3beac1d`)

- Python 3.13 project (`uv`), no Go code, no Redis anywhere.
- `src/rag/` has working chunk→embed→upsert logic (`chunking.py`, `embeddings.py`,
  `ingest.py`), driven only by a manual script (`pipeline.py`).
- Qdrant runs **embedded/local-mode** (`QdrantClient(path=...)`) — file-locked,
  single-process only.
- SQLite (`src/db/`) and MinIO (`src/storage/minio_storage.py`) clients exist but
  are not wired into a real ingestion flow. `ingestion_storage` table exists,
  unused.
- **Known regressions to fix as part of this work** (not new scope, but blocking):
  - `src/storage/minio_storage.py` is missing `initialize_bucket()`, referenced by
    `src/migration/minio_bucket_v1.py:14` — `make run_migrations` currently raises
    `AttributeError`.
  - `src/api/server.py:19` looks up gRPC service name `'RagEngine'`, proto defines
    `RagEngineService` — will `KeyError` at startup. Left broken (query is out of
    scope) but noted so it isn't mistaken for new-work fallout.
- `docker-compose.yml` runs MinIO only.

## 3. Architecture

```
Client
  │  1. POST /ingest/init {filename}
  ▼
Go service ── generates temp object key (UUID), presigned MinIO POST URL
  │  2. returns {upload_url, fields, object_path}
  ▼
Client ── uploads file bytes directly to MinIO (presigned POST)
  │  3. POST /ingest/confirm {object_path}
  ▼
Go service
  │  4. GetObject(object_path) from MinIO, stream sha256 → hash_id
  │  5. dedup check against SQLite `documents` table (by hash_id)
  │     - duplicate → reject, no enqueue (uploaded object is orphaned, POC-accepted)
  │     - new → INSERT documents(hash_id, object_path, status='uploaded',
  │             published_at=NULL)
  │  6. XADD ingest.docs {hash_id, object_path, content_type}
  │  7. UPDATE documents SET published_at=now() on XADD success
  ▼
Redis Stream `ingest.docs` (consumer group `rag-pdf-fixed`)
  ▼
Go outbox poller (background, ticker ~5s)
  — sweeps documents WHERE published_at IS NULL, retries XADD
  ▼
Python RAG service (long-running worker, XREADGROUP consumer)
  │  8. for each message: check own SQLite `indexed_documents` by hash_id
  │     - already indexed → XACK, skip
  │  9. GetObject(object_path) from MinIO
  │  10. load_pdf_chunks() → embed() per chunk (existing code, reused as-is)
  │  11. Qdrant upsert, point id = uuid5(f"{hash_id}:{chunk_id}") (deterministic)
  │      payload: {hash_id, chunk_id, object_path, source_filename}
  │  12. INSERT indexed_documents(hash_id, object_path, chunk_count, indexed_at)
  │  13. XACK
  ▼
Qdrant (networked, docker container, collection `pdf-fixed`, HNSW config unchanged)
```

Client never talks to Redis, Qdrant, or the RAG service directly — only the two
Go HTTP endpoints and MinIO (for the upload itself).

## 4. Components

### 4.1 Go central service (new)

- Plain `net/http`, no framework — POC scope doesn't justify one.
- Endpoints:
  - `POST /ingest/init {filename}` → generates a temp UUID object key (not
    content-addressed — hash isn't known yet), returns MinIO presigned POST URL +
    fields + the object key as `object_path`. **No DB row written here.**
  - `POST /ingest/confirm {object_path}` → streams the object from MinIO,
    computes sha256 server-side, performs dedup check, writes the DB row, attempts
    XADD, returns success/duplicate/error to client.
- Own SQLite DB, table `documents`:
  ```sql
  CREATE TABLE documents (
    hash_id      TEXT PRIMARY KEY,
    object_path  TEXT NOT NULL,
    status       TEXT NOT NULL,       -- 'uploaded'
    published_at TIMESTAMP,           -- NULL until XADD confirmed (outbox)
    confirmed_at TIMESTAMP NOT NULL
  );
  ```
- Outbox poller: background goroutine, ticks every ~5s, sweeps rows where
  `published_at IS NULL`, retries `XADD`, sets `published_at` on success. Decouples
  the confirm endpoint's response from Redis availability — a DB row is durable
  proof of "confirmed", publishing catches up asynchronously.

### 4.2 Redis Stream

- Single stream `ingest.docs` for this POC (one strategy/collection → one
  consumer group). Consumer group name `rag-pdf-fixed`, mirroring the Qdrant
  collection name.
- Message fields: `hash_id`, `object_path`, `content_type`.
- No dead-letter queue / max-retry in this pass — unacked messages stay in the
  consumer group's pending-entries list and get reprocessed on RAG service
  restart (via `XPENDING`/`XCLAIM` or a fresh `XREADGROUP`). Acceptable at POC
  scale; flagged as a future improvement (§7).

### 4.3 Python RAG service (reworked)

- New long-running worker entrypoint (replaces `pipeline.py`'s manual-run role;
  `pipeline.py` becomes obsolete / removed) — replace it with an `XREADGROUP` loop
  driving the existing `chunking.py` / `embeddings.py` / `ingest.py` logic.
- Own SQLite table `indexed_documents`:
  ```sql
  CREATE TABLE indexed_documents (
    hash_id      TEXT PRIMARY KEY,
    object_path  TEXT NOT NULL,
    chunk_count  INTEGER NOT NULL,
    indexed_at   TIMESTAMP NOT NULL
  );
  ```
  (Supersedes the currently-unwired `ingestion_storage` table — same intent,
  actually wired up this time.)
- Point ID stays a deterministic UUIDv5 derived from `f"{hash_id}:{chunk_id}"`
  (Qdrant point IDs must be an unsigned integer or UUID — a raw
  `"{hash_id}:{chunk_id}"` string is rejected at upsert). `hash_id` and
  `chunk_id` are carried as explicit payload fields instead, which is what
  satisfies "chunk addressable by hash_id+chunk_id" in practice — payload is
  what gets filtered/read, not the opaque point id.
- Qdrant client switches from embedded (`path=...`) to networked
  (`url=...`) — collection creation logic in `qdrant_collection_v1.py` (HNSW
  params: `m=4, ef_construct=100, full_scan_threshold=1`) is unchanged, only the
  connection mode changes.
- Defense-in-depth dedup: even though Go already dedups at intake, the RAG
  service checks its own `indexed_documents` before processing — protects
  against registry drift or manual stream replay during testing/debugging.

### 4.4 Infra

- `docker-compose.yml` gains:
  - `qdrant` (image `qdrant/qdrant`, port 6333)
  - `redis` (image `redis:7`)
  - existing `minio` service unchanged
- Regressions fixed as prerequisite work: `initialize_bucket()` restored,
  `qdrant_manager.py` switched to networked client.

## 5. Error Handling & Known POC-Accepted Gaps

| Scenario | Behavior | Accepted gap? |
|---|---|---|
| Client uploads but never calls `/confirm` | Object orphaned in MinIO, no DB row, never enqueued | Yes — no reaper job in this pass |
| `/confirm` called with bad/missing `object_path` | MinIO GetObject fails → 404 to client, nothing written | No gap, handled |
| Duplicate hash at confirm time | Rejected, no enqueue; the just-uploaded duplicate object stays orphaned in MinIO | Yes — no cleanup |
| DB insert succeeds, XADD fails at confirm time | Row persists with `published_at=NULL`; outbox poller retries | Fixed via outbox (§4.1) |
| RAG service crashes mid-message | Message stays unacked in consumer group PEL, reprocessed on restart | Yes — no DLQ/max-retry |
| Chunking/embedding failure (corrupt PDF, ollama down) | XACK not called, message redelivered indefinitely | Yes — no DLQ/max-retry |
| Redis stream replay / duplicate delivery | RAG service's own `indexed_documents` check absorbs it | No gap, handled |

## 6. Testing

- Go: unit tests for hash computation, presign generation, outbox poller
  (mock Redis + MinIO clients). No e2e harness required for POC.
- Python: existing chunking/embedding/search unit tests remain valid in
  isolation; add a test for the stream-consumer loop's dedup-skip and
  point-id-scheme behavior.
- Manual POC validation path: `curl` `/ingest/init` → upload to presigned URL →
  `curl` `/ingest/confirm` → observe RAG service logs consume and index →
  query Qdrant directly to confirm point IDs/payload match §4.3.

## 7. Explicitly Deferred (not this pass)

- Multi-collection/multi-strategy routing (client-specified target collection,
  per-strategy consumer groups/streams). Deferred until a second strategy
  actually exists — current design (one stream, one consumer group named after
  the collection) doesn't preclude adding this later.
- Dead-letter queue / max-retry policy on the Redis Stream.
- Orphaned-object cleanup/reaper for unconfirmed or duplicate uploads.
- Query-side (`Ask`/`AskStream`) fixes — untouched, tracked separately.
- MinIO/Postgres credential hygiene (currently plaintext in `docker-compose.yml`
  and `config.toml`) — not a POC blocker, flagged for whenever this moves past
  local dev.
