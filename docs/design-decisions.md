# Design decisions and lessons

As of 2026-11-20. These are the key decisions behind Pucho and the reasoning for each, adapted from
the October design and learning report and updated to the system as built. Each decision weighs a
concrete failure mode, not a preference. The binding list is PRD section 2 (locked decisions L1 to
L21) in [`rag-poc-prd.md`](rag-poc-prd.md); diagrams are in [`architecture.md`](architecture.md).

## State and storage

| Decision | Reasoning |
| --- | --- |
| Move the file first, then insert DB rows | A crash leaves an inert orphan file, never a ghost row that downstream acts on. Content addressing makes a repeat move a no-op |
| One `document_store` row per distinct document (merged `documents` and `ingestion_processes`) | A single source of ingestion state per content hash |
| Hash computed in Go, not taken from MinIO | MinIO has no trusted SHA-256 for plain presigned PUTs (tested) |
| Hashing is async in a worker; the client polls | A long request on a large file is fragile. Polling holds no server state and survives disconnects |
| Duplicates are not failures | A duplicate upload links to the existing document and reports its status; many uploads fan in to one document |
| A missing temp object fails the intent without spending retries | A missing object never comes back; retrying wastes attempts |
| SQLite with one writer (`SetMaxOpenConns(1)`, WAL, `busy_timeout`) | Simple and local; a single conditional `UPDATE` is the concurrency primitive |

## Workers and recovery

| Decision | Reasoning |
| --- | --- |
| Every write after a claim is fenced with `claimed_by = me` | A worker that lost its claim to the sweep cannot overwrite the new owner |
| Recovery sweeps only demote, never publish | Two instances cannot both see `CLAIMED` and safely agree who publishes |
| Stale-claim reclaim ignores `claimed_by` | Checking the owner would let one stuck instance hold its rows forever |
| Per-job timeout covers the external call only; outcome writes use the worker context | The job context may already be expired when the outcome must be recorded. `job_timeout` stays below `stale_threshold` |
| Retry count persisted on the row; past the limit the row goes to `BLOCKED` | Without a stored count the system cannot tell a first attempt from a fifth. `BLOCKED` is the dead letter |
| Rejected: odd/even time windows to split sweep work | Does not generalize past two instances and breaks at window boundaries under clock drift |

## Events and the status leg

| Decision | Reasoning |
| --- | --- |
| gRPC when a live caller waits, events when nobody is blocked | Ingest and status can finish on their own schedule; answer tokens cannot |
| Publish, then mark published; accept re-publish | At-least-once with idempotent consumers is cheaper and more honest than chasing exactly-once |
| Explicit `attempts` end to end; dedupe key `(hash, attempts)` | Rejects stale or duplicate status updates without a hardcoded "FAILED wins" rule. Event `id` is tracing only |
| `FAILED` handler = one transaction (CAS + retry bump + new outbox row); `XACK` only after commit | A crash cannot leave a bumped counter with no retry event |
| `retryable` on `FAILED`; permanent failures never consume retries | An unsupported or empty file will not parse on the next attempt either |
| `INDEXED` uses CAS plus `status = 'PROCESSING'` | Known cost: a late `INDEXED` after retries are exhausted leaves the document `FAILED` |
| `retries` is never reset on success | It records how many attempts the document needed; a reset could make a late event look stale for the wrong reason |
| Each consumer owns its group, created at `0` with `MKSTREAM` | Events published before the group existed are not missed |
| Poison detection by delivery count in the recovery pass only | `attempts` does not change when a consumer crashes; the `XAUTOCLAIM` delivery count does |
| No consumer heartbeat in v1 | `job_timeout + watchdog_grace < min_idle` guarantees a live job is never reclaimed (heartbeat is backlog B11) |
| The event carries `bucket_path` as an opaque key | Go owns the object layout; the extension is not derivable from the hash |
| `schema_version` in every event | Costs one field; without it both services must deploy in lockstep |
| `stream_max_len` = 324000 | Sized for a 72 h outage at peak load (about 130 to 170 MB). Above the cap, `XADD MAXLEN` silently drops unread events, so lag needs monitoring |

## Query and UI

| Decision | Reasoning |
| --- | --- |
| Go proxies `AskStream` as SSE from the same origin as the SPA | No CORS on Go; a client disconnect cancels the gRPC call |
| Sources first: message 1 carries sources, later messages carry answer deltas | The UI shows evidence before the answer; a Groq error after message 1 ends the stream with `UNAVAILABLE` |
| `min_score` threshold; no Groq call when nothing scores above it | Avoids a hallucinated answer and an external call when retrieval found nothing |
| The browser never sees content hashes | It names documents by upload id; Go maps ids to hashes and merges duplicates in `GET /documents` |
| File bytes go browser to MinIO by presigned PUT | Go never streams uploads; the URL is signed with `[minio].public_url` |
| `qwen3-embedding:4b` (2560 dims), chunks 1000/150, top_k 5 | Chosen by a throughput spike on the dev laptop and a retrieval eval that scored 40/40 at rank 1 |

## Lessons

- A single `UPDATE ... WHERE status = X` is already a compare-and-swap; `RowsAffected()` says who
  won, and 0 means skip. `BEGIN IMMEDIATE` is only for multi-statement read-then-write.
- When two systems cannot share a transaction, pick the leftover state that is cheaper to repair
  (an orphan file, not a ghost row).
- Fencing matters under a stalled worker: A stalls, the sweep demotes, B re-claims, A wakes. A
  status-only check lets A overwrite B's row; an owner check does not.
- Ack-after-commit plus CAS gives an idempotent consumer: a crash before `XACK` redelivers, the CAS
  matches 0 rows, and the entry is acked.
- Redis Streams: `XACK` does not delete entries; `MAXLEN` counts entries and trims unread ones;
  work owed is `lag + pending`; `XAUTOCLAIM` is the stream's own recovery sweep.
- Two counters, two jobs: the delivery count finds poison messages, `attempts` finds stale events.
- Two kinds of version: the content hash versions a document, `schema_version` versions the
  message format.
- `go build` and `go vet` do not flag an unused parameter or a typo in SQL written as a string;
  tests and a mutation check caught both.

## Open risks

See the backlog in PRD section 8. The largest: a document stays `PROCESSING` forever if its event is
lost (stream trimmed during a long outage, or a poison entry dropped), because nothing reconciles
old `PROCESSING` rows yet (B2). Failed documents are never retried, and re-uploading the same
content does not retry them (B1).
