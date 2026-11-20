# Pucho architecture

As of 2026-11-20 (main `24eb9f0`, PRD rev 3.1). Pucho is a document question-answering system built
from two independent services: a Go "center" that owns uploads and state, and a Python RAG service
that indexes documents and answers questions. They meet only at the contracts in
[`rag-poc-prd.md`](rag-poc-prd.md) section 4.

The diagrams started in the October design and learning report and are updated here to match the
system as built. Where this page and the PRD differ, the PRD wins.

## 1. System context

```mermaid
flowchart LR
    Browser["Browser<br/>Pucho SPA (React)"]
    subgraph Go["Go center (services/center)"]
        API["HTTP API + static UI<br/>upload, documents, ask (SSE)"]
        HW["Hash workers x2<br/>+ recovery sweeper"]
        OW["Outbox workers x2<br/>+ recovery sweeper"]
        SC["Status consumer"]
        GDB[("SQLite<br/>upload_intent<br/>document_store<br/>outbox")]
    end
    subgraph Obj["MinIO"]
        UB[("upload bucket<br/>temp objects")]
        IB[("ingestion bucket<br/>aa/bb/hash")]
    end
    subgraph Redis["Redis Streams"]
        S1[["ingest.docs"]]
        S2[["ingest.status"]]
    end
    subgraph Py["Python RAG service (services/rag-service)"]
        PC["rag.consumer<br/>parse, chunk, embed"]
        PS["rag.server<br/>gRPC RagEngineService"]
        PDB[("SQLite<br/>indexed_documents")]
        Q[("Qdrant<br/>chunk vectors")]
    end
    Ollama["Ollama on host<br/>qwen3-embedding:4b"]
    Groq["Groq API<br/>openai/gpt-oss-120b"]

    Browser -->|"REST + SSE"| API
    Browser -->|"PUT via presigned URL"| UB
    API --> GDB
    HW --> GDB
    HW -->|"hash, copy, delete temp"| UB
    HW -->|"content-addressed copy"| IB
    OW --> GDB
    OW -->|"XADD"| S1
    S1 -->|"XREADGROUP"| PC
    PC -->|"fetch object"| IB
    PC --> Ollama
    PC --> Q
    PC --> PDB
    PC -->|"XADD"| S2
    S2 -->|"XREADGROUP"| SC
    SC --> GDB
    API -->|"gRPC AskStream"| PS
    PS --> Ollama
    PS --> Q
    PS --> Groq
```

Transport rule: gRPC when a live caller waits (asking a question), Redis Streams events when nobody
is blocked (ingest and status). Delivery is at-least-once, and every consumer is idempotent.

## 2. Deployment (docker compose)

```mermaid
flowchart TB
    subgraph Host["Laptop host"]
        Ollama["Ollama :11434"]
        subgraph Compose["docker compose project (checkout folder name)"]
            center["center :8080<br/>Go API + SPA from /srv/web"]
            cm["center-migrate<br/>(one-shot: SQLite + buckets)"]
            rm["rag-migrate<br/>(one-shot: SQLite + Qdrant collection)"]
            rc["rag-consumer"]
            rs["rag-server :50051"]
            minio["minio :9000 / console :9001"]
            redis["redis :6379"]
            qdrant["qdrant :6333"]
        end
    end
    Groq["Groq API (internet)"]

    cm --> minio
    rm --> qdrant
    center --> minio
    center --> redis
    center --> rs
    rc --> redis
    rc --> minio
    rc --> qdrant
    rc --> Ollama
    rs --> qdrant
    rs --> Ollama
    rs --> Groq
```

All ports bind to `127.0.0.1`. One shared `config.toml` (compose uses `deploy/config.compose.toml`)
configures every service. The only secret passed through the environment is `GROQ_API_KEY`. The
SPA is built by a Node stage in the center image and served from the same origin as the API, so the
Go API needs no CORS. MinIO allows CORS from `:8080` and from the Vite dev server on `:5173`.

## 3. Data model (Go SQLite)

```mermaid
erDiagram
    upload_intent {
        text uuid PK "upload id shown to the browser"
        text file_name
        text temp_bucket_path
        text hash FK "set by the hash worker"
        text status "WAITING UPLOADED HASHING HASHED EXPIRED FAILED"
        text claimed_by
        timestamp claimed_at
        int retries "hashing attempts"
        timestamp created_at
        timestamp expires_at
        timestamp uploaded_at
    }
    document_store {
        text hash PK "SHA-256 of content"
        text perm_bucket_path "aa/bb/hash"
        text file_name "first uploader's name"
        int size
        text status "PROCESSING INDEXED FAILED"
        int retries "ingest attempt number"
        text failure_reason "PRD 4.2 code, set on terminal FAILED"
        timestamp created_at
        timestamp updated_at
    }
    outbox {
        int id PK
        text hash FK
        text status "PENDING CLAIMED PUBLISHED BLOCKED"
        text claimed_by
        timestamp claimed_at
        int retries "publish attempts"
        timestamp created_at
    }
    upload_intent }o--o| document_store : "hash (many uploads, one document)"
    document_store ||--o{ outbox : "one row per ingest attempt"
```

The three `retries` columns count different things: hashing attempts on `upload_intent`, ingest
attempts on `document_store` (sent to Python as `attempts` and echoed back), and publish attempts
on `outbox`. Python keeps its own `indexed_documents` table and never reads Go's database.

## 4. Upload, hash and publish

```mermaid
sequenceDiagram
    autonumber
    participant B as Browser
    participant A as Go API
    participant DB as Go SQLite
    participant M as MinIO
    participant H as Hash worker
    participant O as Outbox worker
    participant R as Redis ingest.docs

    B->>A: POST /upload {filename}
    A->>DB: INSERT upload_intent (WAITING)
    A-->>B: 201 id + presigned PUT URL
    B->>M: PUT file to upload bucket (key = upload id)
    B->>A: POST /upload/{id}/confirm
    A->>DB: UPDATE WAITING to UPLOADED WHERE not expired
    A-->>B: 202 (idempotent)
    loop every interval, per worker
        H->>DB: claim UPLOADED to HASHING (claimed_by = me)
        H->>M: stream object, SHA-256, size
        H->>M: copy to ingestion/aa/bb/hash, delete temp
        H->>DB: tx: INSERT document_store ON CONFLICT DO NOTHING
        Note over H,DB: new document only: INSERT outbox PENDING.<br/>Then intent HASHING to HASHED WHERE claimed_by = me
    end
    loop every interval, per worker
        O->>DB: claim PENDING to CLAIMED (claimed_by = me)
        O->>DB: load document_store row (attempts = retries)
        O->>R: XADD IngestionRequest (MAXLEN ~ stream_max_len)
        O->>DB: MarkPublished WHERE claimed_by = me AND status = CLAIMED
    end
    B->>A: GET /documents (poll)
    A-->>B: one entry per content hash, newest first
```

A duplicate upload is not a failure: its intent links to the existing `document_store` row and
reports that document's status. The UI shows duplicates as one document.

## 5. Ingest and status

```mermaid
sequenceDiagram
    autonumber
    participant R1 as Redis ingest.docs
    participant P as rag.consumer
    participant M as MinIO
    participant E as Ollama
    participant Q as Qdrant
    participant R2 as Redis ingest.status
    participant G as Go status consumer
    participant DB as Go SQLite

    R1->>P: XREADGROUP {hash, bucket_path, filename, attempts}
    P->>M: fetch object by bucket_path
    P->>P: parse by extension (.pdf .txt .md), chunk 1000/150
    P->>E: embed chunks
    P->>Q: delete by payload hash, then upsert points
    P->>R2: XADD IngestionStatus {hash, INDEXED or FAILED, attempts}
    P->>R1: XACK (only after publish)
    R2->>G: XREADGROUP
    alt INDEXED(N)
        G->>DB: UPDATE status = INDEXED WHERE hash AND retries = N AND status = PROCESSING
    else FAILED(N), retryable, N+1 under max_attempts
        G->>DB: tx: retries = N+1 + INSERT outbox PENDING
    else FAILED(N), permanent or at max
        G->>DB: status = FAILED, failure_reason = reason
    end
    Note over G,DB: 0 rows = stale or duplicate event: log and ack
    G->>R2: XACK (only after commit)
```

Python owns its consumer group on `ingest.docs`; Go owns its group on `ingest.status`. Each job is
bounded by `job_timeout` plus a watchdog, which stays below the `XAUTOCLAIM` `min_idle`, so a live
job is never reclaimed. Entries delivered more than `poison_threshold` times are reported as
`FAILED`, `POISON_MESSAGE`, not retryable.

## 6. Ask (streamed answer)

```mermaid
sequenceDiagram
    autonumber
    participant B as Browser
    participant A as Go API
    participant DB as Go SQLite
    participant S as rag.server
    participant E as Ollama
    participant Q as Qdrant
    participant L as Groq

    B->>A: POST /ask {question, upload_ids?}
    A->>A: validate length and id count (400 before headers)
    A->>DB: resolve upload ids to INDEXED hashes
    alt ids given but none INDEXED
        A-->>B: empty sources + "No relevant information" delta, no Python call
    else
        A->>S: AskStream(question, document_ids) with [go.ask].deadline
        S->>E: embed query (with query instruction)
        S->>Q: search top_k, filter by hash, min_score
        alt no chunk above min_score
            S-->>A: message 1: empty sources + "nothing relevant" answer
        else
            S-->>A: message 1: sources (filename, snippet, score)
            S->>L: chat completion, streamed
            L-->>S: answer tokens
            S-->>A: messages 2..n: answer deltas
        end
        A-->>B: event: sources, event: delta ..., event: done
    end
    Note over B,A: Stop or disconnect cancels the gRPC call.<br/>Failures end the stream with event: error (UNAVAILABLE, INVALID_ARGUMENT)
```

The browser never sees content hashes. It names documents by upload id, and Go maps them to hashes.

## 7. State machines

### Upload intent

```mermaid
stateDiagram-v2
    [*] --> WAITING: POST /upload
    WAITING --> UPLOADED: confirm before expires_at
    WAITING --> EXPIRED: confirm or poll after expires_at
    UPLOADED --> HASHING: worker claim (claimed_by = worker)
    HASHING --> HASHED: StartIngestion tx commits
    HASHING --> UPLOADED: transient error (retries+1) or stale claim recovered
    HASHING --> FAILED: temp object missing, or retries at max
    HASHED --> [*]
    EXPIRED --> [*]
    FAILED --> [*]
```

`GET /documents` shows `UPLOADED` and `HASHING` as `PROCESSING`, an intent-level `FAILED` as
`UPLOAD_FAILED`, and hides `WAITING` and `EXPIRED`.

### Outbox

```mermaid
stateDiagram-v2
    [*] --> PENDING: inserted with the document row or a retry
    PENDING --> CLAIMED: atomic claim by one worker
    CLAIMED --> PUBLISHED: XADD ok and MarkPublished (fenced by claimed_by)
    CLAIMED --> PENDING: publish failed (retries+1) or stale claim recovered
    CLAIMED --> BLOCKED: retries at max, or document row missing
    PUBLISHED --> [*]
    BLOCKED --> [*]: needs a human (dead letter, backlog B1)
```

### Document ingestion

```mermaid
stateDiagram-v2
    [*] --> PROCESSING: document_store row created, retries = 0
    PROCESSING --> PROCESSING: FAILED(N) retryable, N+1 under max: retries = N+1 + new outbox row
    PROCESSING --> INDEXED: INDEXED(N), CAS on retries = N
    PROCESSING --> FAILED: FAILED(N) permanent, or N+1 at max_attempts
    INDEXED --> [*]
    FAILED --> [*]
```

### Outbox worker outcomes

```mermaid
flowchart TD
    A["Fetch PENDING candidates"] --> B["Claim: PENDING to CLAIMED, claimed_by = me"]
    B -->|"0 rows"| X1["Skip: another worker won"]
    B -->|"1 row"| C["Load document_store row"]
    C -->|"DB error"| R1["ReleaseClaim: retries + 1, back to PENDING, BLOCKED at max"]
    C -->|"row missing"| K1["MarkBlocked: retrying cannot help"]
    C -->|"found"| D["XADD event, bounded by job_timeout"]
    D -->|"error"| R1
    D -->|"ok"| E["MarkPublished WHERE claimed_by = me AND status = CLAIMED"]
    E -->|"1 row"| P["PUBLISHED"]
    E -->|"0 rows"| L["Claim lost: log only, the new owner publishes"]
    E -->|"DB error"| S["Leave CLAIMED: the sweeper demotes it, a duplicate event is possible"]
```

Every branch ends in a state a sweep or a human can find. The two duplicate-event branches are
absorbed downstream by the `(hash, attempts)` CAS.

## 8. Crash windows

For every point where a process can die: the state it leaves and the component that repairs it.

| Where the process dies | State left behind | Who repairs it | Cost |
| --- | --- | --- | --- |
| Hash worker, after the copy and before the DB transaction | Object at `aa/bb/<hash>`; intent still `HASHING` | Hash sweeper demotes; the retry copies the same bytes to the same key | Wasted storage at worst (orphan cleanup is backlog B3) |
| Hash worker, while `HASHING` | Stale claim | Hash sweeper: back to `UPLOADED`, or `FAILED` at max | Delay up to `stale_threshold` |
| Outbox worker, after the claim and before `XADD` | Row `CLAIMED`, nothing published | Outbox sweeper demotes to `PENDING` | Delay up to `stale_threshold`; no duplicate |
| Outbox worker, after `XADD` and before `MarkPublished` | Event in the stream, row `CLAIMED` | Outbox sweeper demotes; the event is published again | Duplicate, absorbed by `(hash, attempts)` |
| Redis unavailable | Every publish fails | `ReleaseClaim` spends a retry; `BLOCKED` at max | A human must unblock; nothing lost |
| Python, while processing an entry | Entry pending, no ack | `XAUTOCLAIM` recovery pass reclaims it; past `poison_threshold` it is reported `FAILED` | Redelivery |
| Go, inside the status transaction | Nothing committed | Redelivery applies it once | None |
| Go, after the status commit and before `XACK` | Update applied, entry still pending | Redelivery; the CAS matches 0 rows; the entry is acked | None |
| Python down longer than the stream cap | Oldest events trimmed while the outbox says `PUBLISHED`; document stuck `PROCESSING` | Nothing yet: needs reconciliation (backlog B2) and lag alerting (B4) | Silent until built |

The last row is the largest open risk, because no component notices it.
