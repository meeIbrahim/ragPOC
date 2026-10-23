# RAG service

The Python half of the RAG POC (`docs/rag-poc-prd.md` section 5). It has two processes:

- `python -m rag.consumer` reads ingest events from Redis. For each one it fetches the file from MinIO, then parses, chunks and embeds it with Ollama. It stores the chunks in Qdrant, records the document in SQLite, and publishes a status event.
- `python -m rag.server` serves `rag.v1.RagEngineService` (`Ask`, `AskStream`) over gRPC. It retrieves from Qdrant and generates the answer with Groq.

Both read the shared `config.toml` at the repo root, or the path in `$RAG_CONFIG` or `--config`.

## Data handling

Retrieved chunks and the question are sent to Groq, an external service. Ingest only POC test documents, such as the public eval corpus in `evals/corpus`. **Never ingest client or production documents** until the data-handling position is cleared (PRD 5.4).

## Host dev loop

Prerequisites:
- Docker
- `uv`
- [Ollama](https://ollama.com) on the host, with the embedding model pulled: `ollama pull qwen3-embedding:4b`
- `grpcurl`, for queries
- A Groq API key, exported in your shell only. Never put it in a file in git.

From the repo root:

```sh
export GROQ_API_KEY=...           # shell only
make up                           # MinIO, Redis, Qdrant
make go_migrate                   # creates the MinIO buckets (Go owns them)
make run_migrations               # indexed_documents table, Qdrant collection and hash index
make run_worker                   # terminal 1: the real ingest consumer
make run_server                   # terminal 2: the gRPC server on :50051
```

Ingest a file and wait for its status. The tool prints the content hash, then the status event:

```sh
cd services/rag-service
uv run python -m rag.tools.ingest_file ../../evals/corpus/<file> --wait 900
```

Query with `grpcurl`. Reflection is on, so it needs no `.proto`:

```sh
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -d '{"query": "What is X?"}' localhost:50051 rag.v1.RagEngineService/Ask
grpcurl -plaintext -d '{"query": "What is X?", "document_ids": ["<hash>"]}' \
  localhost:50051 rag.v1.RagEngineService/AskStream
```

`AskStream` returns messages in this order:
1. The first message carries the sources (`contextUsed`) and an empty answer.
2. Later messages carry only answer deltas.

If nothing passes `[rag].min_score`, the answer is a fixed "No relevant information..." text and Groq is not called.

Status codes:
- `INVALID_ARGUMENT`: an empty query, a query over `max_query_chars`, or more than `max_document_ids` ids.
- `UNAVAILABLE`: Ollama, Qdrant or Groq failed.
- `RESOURCE_EXHAUSTED`: more than `[rag_grpc].max_workers` calls are already running. The server rejects the extra call instead of queueing it, and the Phase 3 Go proxy must map this code too.
- `INTERNAL`: anything else, with a generic message.

To run the whole stack in containers, use `make stack` (`rag-consumer` and `rag-server` are included). Ollama stays on the host; the containers reach it at `host.docker.internal:11434`.

## Re-indexing

Changing `[rag].chunk_size` or `chunk_overlap` needs a re-index. Already indexed documents are otherwise skipped.

```sh
make reset_index                  # dry run: prints what it would delete
make reset_index ARGS=--yes       # deletes every point and indexed_documents row
```

`reset_index` acts on whatever the config points at. Never point it at shared data.

Changing `[rag].embedding_model` to one with a different output size means you must also, in this order:
1. Set `embedding_dim` to the new size.
2. Run `make reset_index ARGS=--yes`. Otherwise the `indexed_documents` rows make every document skip re-ingest, and the new collection stays empty with no error. It also clears the rows if the collection is already gone.
3. Drop the Qdrant collection.
4. Run `make run_migrations` again, then re-ingest.

The consumer and the server both refuse to start on a size mismatch.

## Exit codes

| Process | 0 | 1 | 2 | 3 | 130 |
|---|---|---|---|---|---|
| `rag.consumer`, `rag.server` | normal stop | dependency unreachable at startup (a restart may help) | bad config or setup (wrong `embedding_dim`, missing collection, model not pulled, no `GROQ_API_KEY`) | | Ctrl-C |
| `rag.tools.ingest_file` | published, or `INDEXED` with `--wait` | `FAILED` | usage or connection error | `--wait` timed out | |

## Tests

```sh
uv run pytest                                  # unit tests, no services needed
REDIS_URL=redis://localhost:6379 MINIO_URL=localhost:9000 MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=... \
  QDRANT_URL=http://localhost:6333 uv run pytest -m integration
RAG_CONFIG=../../config.toml uv run pytest -m ollama   # host Ollama
uv run pytest -m groq                          # needs GROQ_API_KEY; sends one short prompt
```

The gRPC stubs in `src/generated` are committed. After you change `protos/rag/v1/rag.proto`, regenerate them with `make grpc-generate`.
