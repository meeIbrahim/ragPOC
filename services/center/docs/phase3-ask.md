# Phase 3 ask runbook

This runbook covers the `POST /ask` checks that need Groq and the real ingest consumer (spec `ask-sse` D5). `make e2e` already covers what works without Groq: validation, the short-circuit stream, and `rag-server` down.

| Check | Proves | Spec |
|---|---|---|
| 1. Stream | `curl -N` gets `sources`, then `delta`s, then `done` | Goal, R5 |
| 2. Filter | `upload_ids` limits the sources to the selected documents | R4, E6 |
| 3. Disconnect | Closing `curl` mid-answer cancels the gRPC call; Python logs "answer stream ended early" | R8, E10 |
| 4. Groq unreachable | A bad Groq key gives `event: error` `UNAVAILABLE` | R7, E9 |
| 5. Ollama stopped | Retrieval cannot embed the question: `event: error` `UNAVAILABLE` | R7, E8 |
| 6. SIGTERM | An open stream gets `server shutting down` and the center stops in under 10 s | R9, E11 |

**Data rule:** questions and retrieved chunks go to Groq, so use only the public files in `evals/corpus/`.

**Needs:**
- Docker with Compose v5.2+.
- The host Ollama with `qwen3-embedding:4b` pulled.
- A Groq key in `$GROQ` in your shell. It is passed inline as `GROQ_API_KEY="$GROQ"`; never write it to a file or print it.

It runs as its own compose project, `ragpoc-p3ask`, so its containers and named volumes are separate from the dev stack. MinIO and Qdrant still use the dev bind mounts (`./ingestion_docs/data`, `./qdrant_data`) of the checkout you run it from, so run it from a worktree, or accept that the corpus files land in your dev data. The published ports are the dev ones (8080, 50051, 9000, 6379), so stop the dev stack first.

## Setup

From the repo root, in zsh or bash:

```sh
DC=(docker compose -p ragpoc-p3ask --profile app)
GROQ_API_KEY="$GROQ" "${DC[@]}" up -d --build --wait center rag-consumer rag-server
"${DC[@]}" logs rag-consumer | grep "starting ingest consumer"   # real ingest, not a stub mode
```

`rag-server` has no healthcheck, so `up --wait` returns once the container runs, before the port serves. After every (re)start of it, wait for its ready line, or the next ask gets `connection refused` (`UNAVAILABLE`):

```sh
rag_ready() { until "${DC[@]}" logs rag-server 2>&1 | grep -q "serving rag.v1"; do sleep 1; done; }
rag_ready
```

Both services log `config has keys this service does not read: rag_grpc.host` at startup; that key is Go's only.

The presigned upload URL points at `minio:9000`, which only resolves inside the compose network until feature 2 adds `[minio].public_url`. So `upload_doc FILE` copies the file into the `rag-server` container and runs the client flow there (create, PUT, confirm, poll). It prints the upload id and the final status:

```sh
upload_doc() {
  local name; name=$(basename "$1")
  docker cp "$1" ragpoc-p3ask-rag-server-1:/tmp/"$name"
  "${DC[@]}" exec -T rag-server python - "/tmp/$name" <<'PY'
import json, sys, time, urllib.request
path = sys.argv[1]
base = "http://center:8080"
def call(method, url, body=None, ctype="application/json"):
    req = urllib.request.Request(url, data=body, method=method, headers={"Content-Type": ctype} if body is not None else {})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()
intent = json.loads(call("POST", base + "/upload", json.dumps({"filename": path.rsplit("/", 1)[-1]}).encode()))
with open(path, "rb") as f:
    call("PUT", intent["upload_url"], f.read(), "application/octet-stream")
call("POST", f"{base}/upload/{intent['id']}/confirm", b"")
for _ in range(600):
    status = json.loads(call("GET", f"{base}/upload/{intent['id']}"))["status"]
    if status in ("INDEXED", "FAILED", "EXPIRED"):
        break
    time.sleep(1)
print(intent["id"], status)
PY
}

upload_doc evals/corpus/rust-ownership.md     # note the id: RUST_ID
upload_doc evals/corpus/kubernetes-pods.md    # note the id: K8S_ID
```

Both must print `INDEXED`. Set `RUST_ID` and `K8S_ID` to the printed ids.

## 1. Stream

```sh
curl -sN -X POST localhost:8080/ask -H 'Content-Type: application/json' \
  -d '{"question":"What happens to a value when its owner goes out of scope?"}'
```

Expected: one `event: sources` with `filename`, `snippet` and `score` (no hash), then several `event: delta`, then `event: done`. The sources include `rust-ownership.md`.

## 2. Filter

```sh
curl -sN -X POST localhost:8080/ask -H 'Content-Type: application/json' \
  -d "{\"question\":\"What happens to a value when its owner goes out of scope?\",\"upload_ids\":[\"$K8S_ID\"]}"
```

Expected: every source is `kubernetes-pods.md`, and none is `rust-ownership.md`. The answer is either about pods or the nothing-relevant text.

## 3. Disconnect

```sh
curl -sN -m 0.8 -X POST localhost:8080/ask -H 'Content-Type: application/json' \
  -d '{"question":"Write a long, detailed tutorial on Rust ownership, borrowing, moves, clones and lifetimes, with many examples."}'
sleep 3
"${DC[@]}" logs --since 30s rag-server | grep "answer stream ended early"
"${DC[@]}" logs --since 30s center | grep '"msg":"ask"'
```

Expected: the Python line is present, and the center's `ask` line has `"outcome":"client_gone"`. Groq is fast (a long answer takes about 1 s), so if the output ends with `event: done`, rerun with `-m 0.5`.

## 4. Groq unreachable

```sh
GROQ_API_KEY=invalid "${DC[@]}" up -d --force-recreate --no-deps rag-server && rag_ready
curl -sN -X POST localhost:8080/ask -H 'Content-Type: application/json' -d '{"question":"What is a pod?"}'
GROQ_API_KEY="$GROQ" "${DC[@]}" up -d --force-recreate --no-deps rag-server && rag_ready
```

Expected: `event: sources`, then `event: error` with `{"code":"UNAVAILABLE","message":"the answer service is unavailable, try again"}`. No Groq text reaches the client.

## 5. Ollama stopped

This stops the host Ollama for a few seconds. Quitting the app from a script hits its confirm dialog, so kill it instead:

```sh
pkill -x Ollama; pkill -f "Ollama.app/Contents/Resources/ollama serve"
curl -s -m1 localhost:11434 || echo "ollama down"
curl -sN -X POST localhost:8080/ask -H 'Content-Type: application/json' -d '{"question":"What is a pod?"}'
open -a Ollama
```

Expected: only `event: error` with code `UNAVAILABLE` (the question could not be embedded, so there is no `sources` event).

## 6. SIGTERM with an open stream

```sh
curl -sN -X POST localhost:8080/ask -H 'Content-Type: application/json' \
  -d '{"question":"Write a long, detailed tutorial on Rust ownership, borrowing, moves, clones and lifetimes, with many examples."}' &
sleep 0.5
time "${DC[@]}" stop -t 15 center
wait
"${DC[@]}" logs --since 30s rag-server | grep "answer stream ended early"
```

Expected: the curl output ends with `event: error` and `{"code":"UNAVAILABLE","message":"server shutting down"}`, `stop` takes well under 10 s, and Python logs that the stream ended early. If the output ends with `event: done`, the answer beat the SIGTERM: `"${DC[@]}" up -d --no-deps --wait center` and retry with `sleep 0.3`.

## Teardown

```sh
"${DC[@]}" down -v
rm -rf ./qdrant_data ./ingestion_docs   # only when run from a worktree: these are its bind mounts
```

## Results

| Date | Check | Result |
|---|---|---|
| 2026-11-15 | 1. Stream | PASS: sources (5 × `rust-ownership.md`, no hash in the body), 16 deltas, done |
| 2026-11-15 | 2. Filter | PASS: selecting `kubernetes-pods.md` gave no rust source; no k8s chunk passed `min_score`, so `sources: []` and the nothing-relevant text |
| 2026-11-15 | 3. Disconnect | PASS with `-m 0.8`, 0.6 and 0.5: center `client_gone`, Python "answer stream ended early" each time |
| 2026-11-15 | 4. Groq unreachable | PASS: sources, then `UNAVAILABLE` (Python `groq: AuthenticationError`; no Groq text in the body) |
| 2026-11-15 | 5. Ollama stopped | PASS: only `event: error` `UNAVAILABLE` |
| 2026-11-15 | 6. SIGTERM | PASS with `sleep 0.5`: `stop` took 1.3 s, stream ended with `server shutting down`, Python "ended early" |
