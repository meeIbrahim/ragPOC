# Phase 1 fault runbook

This runbook proves the real ingest consumer's failure handling against real dependencies (spec R11; PRD 7 Phase 1 "Done when"):

| Scenario | Proves | Spec |
|---|---|---|
| 1. Job deadline | A job past `job_timeout` ends `FAILED JOB_TIMEOUT retryable=true` from a deadline check, not the watchdog | E10 |
| 2. Watchdog, restart, reclaim, poison | A step that never returns triggers the watchdog exit and a compose restart under a new consumer name. The entry is reclaimed after `min_idle`, and on the 4th delivery it is `FAILED POISON_MESSAGE retryable=false` | E11 |
| 3. Wrong `embedding_dim` | The consumer exits 2 at startup and the message names both sizes | E12 |

It runs as its own compose project, `ragpoc-p1faults`, with named volumes and no published ports. So it never touches the dev stack or the e2e, and `down -v` removes exactly what it created.

**Needs:**
- Docker with Compose v5.2+.
- The host Ollama with `qwen3-embedding:4b` pulled (scenarios 1 and 3).
- No Groq, and no Go API server.

The configs are `deploy/config.faults.toml` and `deploy/config.faults-blackhole.toml`. They use:
- `job_timeout=3s`, `watchdog_grace=2s`, `min_idle=10s`, `recovery_interval=2s` and `call_timeout=60s`.
- `embed_batch_size=1`, so that one Ollama call (about 0.7 s) is shorter than the watchdog grace.
- `allow_skip_dim_check=true`, which is only set in these configs.

## Setup

From the repo root, in zsh or bash:

```sh
DC=(docker compose -p ragpoc-p1faults -f docker-compose.yml -f docker-compose.faults.yml --profile app)
"${DC[@]}" build center-migrate rag-consumer
"${DC[@]}" run --rm center-migrate            # buckets (Go owns them); a one-shot, so not in `up --wait`
"${DC[@]}" up -d --wait rag-consumer          # also runs rag-migrate (SQLite table, Qdrant collection, index)
"${DC[@]}" logs rag-consumer | grep "starting ingest consumer"   # real ingest, model qwen3-embedding:4b

IN=$(mktemp -d)
cat evals/corpus/jekyll-and-hyde.txt evals/corpus/jekyll-and-hyde.txt > "$IN/big.txt"   # ~330 KB, ~390 chunks
printf 'Blackhole scenario file: this document never gets embedded.\n' > "$IN/small.txt"
```

## 1. Job deadline (`JOB_TIMEOUT`)

```sh
"${DC[@]}" run --rm --no-deps -v "$IN":/in:ro rag-consumer \
  python -m rag.tools.ingest_file /in/big.txt --wait 60; echo "exit $?"
docker inspect ragpoc-p1faults-rag-consumer-1 --format 'RestartCount={{.RestartCount}}'
```

Expected:
- `ingest_file` exits 1.
- The status event is `"status": "FAILED"`, `"reason": "JOB_TIMEOUT"`, `"retryable": true`, `"detail": "job deadline passed"`.
- `RestartCount=0`, because the deadline check ended the job, not the watchdog.

## 2. Watchdog, restart, reclaim, poison

This switches the consumer to the blackhole config and starts the blackhole. The blackhole is a server that accepts connections on the Ollama port and never replies. The dimension check is skipped, which works only because `RAG_SKIP_DIM_CHECK=1` and the config's `allow_skip_dim_check=true` are both set.

```sh
export FAULTS_CONFIG=./deploy/config.faults-blackhole.toml
RAG_SKIP_DIM_CHECK=1 "${DC[@]}" up -d --force-recreate --no-deps blackhole rag-consumer
"${DC[@]}" run --rm --no-deps -v "$IN":/in:ro rag-consumer \
  python -m rag.tools.ingest_file /in/small.txt --wait 180; echo "exit $?"
docker inspect ragpoc-p1faults-rag-consumer-1 --format 'RestartCount={{.RestartCount}}'
"${DC[@]}" logs rag-consumer | grep -E "starting ingest consumer|watchdog|poison"
unset FAULTS_CONFIG
```

Expected (takes about 30 s):
- `ingest_file` exits 1 with `"reason": "POISON_MESSAGE"`, `"retryable": false`, `"attempts": 0`.
- `RestartCount=3`.
- The logs show 4 different consumer names (`<host>/ingest/<uuid>`), 3 lines `watchdog: a job ran past 5s and did not return; exiting the process`, and then `poison ingest entry ... delivered 4 times (threshold 3)`.

## 3. Wrong `embedding_dim`

```sh
sed 's/^embedding_dim=2560$/embedding_dim=1024/' deploy/config.faults.toml > "$IN/dim1024.toml"
FAULTS_CONFIG="$IN/dim1024.toml" "${DC[@]}" run --rm --no-deps rag-consumer; echo "exit $?"
```

Expected:
- Exit 2.
- An `ERROR` saying: `embedding model 'qwen3-embedding:4b' outputs 2560 dimensions but [rag].embedding_dim is 1024: set embedding_dim to the model's size and re-create the Qdrant collection ...`

## Teardown

```sh
"${DC[@]}" down -v          # removes only the ragpoc-p1faults containers and volumes
rm -r "$IN"
```

## Recorded run (2026-10-23, Apple M4, Ollama 0.40.8, commit before the PR)

1. **JOB_TIMEOUT:**
   - Status: `FAILED`, `JOB_TIMEOUT`, `retryable: true`, `detail: "job deadline passed"`.
   - `RestartCount=0`. The run took about 4 s.
2. **Watchdog and poison:**
   - `ingest_file` exited 1 with `FAILED`, `POISON_MESSAGE`, `retryable: false`, `attempts: 0`.
   - `RestartCount=3`.
   - Consumers `…/ingest/8d179647…`, `…/f94f9f7c…`, `…/85cf200c…` and `…/02dee993…` started about 10 s apart (the `min_idle` reclaim). Each of the first three logged the watchdog exit.
   - The 4th logged `poison ingest entry 1792753881026-0 delivered 4 times (threshold 3)`.
   - From `XADD` to poison took 31 s.
3. **Dimension mismatch:** exit 2, with the message above.

`up -d --wait` with a one-shot service named in it returns 1 even when the one-shot exits 0. That is why `center-migrate` runs with `run --rm` first.
