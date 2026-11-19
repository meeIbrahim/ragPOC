#!/usr/bin/env bash
# Scripted Pucho demo against the running stack (compose-demo A5, A7). Run with `make demo`
# after `GROQ_API_KEY="$GROQ" make stack`. Every step prints PASS or FAIL; the first FAIL stops.
#
# File names are unique per run but the content is fixed, so a second run on the same stack
# merges into the existing documents and still passes. Source checks therefore compare against
# the entry's stored filename from GET /documents, not this run's name.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CENTER="${CENTER_URL:-http://localhost:8080}"
OLLAMA="${OLLAMA_URL:-http://localhost:11434}"
# compose's own default project name: the checkout folder, normalized (see Makefile).
PROJECT="${COMPOSE_PROJECT_NAME:-$(basename "$ROOT" | tr 'A-Z' 'a-z' | tr -cd 'a-z0-9_-' | sed 's/^[^a-z0-9]*//')}"
INDEX_TIMEOUT="${DEMO_INDEX_TIMEOUT:-300}"
ASK_TIMEOUT="${DEMO_ASK_TIMEOUT:-120}"
RUN="$(date -u +%Y%m%dT%H%M%SZ)"

# Fixed questions: evals/questions.jsonl k801 (rank-1 retrieval proven), one about the demo PDF's
# own intro (only the PDF holds it), and evals/negatives.jsonl neg01 (off-topic).
Q_ALL="What is the tiniest unit of compute that the cluster lets you create and manage?"
Q_PDF="Which chapter of the novel does the Pucho demo sample reproduce?"
Q_OFF="How long should sourdough dough proof before baking?"

pass() { printf '  PASS  %s\n' "$1"; }
fail() {
  printf '  FAIL  %s\n' "$1" >&2
  exit 1
}

echo "Pucho demo: project $PROJECT, center $CENTER, run $RUN"

# ---- Pre-flight

echo "Pre-flight"
curl -sf -m 5 "$CENTER/documents" >/dev/null || fail "the center doesn't answer on $CENTER. Start the stack: GROQ_API_KEY=\"\$GROQ\" make stack"
pass "center answers on $CENTER"

model="$(sed -n 's/^embedding_model="\(.*\)"$/\1/p' "$ROOT/deploy/config.compose.toml" | head -1)"
tags="$(curl -sf -m 5 "$OLLAMA/api/tags")" || fail "Ollama doesn't answer on $OLLAMA. Start Ollama on the host."
jq -e --arg m "$model" '[.models[].name] | index($m)' <<<"$tags" >/dev/null ||
  fail "Ollama doesn't list the embedding model $model. Run: ollama pull $model"
pass "Ollama answers and lists $model"

health="$(docker compose -p "$PROJECT" --profile app ps --format '{{.Health}}' rag-server 2>/dev/null || true)"
[[ "$health" == "healthy" ]] ||
  fail "rag-server is not healthy (${health:-not running}). It refuses to start without GROQ_API_KEY: GROQ_API_KEY=\"\$GROQ\" make stack"
pass "rag-server is healthy"

# ---- Helpers

# entry_for UPLOAD_ID: the GET /documents entry that lists the upload id, or nothing.
entry_for() {
  local docs
  docs="$(curl -sf -m 10 "$CENTER/documents")" || return 0
  jq -c --arg id "$1" 'first(.documents[] | select(.upload_ids | index($id)))' <<<"$docs"
}

# upload FILE NAME: create, PUT, confirm; prints the upload id.
upload() {
  local file="$1" name="$2" created id url
  created="$(curl -sf -m 10 -X POST "$CENTER/upload" -H 'Content-Type: application/json' \
    -d "$(jq -cn --arg f "$name" '{filename: $f}')")" || fail "POST /upload for $name failed"
  id="$(jq -r .id <<<"$created")"
  url="$(jq -r .upload_url <<<"$created")"
  # The presigned URL carries a signature: never print it.
  curl -sf -m 60 -X PUT --data-binary "@$file" "$url" >/dev/null || fail "PUT of $name to storage failed"
  curl -sf -m 10 -X POST "$CENTER/upload/$id/confirm" >/dev/null || fail "confirm of $name failed"
  echo "$id"
}

# wait_indexed UPLOAD_ID NAME: waits for the entry listing the id to be INDEXED; prints the entry.
wait_indexed() {
  local id="$1" name="$2" deadline=$((SECONDS + INDEX_TIMEOUT)) entry status
  while ((SECONDS < deadline)); do
    entry="$(entry_for "$id")"
    status="$(jq -r '.status // empty' <<<"${entry:-null}")"
    case "$status" in
    INDEXED)
      echo "$entry"
      return 0
      ;;
    FAILED) fail "$name failed to index: $(jq -r '.failure_reason // "no reason"' <<<"$entry")" ;;
    esac
    sleep 2
  done
  fail "$name was not INDEXED within ${INDEX_TIMEOUT}s"
}

# ask QUESTION IDS_JSON: POST /ask; prints one compact JSON object per SSE event.
ask() {
  local body
  body="$(jq -cn --arg q "$1" --argjson ids "$2" '{question: $q, upload_ids: $ids}')"
  curl -sN -m "$ASK_TIMEOUT" -X POST "$CENTER/ask" -H 'Content-Type: application/json' -d "$body" |
    awk '/^event: /{ev=substr($0,8)} /^data: /{print "{\"event\":\"" ev "\",\"data\":" substr($0,7) "}"}'
}

# ---- Steps

echo "1. Upload and index"
# index_one KEY FILE: uploads and waits; sets ID_<var> and NAME_<var> (bash 3.2 has no assoc arrays).
index_one() {
  local var="$1" key="$2" file="$3" name id entry
  name="demo-$RUN-$key"
  id="$(upload "$file" "$name")"
  entry="$(wait_indexed "$id" "$name")"
  printf -v "ID_$var" '%s' "$id"
  printf -v "NAME_$var" '%s' "$(jq -r .filename <<<"$entry")"
  pass "$name INDEXED (listed as $(jq -r .filename <<<"$entry"))"
}
index_one K8S kubernetes-pods.md "$ROOT/evals/corpus/kubernetes-pods.md"
index_one RUST rust-ownership.md "$ROOT/evals/corpus/rust-ownership.md"
index_one PDF pucho-demo.pdf "$ROOT/demo/pucho-demo.pdf"

echo "2. Duplicate merges"
rust_entry="$(entry_for "$ID_RUST")"
rust_id="$(jq -r .id <<<"$rust_entry")"
dup_name="demo-$RUN-rust-ownership-copy.md"
dup_id="$(upload "$ROOT/evals/corpus/rust-ownership.md" "$dup_name")"
deadline=$((SECONDS + 60))
merged=""
while ((SECONDS < deadline)); do
  # Before hashing the copy is its own PROCESSING entry; it folds into the older one after.
  if [[ "$(jq -r '.id // empty' <<<"$(entry_for "$dup_id")")" == "$rust_id" ]]; then
    merged=yes
    break
  fi
  sleep 2
done
[[ -n "$merged" ]] || fail "$dup_name did not merge into $NAME_RUST within 60s"
count="$(curl -sf -m 10 "$CENTER/documents" | jq --arg id "$dup_id" '[.documents[] | select(.upload_ids | index($id))] | length')"
[[ "$count" == "1" ]] || fail "$dup_name is listed in $count entries; want 1"
pass "$dup_name merged into $NAME_RUST (one entry, $(jq '.upload_ids | length' <<<"$(entry_for "$dup_id")") upload ids)"

echo "3. Unsupported type"
code="$(curl -s -m 10 -o /dev/null -w '%{http_code}' -X POST "$CENTER/upload" -H 'Content-Type: application/json' -d '{"filename":"photo.png"}')"
[[ "$code" == "400" ]] || fail "POST /upload photo.png = $code; want 400"
pass "photo.png rejected with 400"

echo "4. Ask over all documents"
events="$(ask "$Q_ALL" '[]')"
if jq -e 'select(.event == "error")' <<<"$events" >/dev/null 2>&1; then
  fail "the answer failed: $(jq -r 'select(.event == "error") | "\(.data.code): \(.data.message)"' <<<"$events"). Check GROQ_API_KEY (a bad key fails here)."
fi
nsources="$(jq -s 'map(select(.event == "sources"))[0].data.sources | length' <<<"$events")"
ndeltas="$(jq -s 'map(select(.event == "delta")) | length' <<<"$events")"
ndone="$(jq -s 'map(select(.event == "done")) | length' <<<"$events")"
first="$(jq -rs 'map(select(.event == "sources"))[0].data.sources[0].filename // empty' <<<"$events")"
[[ "$nsources" -ge 1 && "$ndeltas" -ge 1 && "$ndone" == "1" ]] ||
  fail "stream had $nsources sources, $ndeltas deltas, $ndone done; want >=1, >=1, 1"
[[ "$first" == "$NAME_K8S" ]] || fail "top source is $first; want $NAME_K8S"
pass "sources ($nsources), $ndeltas deltas, done; top source $NAME_K8S"

echo "5. Ask scoped to the PDF"
events="$(ask "$Q_PDF" "$(jq -cn --arg id "$ID_PDF" '[$id]')")"
others="$(jq -rs --arg f "$NAME_PDF" 'map(select(.event == "sources"))[0].data.sources // [] | map(select(.filename != $f)) | length' <<<"$events")"
nsources="$(jq -s 'map(select(.event == "sources"))[0].data.sources // [] | length' <<<"$events")"
[[ "$nsources" -ge 1 && "$others" == "0" ]] || fail "scoped ask had $nsources sources, $others not from the PDF"
pass "all $nsources sources are $NAME_PDF"

echo "6. Off-topic question"
events="$(ask "$Q_OFF" '[]')"
empty="$(jq -s 'map(select(.event == "sources"))[0].data.sources | length' <<<"$events")"
[[ "$empty" == "0" ]] || fail "off-topic question got $empty sources; want sources: []"
pass "sources: [] (nothing relevant)"

echo "All demo steps passed."
