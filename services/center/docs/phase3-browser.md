# Phase 3 browser readiness runbook

This runbook checks the browser path against compose (spec `browser-readiness`, acceptance). The unit tests cover presigning, the static handler and routing. These checks prove the parts that need real MinIO and a real host: a PUT from the host to a presigned URL, MinIO's CORS answer, and `GET /` from `[go.ui].static_dir`.

| Check | Proves | Spec |
|---|---|---|
| 1. Host PUT | `upload_url` names `localhost:9000`; a PUT from the host succeeds; confirm and poll reach `INDEXED` | Goal, R3, edge 2 |
| 2. Preflight | MinIO allows a preflight from `http://localhost:8080` and not from another origin | D2, edge 9 |
| 3. Document list | `GET /documents` lists the upload once, with no hash | Feature 3 R1 |
| 4. No UI | Without `static_dir`, `GET /` is 404 | A4, edge 6 |
| 5. UI | With a temporary `static_dir`, `GET /` serves `index.html` with `Cache-Control: no-cache` | R5, A7 |

**Data rule:** use only the public files in `evals/corpus/`.

**Needs:**
- Docker with Compose v5.2+, `curl`, `jq`.
- The host Ollama with `qwen3-embedding:4b` pulled (real ingest).

It runs as its own compose project, `ragpoc-p3browser`, so its containers and named volumes are separate from the dev stack. MinIO and Qdrant still use the dev bind mounts (`./ingestion_docs/data`, `./qdrant_data`) of the checkout you run it from. Run it from a worktree, or accept that the corpus file lands in your dev data. The published ports are the dev ones (8080, 9000, 6379, 6333), so stop the dev stack first. `rag-server` is not needed.

## Setup

From the repo root, in zsh or bash:

```sh
DC=(docker compose -p ragpoc-p3browser --profile app)
"${DC[@]}" up -d --build --wait center rag-consumer
```

## 1. Host PUT

```sh
resp=$(curl -s -X POST localhost:8080/upload -H 'Content-Type: application/json' -d '{"filename":"kubernetes-pods.md"}')
id=$(jq -r .id <<<"$resp"); url=$(jq -r .upload_url <<<"$resp")
echo "${url%%\?*}"    # http://localhost:9000/upload-bucket/<object>
curl -s -o /dev/null -w '%{http_code}\n' -X PUT --data-binary @evals/corpus/kubernetes-pods.md "$url"    # 200
curl -s -o /dev/null -w '%{http_code}\n' -X POST "localhost:8080/upload/$id/confirm"                      # 202
for i in $(seq 120); do s=$(curl -s "localhost:8080/upload/$id" | jq -r .status); [ "$s" = INDEXED ] || [ "$s" = FAILED ] && break; sleep 1; done; echo "$s"    # INDEXED
```

## 2. Preflight

Use a fresh `upload_url` (step 1's), so the check is against a real presigned URL:

```sh
preflight() { curl -s -i -X OPTIONS -H "Origin: $1" -H 'Access-Control-Request-Method: PUT' "$url" | tr -d '\r' | grep -i '^access-control-allow-origin' || echo "(no Access-Control-Allow-Origin)"; }
preflight http://localhost:8080    # Access-Control-Allow-Origin: http://localhost:8080
preflight http://evil.example      # (no Access-Control-Allow-Origin)
```

## 3. Document list

```sh
curl -s localhost:8080/documents | jq .                       # one entry for $id, INDEXED
curl -s localhost:8080/documents | grep -c '"hash' || true    # 0
```

## 4. No UI

```sh
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/    # 404: static_dir is "" in deploy/config.compose.toml
```

## 5. UI (temporary override, not committed)

`deploy/config.compose.toml` keeps `static_dir=""` until feature 8. This step mounts a temporary copy of it with `static_dir="/ui"` and a one-file UI. The copy holds the MinIO credentials, so it lives in a private temp directory and is deleted at the end:

```sh
tmp=$(mktemp -d); mkdir "$tmp/ui"; echo '<html>runbook</html>' > "$tmp/ui/index.html"
sed 's|^static_dir=""$|static_dir="/ui"|' deploy/config.compose.toml > "$tmp/config.toml"
cat > "$tmp/override.yml" <<EOF
services:
  center:
    volumes:
      - $tmp/config.toml:/config/config.toml:ro
      - $tmp/ui:/ui:ro
EOF
DCU=(docker compose -p ragpoc-p3browser --profile app -f docker-compose.yml -f "$tmp/override.yml")
"${DCU[@]}" up -d --wait --no-deps center
curl -s -i localhost:8080/ | tr -d '\r' | grep -iE '^HTTP|^cache-control|runbook'    # 200, no-cache, the file
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/documents                   # 200: the API still wins
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/                    # 405
"${DC[@]}" up -d --wait --no-deps center    # back to the committed config
rm -rf "$tmp"
```

## Teardown

```sh
"${DC[@]}" down -v
rm -rf ./qdrant_data ./ingestion_docs   # only when run from a worktree: these are its bind mounts
```

## Results

| Date | Check | Result |
|---|---|---|
| 2026-11-18 | 1. Host PUT | PASS: `upload_url` on `http://localhost:9000/upload-bucket/<id>`, PUT 200, confirm 202, `INDEXED` after 27 s (real ingest, host Ollama) |
| 2026-11-18 | 2. Preflight | PASS: `Access-Control-Allow-Origin: http://localhost:8080` for that origin; none for `http://evil.example` (MinIO `RELEASE.2025-09-07T16-13-09Z`) |
| 2026-11-18 | 3. Document list | PASS: one `INDEXED` entry, `upload_ids` = [id], no `hash` key |
| 2026-11-18 | 4. No UI | PASS: `GET /` 404 |
| 2026-11-18 | 5. UI | PASS: `GET /` 200 with `Cache-Control: no-cache` and the file; `GET /documents` 200; `POST /` 405; `GET /index.html` 301 to `./` (FileServer); the center logs `serving the web UI from /ui`; after the revert `GET /` is 404 again |
