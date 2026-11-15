#!/usr/bin/env bash
# Runs a command with MINIO_URL, MINIO_ACCESS_KEY and MINIO_SECRET_KEY pointing at a throwaway
# MinIO, then removes it, pass or fail:
#
#   scripts/with-throwaway-minio.sh uv run pytest -m integration
#
# The container uses the image pinned in docker-compose.yml, a random loopback port, and random
# credentials made for this run only, so no credential is stored anywhere and the dev MinIO is
# never touched. If MINIO_URL is already set, the command runs against that MinIO instead.
set -euo pipefail

if [ -n "${MINIO_URL:-}" ]; then
  exec "$@"
fi

cd "$(dirname "$0")/.."
image=$(awk '/^  minio:/ { in_minio = 1 } in_minio && /image:/ { print $2; exit }' docker-compose.yml)
if [ -z "$image" ]; then
  echo "with-throwaway-minio: no minio image found in docker-compose.yml" >&2
  exit 1
fi
cd - >/dev/null

name="ragpoc-it-minio-$$"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# Passed by name (`-e VAR`), so the values never appear in the docker command line.
MINIO_ROOT_USER="it-$(openssl rand -hex 6)"
MINIO_ROOT_PASSWORD="$(openssl rand -hex 16)"
export MINIO_ROOT_USER MINIO_ROOT_PASSWORD
docker run -d --rm --name "$name" -p 127.0.0.1::9000 \
  -e MINIO_ROOT_USER -e MINIO_ROOT_PASSWORD "$image" server /data >/dev/null

addr=$(docker port "$name" 9000/tcp | head -n 1)
ready=0
for _ in $(seq 1 30); do
  if curl -fs -o /dev/null "http://$addr/minio/health/ready"; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "with-throwaway-minio: MinIO at $addr not ready after 30s" >&2
  docker logs "$name" >&2 || true
  exit 1
fi

export MINIO_URL="$addr" MINIO_ACCESS_KEY="$MINIO_ROOT_USER" MINIO_SECRET_KEY="$MINIO_ROOT_PASSWORD"
unset MINIO_ROOT_USER MINIO_ROOT_PASSWORD
"$@"
