#!/usr/bin/env bash
# Deletes all demo data of THIS checkout's stack (compose-demo A8): the compose project's named
# volumes and the bind-mounted data folders. FAILED rows can't be deleted any other way (PRD B1,
# B3), so a demo that needs a clean list starts here. Nothing happens unless you type "reset".
# Then: GROQ_API_KEY="$GROQ" make stack.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
[[ -f "$ROOT/docker-compose.yml" ]] || {
  echo "no docker-compose.yml in $ROOT; refusing to run" >&2
  exit 1
}
PROJECT="${COMPOSE_PROJECT_NAME:-$(basename "$ROOT" | tr 'A-Z' 'a-z' | tr -cd 'a-z0-9_-' | sed 's/^[^a-z0-9]*//')}"
[[ -n "$PROJECT" ]] || {
  echo "empty compose project name; refusing to run" >&2
  exit 1
}

# Folders compose bind-mounts relative to docker-compose.yml (MinIO data, Qdrant storage).
FOLDERS=()
for rel in ingestion_docs/data qdrant_data; do
  dir="$ROOT/$rel"
  if [[ -L "$dir" ]]; then
    echo "$dir is a symlink; refusing to delete through it" >&2
    exit 1
  fi
  [[ -d "$dir" ]] || continue
  real="$(cd "$dir" && pwd -P)"
  case "$real" in
  "$ROOT"/*) FOLDERS+=("$real") ;;
  *)
    echo "$real is not inside $ROOT; refusing to run" >&2
    exit 1
    ;;
  esac
done

VOLUMES="$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT")"

echo "Compose project: $PROJECT"
echo "Checkout:        $ROOT"
echo "This stops the stack and deletes:"
echo "  named volumes:"
if [[ -n "$VOLUMES" ]]; then
  sed 's/^/    /' <<<"$VOLUMES"
else
  echo "    (none)"
fi
echo "  everything inside these folders:"
if ((${#FOLDERS[@]} > 0)); then
  printf '    %s\n' "${FOLDERS[@]}"
else
  echo "    (none)"
fi
echo "On Linux the folder contents may be root-owned; then the delete fails part-way and needs sudo."

read -r -p 'Type reset to delete all of the above: ' answer || answer=""
if [[ "$answer" != "reset" ]]; then
  echo "Aborted. Nothing was deleted."
  exit 1
fi

docker compose -p "$PROJECT" -f "$ROOT/docker-compose.yml" --profile app down -v
for dir in ${FOLDERS[@]+"${FOLDERS[@]}"}; do
  # -mindepth 1 keeps the folder (compose mounts it) and removes hidden files like .minio.sys.
  find "$dir" -mindepth 1 -delete
done
echo "Reset done. Start a clean stack with: GROQ_API_KEY=\"\$GROQ\" make stack"
