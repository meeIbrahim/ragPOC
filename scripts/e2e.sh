#!/usr/bin/env bash
# Compose e2e for Phase 0: the real Go center service and the real Python consumer (stub mode) over
# real MinIO, Redis, Qdrant and SQLite, in an isolated compose project.
#
#   make e2e                  all scenarios
#   scripts/e2e.sh crash      only the named scenarios (by test name, see SCENARIOS)
#
# Each scenario needs the stub consumer in a particular mode, so this script re-creates the
# consumer container with that mode and then runs just that subtest of services/center/e2e.
# The project name `ragpoc-e2e` and docker-compose.e2e.yml keep it apart from the dev stack:
# named volumes, no published ports. `down -v` at the end removes every container and volume.
set -euo pipefail
cd "$(dirname "$0")/.."

DC=(docker compose -p ragpoc-e2e -f docker-compose.yml -f docker-compose.e2e.yml)

# "<stub mode> <subtest>"; the order is the order they run in.
SCENARIOS=(
  "indexed indexed"
  "failed-retryable failed_retryable"
  "failed-permanent failed_permanent"
  "slow slow"
  "crash crash"
  "hang hang"
  "indexed unsupported_ext"
  "indexed consumer_cleanup"
)
# crash and hang must restart the consumer container at least this many times (3 starts die, the
# 4th reports the poison entry).
MIN_RESTARTS=3

cleanup() { "${DC[@]}" --profile e2e down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

wanted() { # is the subtest selected on the command line?
  [ "$#" -eq 1 ] && return 0
  local sub=$1; shift
  local arg
  for arg in "$@"; do [ "$arg" = "$sub" ] && return 0; done
  return 1
}

restarts() { docker inspect -f '{{.RestartCount}}' "$("${DC[@]}" ps -q rag-consumer)"; }

echo "== building and starting the stack (project ragpoc-e2e)"
"${DC[@]}" up -d --build center rag-consumer

results=()
failed=0
for entry in "${SCENARIOS[@]}"; do
  read -r mode sub <<<"$entry"
  wanted "$sub" "$@" || continue

  echo
  echo "== scenario: $sub (stub mode: $mode)"
  RAG_STUB_MODE="$mode" "${DC[@]}" up -d --force-recreate --no-deps rag-consumer
  start=$(date +%s)

  status=PASS
  if ! "${DC[@]}" run --build --rm e2e -test.run "^TestPhase0\$/^${sub}\$" -test.v -test.timeout 8m; then
    status=FAIL
  elif [ "$sub" = crash ] || [ "$sub" = hang ]; then
    count=$(restarts)
    echo "   consumer container restarts: $count"
    if [ "$count" -lt "$MIN_RESTARTS" ]; then
      echo "   expected at least $MIN_RESTARTS restarts" >&2
      status=FAIL
    fi
  fi

  if [ "$status" = FAIL ]; then
    failed=1
    echo "---- last log lines ----" >&2
    "${DC[@]}" logs --tail 50 center rag-consumer >&2 || true
  fi
  results+=("$(printf '%-6s %-18s %4ss' "$status" "$sub" "$(( $(date +%s) - start ))")")
done

echo
echo "== summary"
printf '   %s\n' "${results[@]}"
if [ "$failed" -ne 0 ]; then
  echo "e2e FAILED" >&2
  exit 1
fi
echo "e2e passed"
