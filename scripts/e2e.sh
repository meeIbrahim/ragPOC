#!/usr/bin/env bash
# Compose e2e for Phase 0: the real Go center service and the real Python consumer (stub mode) over
# real MinIO, Redis, Qdrant and SQLite, in an isolated compose project.
#
#   make e2e                  all scenarios
#   scripts/e2e.sh crash      only the named scenarios (by test name, see SCENARIOS)
#   make e2e-faults           the failure-injection scenarios instead (FAULT_SCENARIOS)
#   scripts/e2e.sh --faults redis_outage_short
#
# Each scenario needs the stub consumer in a particular mode, so this script re-creates the
# consumer container with that mode and then runs just that subtest of services/center/e2e.
# The project name `ragpoc-e2e` and docker-compose.e2e.yml keep it apart from the dev stack:
# named volumes, no published ports. `down -v` at the end removes every container and volume.
set -euo pipefail
cd "$(dirname "$0")/.."

MODE=phase0
if [ "${1:-}" = "--faults" ]; then
  MODE=faults
  shift
fi

# Per-run image tags: another checkout building the dev images must not change what this run tests.
export IMAGE_TAG=e2e
DC=(docker compose -p ragpoc-e2e -f docker-compose.yml -f docker-compose.e2e.yml --profile app --profile e2e)

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
# crash and hang must restart the consumer container exactly this many times: 3 starts die, the
# 4th reports the poison entry and keeps running.
WANT_RESTARTS=3

# Failure-injection scenarios (Phase 2, `--faults`), each a TestPhase2 subtest or an
# _arrange/_verify pair with an outside action between them (see run_fault). Kept out of the
# default run, so a timing flake here never turns `make e2e` red.
FAULT_SCENARIOS=(
  duplicate_upload_concurrent
  redis_outage_short
  redis_outage_long
  hash_kill_mid_claim
  outbox_kill_after_publish
)

cleanup() { "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true; }

wanted() { # is the subtest selected on the command line?
  [ "$#" -eq 1 ] && return 0
  local sub=$1; shift
  local arg
  for arg in "$@"; do [ "$arg" = "$sub" ] && return 0; done
  return 1
}

# The scenario names of the selected mode.
if [ "$MODE" = faults ]; then
  KNOWN=("${FAULT_SCENARIOS[@]}")
else
  KNOWN=("${SCENARIOS[@]#* }")
fi

# A typo must fail loudly: an empty selection would otherwise print "e2e passed" having run nothing.
for arg in "$@"; do
  known=0
  for name in "${KNOWN[@]}"; do
    [ "$name" = "$arg" ] && known=1
  done
  if [ "$known" -eq 0 ]; then
    echo "unknown $MODE scenario: $arg" >&2
    echo "known scenarios:$(printf ' %s' "${KNOWN[@]}")" >&2
    exit 2
  fi
done

restarts() { # restarts <service>
  local id
  id=$("${DC[@]}" ps -q "${1:-rag-consumer}")
  [ -n "$id" ] && docker inspect -f '{{.RestartCount}}' "$id"
}

# ---- fault mode helpers

RUN_ID="$(date +%s)-$$"

step() { # step <TestPhase2 subtest>
  echo "   -- $1"
  E2E_RUN_ID="$RUN_ID" "${DC[@]}" run --no-deps --rm e2e -test.run "^TestPhase2\$/^$1\$" -test.v -test.timeout 5m
}

# recreate_center <fault>: a fresh center with CENTER_FAULT set for this one command only (never
# exported, so no later `up` inherits it). Only while Redis is up: the center pings Redis at
# startup and exits without it.
recreate_center() {
  CENTER_FAULT="$1" "${DC[@]}" up -d --force-recreate --no-deps --wait center
}

redis_up() { "${DC[@]}" up -d --no-deps --wait redis; } # `start` has no health wait

# A center re-created without the fault must not have logged the fault WARN (edge 10).
center_fault_free() {
  if "${DC[@]}" logs center 2>&1 | grep -q 'fault hook enabled'; then
    echo "   center still runs a fault hook after re-create" >&2
    return 1
  fi
}

# run_fault <scenario>: the steps and outside actions of one fault scenario. Any failed step
# returns non-zero; the caller restores Redis and the center either way.
run_fault() {
  case "$1" in
    duplicate_upload_concurrent)
      step duplicate_upload_concurrent ;;
    redis_outage_short)
      "${DC[@]}" stop redis &&
        step redis_outage_short_arrange &&
        redis_up &&
        step redis_outage_short_verify ;;
    redis_outage_long)
      # Never re-create the center here: a new container resets RestartCount, which is what
      # proves the center survived the outage (A3).
      local before after
      before=$(restarts center) &&
        "${DC[@]}" stop redis &&
        step redis_outage_long_arrange &&
        redis_up &&
        step redis_outage_long_verify &&
        after=$(restarts center) || return 1
      echo "   center restarts during the outage: $before -> $after"
      [ "$before" = "$after" ] || { echo "   the center restarted while Redis was down" >&2; return 1; }
      ;;
    hash_kill_mid_claim)
      kill_scenario hash-hang hash_kill_mid_claim ;;
    outbox_kill_after_publish)
      kill_scenario outbox-hang-after-publish outbox_kill_after_publish ;;
  esac
}

# kill_scenario <fault> <scenario>: arrange against a hooked center, SIGKILL it (SIGTERM would
# let the worker release its claim), then verify against a fresh, fault-free center.
kill_scenario() {
  recreate_center "$1" &&
    step "$2_arrange" &&
    "${DC[@]}" kill -s SIGKILL center &&
    recreate_center "" &&
    center_fault_free &&
    step "$2_verify"
}

# restore_after_fault: whatever a scenario left behind, the next one starts with Redis up and a
# fault-free center.
restore_after_fault() {
  redis_up >/dev/null 2>&1 || echo "   could not bring Redis back" >&2
  if "${DC[@]}" logs center 2>&1 | grep -q 'fault hook enabled' || [ -z "$("${DC[@]}" ps -q --status running center)" ]; then
    recreate_center "" >/dev/null 2>&1 || echo "   could not re-create the center" >&2
  fi
}

# Only now: a typo above must not tear down a concurrent e2e run.
trap cleanup EXIT

# Start from nothing: leftovers of an aborted run (old volumes, a consumer in another mode) would
# make the first scenario lie.
cleanup

echo "== building and starting the stack (project ragpoc-e2e)"
# One-shot migrations run once here; the per-scenario `run` below uses --no-deps so it does not
# run them again.
if ! "${DC[@]}" up -d --build --wait center rag-consumer; then
  echo "stack failed to start" >&2
  "${DC[@]}" logs --tail 50 >&2 || true
  exit 1
fi

"${DC[@]}" build e2e

results=()
failed=0

if [ "$MODE" = faults ]; then
  for sub in "${FAULT_SCENARIOS[@]}"; do
    wanted "$sub" "$@" || continue
    echo
    echo "== fault scenario: $sub (run $RUN_ID)"
    RAG_STUB_MODE=indexed "${DC[@]}" up -d --force-recreate --no-deps rag-consumer
    start=$(date +%s)
    status=PASS
    run_fault "$sub" || status=FAIL
    if [ "$status" = FAIL ]; then
      failed=1
      echo "---- last log lines ----" >&2
      "${DC[@]}" logs --tail 50 center rag-consumer >&2 || true
    fi
    restore_after_fault
    results+=("$(printf '%-6s %-28s %4ss' "$status" "$sub" "$(( $(date +%s) - start ))")")
  done
  # skip the Phase 0 loop
  SCENARIOS=()
fi

for entry in "${SCENARIOS[@]+"${SCENARIOS[@]}"}"; do
  read -r mode sub <<<"$entry"
  wanted "$sub" "$@" || continue

  echo
  echo "== scenario: $sub (stub mode: $mode)"
  RAG_STUB_MODE="$mode" "${DC[@]}" up -d --force-recreate --no-deps rag-consumer
  start=$(date +%s)

  status=PASS
  if ! "${DC[@]}" run --no-deps --rm e2e -test.run "^TestPhase0\$/^${sub}\$" -test.v -test.timeout 8m; then
    status=FAIL
  elif [ "$sub" = crash ] || [ "$sub" = hang ]; then
    count=$(restarts) || count=unknown
    echo "   consumer container restarts: $count"
    if [ "$count" != "$WANT_RESTARTS" ]; then
      echo "   expected exactly $WANT_RESTARTS restarts" >&2
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
if [ "${#results[@]}" -eq 0 ]; then
  echo "no scenario ran" >&2
  exit 1
fi
printf '   %s\n' "${results[@]}"
if [ "$failed" -ne 0 ]; then
  echo "e2e ($MODE) FAILED" >&2
  exit 1
fi
echo "e2e ($MODE) passed"
