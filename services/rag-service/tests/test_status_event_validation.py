"""Review round 1: the event models validate at construction, not only through their factories,
and stay inside the bounds the Go decoder and the schema enforce."""

import json
from datetime import UTC, datetime

import pytest

from rag.transport.status_event import IngestEvent, InvalidEventError, Reason, Status, StatusEvent

HASH = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
NOW = datetime(2026, 10, 14, 10, 0, 0, tzinfo=UTC)


def go_ingest() -> str:
    from pathlib import Path

    return (Path(__file__).resolve().parents[3] / "contracts" / "events" / "fixtures" / "ingest_event.json").read_text()


# ---- chunk_count has the same int32 bound as the Go decoder and the schema

@pytest.mark.parametrize("count", [2**31, 2**40])
def test_chunk_count_above_the_schema_maximum_is_rejected(count):
    with pytest.raises(InvalidEventError, match="chunk_count"):
        StatusEvent.indexed(HASH, 0, chunk_count=count)


def test_chunk_count_at_the_maximum_is_accepted():
    assert json.loads(StatusEvent.indexed(HASH, 0, chunk_count=2**31 - 1).to_json())["chunk_count"] == 2**31 - 1


# ---- the plain constructor is as strict as the factories

def test_a_plain_string_status_is_coerced_to_the_enum():
    event = StatusEvent(hash=HASH, status="INDEXED", attempts=0)
    assert event.status is Status.INDEXED


def test_a_plain_string_failed_status_still_gets_the_failed_checks():
    # Before: the FAILED checks were skipped (a str is not Status.FAILED) and to_json() crashed.
    with pytest.raises(InvalidEventError, match="reason"):
        StatusEvent(hash=HASH, status="FAILED", attempts=1, retryable=True)
    with pytest.raises(InvalidEventError, match="retryable"):
        StatusEvent(hash=HASH, status="FAILED", attempts=1, reason="INTERNAL")


def test_an_unknown_status_is_rejected():
    with pytest.raises(InvalidEventError, match="status"):
        StatusEvent(hash=HASH, status="BOGUS", attempts=0)


def test_a_plain_string_reason_is_coerced_and_serialises():
    event = StatusEvent(hash=HASH, status=Status.FAILED, attempts=0, retryable=False, reason="CORRUPT_FILE")
    assert event.reason is Reason.CORRUPT_FILE
    assert json.loads(event.to_json())["reason"] == "CORRUPT_FILE"


def test_an_unknown_reason_is_rejected_by_the_constructor():
    with pytest.raises(InvalidEventError, match="reason"):
        StatusEvent(hash=HASH, status=Status.FAILED, attempts=0, retryable=True, reason="OUT_OF_COFFEE")


@pytest.mark.parametrize("retryable", ["yes", 1, 0])
def test_retryable_must_be_a_real_boolean_on_every_status(retryable):
    with pytest.raises(InvalidEventError, match="retryable"):
        StatusEvent(hash=HASH, status=Status.INDEXED, attempts=0, retryable=retryable)


def test_the_detail_is_clipped_by_the_constructor_too():
    event = StatusEvent(hash=HASH, status=Status.FAILED, attempts=0, retryable=True, reason=Reason.INTERNAL, detail="x" * 5000)
    assert len(event.detail) == 500


def test_the_detail_must_be_text():
    with pytest.raises(InvalidEventError, match="detail"):
        StatusEvent(hash=HASH, status=Status.FAILED, attempts=0, retryable=True, reason=Reason.INTERNAL, detail=12345)


@pytest.mark.parametrize("created_at", ["2026", 1_700_000_000, None])
def test_created_at_must_be_a_datetime(created_at):
    with pytest.raises(InvalidEventError, match="created_at"):
        StatusEvent(hash=HASH, status=Status.INDEXED, attempts=0, created_at=created_at)


@pytest.mark.parametrize("event_id", ["", None, 7])
def test_id_must_be_a_non_empty_string(event_id):
    # The schema says minLength 1; an empty id would be a contract violation Go cannot see.
    with pytest.raises(InvalidEventError, match="id"):
        StatusEvent(hash=HASH, status=Status.INDEXED, attempts=0, id=event_id, created_at=NOW)


@pytest.mark.parametrize("bad_hash", [None, 5, ""])
def test_hash_must_be_a_non_empty_string(bad_hash):
    with pytest.raises(InvalidEventError, match="hash"):
        StatusEvent(hash=bad_hash, status=Status.INDEXED, attempts=0)


# ---- parsing hostile input never escapes as a non-contract exception

def test_deeply_nested_json_is_an_invalid_event_not_a_recursion_error():
    # RecursionError is not a ValueError. The consumer must be able to log and acknowledge it.
    with pytest.raises(InvalidEventError):
        IngestEvent.parse(b'{"a":' * 100000)


def test_deeply_nested_arrays_too():
    with pytest.raises(InvalidEventError):
        IngestEvent.parse(b"[" * 100000)


def test_parse_only_ever_raises_invalid_event_error_for_garbage():
    for raw in (b"\x00", b"\xff\xfe\x00", b"{" * 5, b'{"a":NaN}', b"1e999999", b"-", b'{"hash":"' + b"x" * 2_000_000):
        try:
            IngestEvent.parse(raw)
        except InvalidEventError:
            continue
        pytest.fail(f"{raw[:20]!r} was accepted")


def test_a_valid_go_event_still_parses():
    assert IngestEvent.parse(go_ingest()).attempts == 0
