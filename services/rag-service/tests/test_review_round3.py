"""Review round 3: messages that cannot fail themselves, a pinned integer digit limit, a client
cache that never holds a closed client, and strings that can be stored and read back."""

import json
import sys
from pathlib import Path

import pytest

from rag.core import qdrant_manager
from rag.transport.status_event import IngestEvent, InvalidEventError, StatusEvent

HASH = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
FIXTURE_TEXT = (Path(__file__).resolve().parents[3] / "contracts" / "events" / "fixtures" / "ingest_event.json").read_text()


@pytest.fixture
def digit_limit():
    """Pin Python's integer-to-string limit. With the limit raised or disabled (as
    PYTHONINTMAXSTRDIGITS=0 does) a huge integer parses normally and the tests would pass for a
    different reason than the one they name."""
    old = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(4300)
    yield
    sys.set_int_max_str_digits(old)


# ---- the error message must not be able to fail itself

def test_a_huge_int_is_reported_not_crashed_on(digit_limit):
    # repr(10**5000) raises ValueError under the digit limit; that must not replace the real error.
    with pytest.raises(InvalidEventError, match="chunk_count"):
        StatusEvent.indexed(HASH, 0, chunk_count=10**5000)
    with pytest.raises(InvalidEventError, match="attempts"):
        StatusEvent.indexed(HASH, 10**5000)


def test_an_object_whose_repr_raises_is_reported_not_crashed_on():
    class Hostile:
        def __repr__(self):
            raise RuntimeError("boom")

    with pytest.raises(InvalidEventError, match="attempts"):
        StatusEvent.indexed(HASH, Hostile())


def test_a_huge_string_is_clipped_without_building_its_whole_repr():
    with pytest.raises(InvalidEventError) as excinfo:
        StatusEvent(hash=HASH, status="x" * 5_000_000, attempts=0)
    assert len(str(excinfo.value)) < 300


# ---- huge integers: only the digit limit can be the reason for the error

def with_number(field: str, digits: str) -> bytes:
    """The whole valid fixture with one number swapped, so nothing but the number is wrong."""
    document = json.loads(FIXTURE_TEXT)
    assert field in document
    body = FIXTURE_TEXT.replace(f'"{field}":{document[field]}', f'"{field}":{digits}', 1)
    assert digits in body
    return body.encode()


@pytest.mark.parametrize(
    "field,digits",
    [("size", "1" * 5000), ("attempts", "9" * 4301), ("schema_version", "-" + "7" * 5000)],
)
def test_only_the_digit_limit_makes_a_huge_integer_fail(digit_limit, field, digits):
    # Sanity: with a sane number the same payload is valid, so the digit limit is the only cause.
    assert IngestEvent.parse(FIXTURE_TEXT)
    with pytest.raises(InvalidEventError, match="not valid JSON"):
        IngestEvent.parse(with_number(field, digits))


# ---- a closed client must not stay in the cache

def test_close_client_empties_the_cache_so_the_next_client_is_fresh(rag_config):
    first = qdrant_manager.get_client()
    qdrant_manager.close_client()
    assert qdrant_manager.get_client.cache_info().currsize == 0
    second = qdrant_manager.get_client()
    try:
        assert second is not first
    finally:
        second.close()


def test_close_client_is_safe_to_call_twice(rag_config):
    qdrant_manager.get_client()
    qdrant_manager.close_client()
    qdrant_manager.close_client()
    assert qdrant_manager.get_client.cache_info().currsize == 0


# ---- strings that cannot be written to SQLite or read back identically are refused

@pytest.mark.parametrize("field", ["id", "hash", "filename", "bucket_path"])
@pytest.mark.parametrize("surrogate", ["\\udfff", "\\ud800", "ok\\udc00ok"])
def test_a_lone_surrogate_is_refused(field, surrogate):
    # Schema-valid JSON, but not encodable as UTF-8: SQLite would raise on write, and Go reads it
    # back as U+FFFD, so the hash would not match across services.
    document = json.loads(FIXTURE_TEXT)
    text = FIXTURE_TEXT.replace(f'"{field}":"{document[field]}"', f'"{field}":"{surrogate}"', 1)
    assert surrogate in text
    with pytest.raises(InvalidEventError, match=field):
        IngestEvent.parse(text)


@pytest.mark.parametrize("filename", ["résumé.pdf", "文件.txt", "emoji-\U0001f600.md", "tab\there.txt"])
def test_ordinary_unicode_is_still_accepted(filename):
    document = json.loads(FIXTURE_TEXT) | {"filename": filename}
    assert IngestEvent.parse(json.dumps(document)).filename == filename
    assert IngestEvent.parse(json.dumps(document, ensure_ascii=False)).filename == filename
