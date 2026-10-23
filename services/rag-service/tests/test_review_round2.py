"""Review round 2: hostile-input handling that must never escape as a non-contract exception,
bounded error messages, timezone handling, and entry-point cleanup."""

import json
import logging
import sqlite3
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from pathlib import Path
from types import SimpleNamespace

import pytest
from qdrant_client.models import Distance, MultiVectorComparator, MultiVectorConfig, VectorParams

from migration import main as migration_main
from migration.qdrant_collection_v1 import QdrantCollection
from rag.core import config_reader, qdrant_manager
from rag.core.config_reader import ConfigError
from rag.db import sqlite_manager
from rag.db.sqlite_manager import SQLiteManager
from rag.transport.status_event import IngestEvent, InvalidEventError, Status, StatusEvent

HASH = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
FIXTURE = Path(__file__).resolve().parents[3] / "contracts" / "events" / "fixtures" / "ingest_event.json"


def ingest_with(**changes) -> str:
    return json.dumps(json.loads(FIXTURE.read_text()) | changes)


# ---- json.loads can raise a plain ValueError (the int digit limit), not only JSONDecodeError

# ---- error messages never echo an unbounded untrusted value

@pytest.mark.parametrize("field", ["created_at", "schema_version", "size", "attempts"])
def test_error_messages_are_bounded(field):
    huge = "x" * 1_000_000
    with pytest.raises(InvalidEventError) as excinfo:
        IngestEvent.parse(ingest_with(**{field: huge}))
    assert len(str(excinfo.value)) < 300


def test_status_event_error_messages_are_bounded():
    with pytest.raises(InvalidEventError) as excinfo:
        StatusEvent(hash=HASH, status="x" * 1_000_000, attempts=0)
    assert len(str(excinfo.value)) < 300


# ---- created_at: aware means utcoffset() is not None, and conversion happens once

class NoOffset(tzinfo):
    """tzinfo is set, but utcoffset() is None: Python treats such a datetime as naive."""

    def utcoffset(self, dt):
        return None

    def dst(self, dt):
        return None

    def tzname(self, dt):
        return "none"


class Broken(tzinfo):
    def utcoffset(self, dt):
        raise RuntimeError("tz database unavailable")

    def dst(self, dt):
        return None


def test_a_tzinfo_without_an_offset_is_rejected_not_read_as_local_time():
    with pytest.raises(InvalidEventError, match="created_at"):
        StatusEvent.indexed(HASH, 0, created_at=datetime(2024, 1, 1, tzinfo=NoOffset()))


def test_a_tzinfo_that_raises_is_an_invalid_event():
    with pytest.raises(InvalidEventError, match="created_at"):
        StatusEvent.indexed(HASH, 0, created_at=datetime(2024, 1, 1, tzinfo=Broken()))


@pytest.mark.parametrize(
    "moment",
    [
        datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=1))),
        datetime(9999, 12, 31, 23, tzinfo=timezone(timedelta(hours=-1))),
    ],
    ids=["year-1-plus-1h", "year-9999-minus-1h"],
)
def test_a_timestamp_that_overflows_utc_is_rejected_by_the_constructor(moment):
    # Before: the constructor accepted it and to_json() raised OverflowError.
    with pytest.raises(InvalidEventError, match="created_at"):
        StatusEvent.indexed(HASH, 0, created_at=moment)


def test_created_at_is_stored_in_utc():
    local = datetime(2026, 10, 14, 15, 0, 0, tzinfo=timezone(timedelta(hours=5)))
    event = StatusEvent.indexed(HASH, 0, created_at=local)
    assert event.created_at == datetime(2026, 10, 14, 10, 0, 0, tzinfo=UTC)
    assert event.created_at.utcoffset() == timedelta(0)


def test_every_accepted_event_serialises():
    # The constructor is the only gate: nothing that passes it may fail later in to_json().
    for created_at in (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, 12, tzinfo=timezone(timedelta(hours=-5)))):
        assert json.loads(StatusEvent.indexed(HASH, 0, created_at=created_at).to_json())["status"] == Status.INDEXED


# ---- migration entry point

class Ok:
    desc = "ok"

    def run(self):
        pass


@pytest.fixture
def keep_pytest_log_capture(monkeypatch):
    monkeypatch.setattr(migration_main.logging_setup, "configure", lambda *a, **k: None)


def test_main_does_not_build_a_client_just_to_close_it(rag_config, monkeypatch, keep_pytest_log_capture):
    monkeypatch.setattr(migration_main, "MIGRATIONS", [Ok, Ok, Ok])
    assert migration_main.main() == 0
    assert qdrant_manager.get_client.cache_info().currsize == 0


def test_an_unbuildable_qdrant_url_exits_1_with_one_clean_failure(tmp_path, monkeypatch, caplog, keep_pytest_log_capture):
    from support import VALID_TOML

    path = tmp_path / "bad.toml"
    path.write_text(VALID_TOML.replace('dir="./sqlite_db"', f'dir="{tmp_path / "db"}"').replace("http://localhost:6333", "::::not a url"))
    monkeypatch.setenv("RAG_CONFIG", str(path))
    config_reader.get_config.cache_clear()
    qdrant_manager.get_client.cache_clear()
    try:
        with caplog.at_level(logging.ERROR):
            code = migration_main.main()  # must return, not raise a second traceback from the finally block
    finally:
        config_reader.get_config.cache_clear()
        qdrant_manager.get_client.cache_clear()
    assert code == 1
    assert "Migrations aborted" in caplog.text


def test_a_failing_close_does_not_change_the_exit_code(rag_config, monkeypatch, caplog, keep_pytest_log_capture):
    monkeypatch.setattr(migration_main, "MIGRATIONS", [Ok, Ok, Ok])

    def explode():
        raise OSError("socket already closed")

    monkeypatch.setattr(migration_main.qdrant_manager, "close_client", explode)
    with caplog.at_level(logging.WARNING):
        assert migration_main.main() == 0
    assert "socket already closed" in caplog.text


def test_close_client_closes_a_built_client_and_ignores_an_unbuilt_one(rag_config):
    qdrant_manager.close_client()  # nothing built: must not build one
    assert qdrant_manager.get_client.cache_info().currsize == 0
    client = qdrant_manager.get_client()
    closed = []
    client.close = lambda: closed.append(True)
    qdrant_manager.close_client()
    assert closed == [True]


# ---- qdrant migration: a multivector collection is as incompatible as a named one

def test_a_multivector_collection_is_refused(rag_config):
    vectors = VectorParams(
        size=4096, distance=Distance.COSINE, multivector_config=MultiVectorConfig(comparator=MultiVectorComparator.MAX_SIM)
    )

    class Fake:
        created = []

        def collection_exists(self, name):
            return True

        def get_collection(self, name):
            return SimpleNamespace(config=SimpleNamespace(params=SimpleNamespace(vectors=vectors)))

        def create_collection(self, **kwargs):
            self.created.append(kwargs)

    with pytest.raises(RuntimeError, match="multi"):
        QdrantCollection(Fake()).run()


# ---- sqlite manager: a failed setup must not leak the connection

def test_a_failing_pragma_closes_the_connection_and_leaves_the_manager_usable(tmp_path, monkeypatch):
    events = []

    class FakeConn:
        def execute(self, sql):
            raise sqlite3.OperationalError("database is locked")

        def close(self):
            events.append("closed")

    monkeypatch.setattr(sqlite_manager.sqlite3, "connect", lambda *a, **k: FakeConn())
    manager = SQLiteManager("c", str(tmp_path))
    with pytest.raises(sqlite3.OperationalError):
        manager.__enter__()
    assert events == ["closed"]
    assert manager._conn is None


# ---- default path hint

def test_a_missing_default_config_mentions_rag_config(monkeypatch, tmp_path):
    monkeypatch.delenv("RAG_CONFIG", raising=False)
    monkeypatch.setattr(config_reader, "default_path", lambda: tmp_path / "config.toml")
    with pytest.raises(ConfigError, match="RAG_CONFIG"):
        config_reader.load()


def test_an_explicit_missing_path_does_not_blame_the_default(tmp_path):
    with pytest.raises(ConfigError) as excinfo:
        config_reader.load(tmp_path / "nope.toml")
    assert "RAG_CONFIG" not in str(excinfo.value)
