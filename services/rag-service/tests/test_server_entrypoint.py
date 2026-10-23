"""R7 `python -m rag.server`: startup checks and exit codes (spec A7, E12, E15, E22)."""

import logging

import pytest

from fakes import FakeEmbedder, FakeIndex
from rag import server as entry
from rag.ports import DependencyUnavailable


class FakeServer:
    def __init__(self, interrupt=False):
        self.interrupt = interrupt
        self.waited = False
        self.stopped = False

    def wait_for_termination(self):
        self.waited = True
        if self.interrupt:
            raise KeyboardInterrupt

    def stop(self, grace):
        self.stopped = True


@pytest.fixture
def parts(rag_config, monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key-not-used")
    monkeypatch.setattr(entry.logging_setup, "configure", lambda *a, **k: None)
    state = {"embedder": FakeEmbedder(dim=4096), "index": FakeIndex(), "served": [], "server": FakeServer()}
    monkeypatch.setattr(entry.wiring, "embedder", lambda config: state["embedder"])
    monkeypatch.setattr(entry.wiring, "index", lambda config: state["index"])
    monkeypatch.setattr(entry.wiring, "query_service", lambda config, llm: ("query-service", llm))

    def fake_serve(service, port, max_workers):
        state["served"].append((service, port, max_workers))
        return state["server"], port

    monkeypatch.setattr(entry, "serve", fake_serve)
    return state


def test_starts_the_server_on_the_configured_port(parts):
    assert entry.main([]) == 0
    ((service, llm), port, workers), = parts["served"]
    assert (service, port, workers) == ("query-service", 50051, 8)
    assert type(llm).__name__ == "GroqLLM"
    assert parts["server"].waited


def test_missing_groq_key_exits_2_before_serving_E15(parts, monkeypatch, caplog):
    monkeypatch.delenv("GROQ_API_KEY")
    with caplog.at_level(logging.ERROR):
        assert entry.main([]) == 2
    assert "GROQ_API_KEY" in caplog.text
    assert parts["served"] == []


def test_a_wrong_embedding_dim_exits_2_E12(parts, caplog):
    parts["embedder"].dim = 1024
    with caplog.at_level(logging.ERROR):
        assert entry.main([]) == 2
    assert "1024" in caplog.text
    assert parts["served"] == []


def test_a_missing_collection_exits_2_E22(parts, caplog):
    parts["index"].exists = False
    with caplog.at_level(logging.ERROR):
        assert entry.main([]) == 2
    assert "make run_migrations" in caplog.text


def test_an_unreachable_dependency_exits_1(parts):
    parts["index"].error = DependencyUnavailable("qdrant", "ConnectError")
    assert entry.main([]) == 1
    assert parts["served"] == []


def test_an_invalid_config_exits_2(tmp_path, monkeypatch):
    monkeypatch.setattr(entry.logging_setup, "configure", lambda *a, **k: None)
    bad = tmp_path / "bad.toml"
    bad.write_text("[rag\n")
    assert entry.main(["--config", str(bad)]) == 2


def test_ctrl_c_stops_the_server_and_exits_130(parts):
    parts["server"] = FakeServer(interrupt=True)
    assert entry.main([]) == 130
    assert parts["server"].stopped
