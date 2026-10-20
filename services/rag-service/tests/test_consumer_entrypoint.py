"""Task 28: `python -m rag.consumer [--config] [--mode]` wires the pieces from the shared config."""

import logging
from datetime import timedelta

import pytest

from rag import consumer as entry
from rag.ingest.stub import Mode, StubProcessor
from rag.transport.stream_consumer import ConsumerSettings


class FakeStreamConsumer:
    instances = []

    def __init__(self, redis, settings, publisher, processor, watchdog, **kwargs):
        self.redis, self.settings, self.publisher, self.processor, self.watchdog = redis, settings, publisher, processor, watchdog
        self.name = "host/ingest/fake"
        self.ran = False
        self.raise_on_run = None
        FakeStreamConsumer.instances.append(self)

    def run(self):
        self.ran = True
        if self.raise_on_run:
            raise self.raise_on_run

    def stop(self):
        self.stopped = True


class FakeRedis:
    created = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        FakeRedis.created.append(self)


@pytest.fixture
def wired(rag_config, monkeypatch):
    FakeStreamConsumer.instances = []
    FakeRedis.created = []
    monkeypatch.setattr(entry, "StreamConsumer", FakeStreamConsumer)
    monkeypatch.setattr(entry, "Redis", FakeRedis)
    monkeypatch.setattr(entry.logging_setup, "configure", lambda *a, **k: None)  # keep pytest's log capture
    monkeypatch.delenv("RAG_STUB_MODE", raising=False)
    return rag_config


# ---- mode (PRD: required; invalid -> exit 2)

def test_a_valid_mode_flag_starts_the_consumer(wired):
    assert entry.main(["--mode", "indexed"]) == 0
    (consumer,) = FakeStreamConsumer.instances
    assert consumer.ran
    assert isinstance(consumer.processor, StubProcessor)


@pytest.mark.parametrize("mode", [m.value for m in Mode])
def test_every_documented_mode_is_accepted(wired, mode):
    assert entry.main(["--mode", mode]) == 0


def test_the_mode_defaults_to_the_environment(wired, monkeypatch):
    monkeypatch.setenv("RAG_STUB_MODE", "failed-permanent")
    assert entry.main([]) == 0
    assert FakeStreamConsumer.instances[0].processor._mode is Mode.FAILED_PERMANENT


def test_the_flag_wins_over_the_environment(wired, monkeypatch):
    monkeypatch.setenv("RAG_STUB_MODE", "crash")
    assert entry.main(["--mode", "indexed"]) == 0
    assert FakeStreamConsumer.instances[0].processor._mode is Mode.INDEXED


def test_a_missing_mode_is_a_usage_error(wired, caplog):
    with caplog.at_level(logging.ERROR):
        assert entry.main([]) == 2
    assert "RAG_STUB_MODE" in caplog.text
    assert FakeStreamConsumer.instances == []


@pytest.mark.parametrize("mode", ["explode", "", "INDEXED ", "failed_retryable"])
def test_an_invalid_mode_is_a_usage_error_that_lists_the_valid_ones(wired, caplog, mode):
    with caplog.at_level(logging.ERROR):
        assert entry.main(["--mode", mode]) == 2
    assert "failed-retryable" in caplog.text
    assert FakeStreamConsumer.instances == []


def test_an_invalid_mode_in_the_environment_is_also_exit_2(wired, monkeypatch):
    monkeypatch.setenv("RAG_STUB_MODE", "bogus")
    assert entry.main([]) == 2


# ---- config

def test_an_invalid_config_exits_2_before_connecting_to_anything(tmp_path, monkeypatch, caplog):
    FakeRedis.created = []
    monkeypatch.setattr(entry, "Redis", FakeRedis)
    monkeypatch.setattr(entry.logging_setup, "configure", lambda *a, **k: None)
    bad = tmp_path / "bad.toml"
    bad.write_text("[redis]\nport=0\n")
    with caplog.at_level(logging.ERROR):
        assert entry.main(["--mode", "indexed", "--config", str(bad)]) == 2
    assert "redis.port" in caplog.text
    assert FakeRedis.created == []


def test_the_config_flag_selects_the_file(wired, tmp_path, monkeypatch):
    other = tmp_path / "other.toml"
    other.write_text((tmp_path / "config.toml").read_text().replace('stream="ingest.docs"', 'stream="custom.stream"'))
    assert entry.main(["--mode", "indexed", "--config", str(other)]) == 0
    assert FakeStreamConsumer.instances[0].settings.stream == "custom.stream"


def test_unknown_config_keys_are_warned_about(wired, tmp_path, monkeypatch, caplog):
    path = tmp_path / "config.toml"
    path.write_text(path.read_text().replace("top_k=5", 'top_k=5\nprompt_template="x"'))
    with caplog.at_level(logging.WARNING):
        assert entry.main(["--mode", "indexed", "--config", str(path)]) == 0
    assert "rag.prompt_template" in caplog.text


# ---- wiring from the shared config

def test_the_redis_client_uses_the_config_and_bounded_timeouts(wired):
    entry.main(["--mode", "indexed"])
    kwargs = FakeRedis.created[0].kwargs
    assert (kwargs["host"], kwargs["port"]) == ("localhost", 6379)
    assert kwargs["decode_responses"] is True  # the consumer works with str, not bytes
    # The read timeout must outlast the longest BLOCK, or a quiet stream would look like a dead
    # connection.
    longest_block = min(
        ConsumerSettings.from_config(wired).read_block, wired.rag.consumer.recovery_interval
    ).total_seconds()
    assert kwargs["socket_timeout"] == wired.rag.call_timeout.total_seconds()
    assert kwargs["socket_timeout"] > longest_block
    assert kwargs["socket_connect_timeout"] == wired.rag.call_timeout.total_seconds()


def test_the_consumer_settings_come_from_the_config(wired):
    entry.main(["--mode", "indexed"])
    assert FakeStreamConsumer.instances[0].settings == ConsumerSettings.from_config(wired)


def test_the_watchdog_limit_is_job_timeout_plus_grace(wired):
    entry.main(["--mode", "indexed"])
    watchdog = FakeStreamConsumer.instances[0].watchdog
    expected = wired.rag.consumer.job_timeout + wired.rag.consumer.watchdog_grace
    assert watchdog._timeout == expected.total_seconds() == timedelta(minutes=17).total_seconds()


def test_the_publisher_targets_the_status_stream_with_the_shared_cap(wired):
    entry.main(["--mode", "indexed"])
    publisher = FakeStreamConsumer.instances[0].publisher
    assert publisher._stream == "ingest.status" and publisher._maxlen == wired.redis.stream_max_len


# ---- lifecycle

def test_ctrl_c_ends_the_process_cleanly(wired, monkeypatch):
    original = FakeStreamConsumer.run

    def interrupted(self):
        raise KeyboardInterrupt

    monkeypatch.setattr(FakeStreamConsumer, "run", interrupted)
    assert entry.main(["--mode", "indexed"]) == 130
    monkeypatch.setattr(FakeStreamConsumer, "run", original)


def test_the_module_is_runnable_as_a_script():
    import importlib.util

    spec = importlib.util.find_spec("rag.consumer")
    assert spec is not None
    assert "__main__" in open(spec.origin).read()


# ---- the Redis client is hardened (review round 1)

def test_undecodable_bytes_in_an_entry_become_replacement_characters_not_a_crash(wired):
    entry.main(["--mode", "indexed"])
    assert FakeRedis.created[0].kwargs["encoding_errors"] == "replace"


def test_the_wire_protocol_is_pinned_because_the_consumer_relies_on_its_reply_shapes(wired):
    entry.main(["--mode", "indexed"])
    assert FakeRedis.created[0].kwargs["protocol"] == 2


def test_redis_py_does_not_retry_on_its_own(wired):
    # Its default is 10 retries with backoff: one XADD could then take minutes, long after the
    # watchdog was disarmed, and a re-sent XAUTOCLAIM would bump a delivery count twice.
    entry.main(["--mode", "indexed"])
    retry = FakeRedis.created[0].kwargs["retry"]
    assert retry.get_retries() == 0


def test_the_budget_bound_follows_a_tiny_call_timeout_too(wired, tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(path.read_text().replace('call_timeout="60s"', 'call_timeout="1s"'))
    entry.main(["--mode", "indexed", "--config", str(path)])
    kwargs = FakeRedis.created[0].kwargs
    assert FakeStreamConsumer.instances[0].settings.call_timeout == kwargs["socket_connect_timeout"] + kwargs["socket_timeout"] == 1 + 7


def test_the_read_timeout_outlasts_the_block_even_for_a_tiny_call_timeout(wired, tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(path.read_text().replace('call_timeout="60s"', 'call_timeout="1s"'))
    assert entry.main(["--mode", "indexed", "--config", str(path)]) == 0
    timeout = FakeRedis.created[0].kwargs["socket_timeout"]
    assert timeout >= ConsumerSettings.from_config(wired).read_block.total_seconds() + 5


def test_the_publish_budget_bounds_one_attempt_by_connect_plus_read_timeout(wired):
    # A reconnecting attempt can spend socket_connect_timeout and then socket_timeout.
    entry.main(["--mode", "indexed"])
    kwargs = FakeRedis.created[0].kwargs
    bound = kwargs["socket_connect_timeout"] + kwargs["socket_timeout"]
    assert FakeStreamConsumer.instances[0].settings.call_timeout == bound
