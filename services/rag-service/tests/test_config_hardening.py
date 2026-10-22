"""Review round 1: config bounds that keep Go and Python agreeing, and operator-facing behaviour."""

import logging
from pathlib import Path

import pytest

from rag.core import config_reader
from rag.core.config_reader import ConfigError, MinioConfig, load
from support import VALID_TOML

REPO_ROOT = Path(__file__).resolve().parents[3]


def write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(body)
    return path


def broken(old: str, new: str) -> str:
    assert old in VALID_TOML, f"test bug: {old!r} not in the base config"
    return VALID_TOML.replace(old, new, 1)


# ---- integer upper bounds (Go decodes TOML integers into int64 and rejects the rest)

@pytest.mark.parametrize(
    "old,new,key",
    [
        ("port=6379", "port=99999999999999999999", "redis.port"),
        ("stream_max_len=324000", "stream_max_len=9223372036854775808", "redis.stream_max_len"),
        ("max_attempts=3", "max_attempts=9223372036854775808", "ingestion.max_attempts"),
        ("top_k=5", "top_k=9223372036854775808", "rag.top_k"),
        ("embedding_dim=4096", "embedding_dim=9223372036854775808", "rag.embedding_dim"),
        ("poison_threshold=3\nconsumer_cleanup_idle", "poison_threshold=9223372036854775808\nconsumer_cleanup_idle", "rag.consumer.poison_threshold"),
    ],
)
def test_integers_beyond_int64_are_rejected(tmp_path, old, new, key):
    with pytest.raises(ConfigError, match=key.replace(".", r"\.")):
        load(write(tmp_path, broken(old, new)))


def test_int64_maximum_is_accepted(tmp_path):
    cfg = load(write(tmp_path, broken("stream_max_len=324000", "stream_max_len=9223372036854775807")))
    assert cfg.redis.stream_max_len == 2**63 - 1


@pytest.mark.parametrize("old,new,key", [
    ("port=6379", "port=65536", "redis.port"),
    ("port=50051", "port=65536", "rag_grpc.port"),
])
def test_ports_above_65535_are_rejected(tmp_path, old, new, key):
    with pytest.raises(ConfigError, match=key.replace(".", r"\.")):
        load(write(tmp_path, broken(old, new)))


def test_port_65535_is_accepted(tmp_path):
    assert load(write(tmp_path, broken("port=6379", "port=65535"))).redis.port == 65535


# ---- every problem is reported in one run, including the timing rules

def test_a_timing_violation_is_reported_together_with_an_unrelated_problem(tmp_path):
    body = broken('min_idle="20m"', 'min_idle="10m"').replace('llm_model="openai/gpt-oss-120b"', "")
    with pytest.raises(ConfigError) as excinfo:
        load(write(tmp_path, body))
    assert "rag.llm_model" in str(excinfo.value)
    assert "rag.consumer.min_idle" in str(excinfo.value)


def test_a_missing_duration_does_not_produce_a_bogus_timing_error(tmp_path):
    with pytest.raises(ConfigError) as excinfo:
        load(write(tmp_path, broken('job_timeout="15m"', "")))
    message = str(excinfo.value)
    assert "rag.consumer.job_timeout" in message
    assert "must exceed" not in message  # the rules need every duration to be valid first


# ---- secrets stay out of reprs and logs

def test_the_minio_secrets_are_not_in_the_repr():
    text = repr(MinioConfig(url="u", access_key="AKIAEXAMPLE", secret_key="SECRET-VALUE", upload_bucket="a", ingestion_bucket="b"))
    assert "SECRET-VALUE" not in text and "AKIAEXAMPLE" not in text


def test_the_whole_config_repr_has_no_secrets(tmp_path):
    cfg = load(write(tmp_path, VALID_TOML))
    assert "minio_password" not in repr(cfg) and "minio_user" not in repr(cfg)


# ---- default path

def test_default_path_without_a_repo_layout_is_a_config_error(monkeypatch):
    # A Docker image installs the code under /app/src/..., where five parents do not exist.
    monkeypatch.setattr(config_reader, "__file__", "/app/src/rag/core/config_reader.py")
    with pytest.raises(ConfigError, match="RAG_CONFIG"):
        config_reader.default_path()


def test_an_empty_rag_config_env_falls_back_to_the_default(monkeypatch):
    monkeypatch.setenv("RAG_CONFIG", "")
    assert config_reader.default_path() == REPO_ROOT / "config.toml"
    assert load().rag.collection


# ---- unknown keys are logged by a shared helper, not by one entry point

def test_warn_unknown_keys_logs_them(tmp_path, caplog):
    cfg = load(write(tmp_path, broken("top_k=5", 'top_k=5\nprompt_template="x"')))
    with caplog.at_level(logging.WARNING):
        config_reader.warn_unknown_keys(cfg, logging.getLogger("test"))
    assert "rag.prompt_template" in caplog.text


def test_warn_unknown_keys_is_silent_when_there_are_none(tmp_path, caplog):
    cfg = load(write(tmp_path, VALID_TOML))
    with caplog.at_level(logging.WARNING):
        config_reader.warn_unknown_keys(cfg, logging.getLogger("test"))
    assert caplog.text == ""


def test_deploy_configs_are_loaded_once_they_exist():
    deploy = sorted((REPO_ROOT / "deploy").glob("*.toml"))
    if not deploy:
        pytest.skip("no deploy/*.toml yet (plan task 31 adds config.compose.toml and config.e2e.toml)")
    for path in deploy:
        load(path)


# ---- the deploy configs (plan task 31)

@pytest.mark.parametrize("name", ["config.compose.toml", "config.e2e.toml"])
def test_each_deploy_config_loads_with_no_unknown_keys(name):
    cfg = load(REPO_ROOT / "deploy" / name)
    assert cfg.unknown_keys == ()
    assert (cfg.minio.url, cfg.redis.host, cfg.qdrant.url) == ("minio:9000", "redis", "http://qdrant:6333")
    assert cfg.ingestion.max_attempts == 3


def test_the_e2e_config_has_the_short_timings_the_e2e_depends_on():
    from datetime import timedelta

    consumer = load(REPO_ROOT / "deploy" / "config.e2e.toml").rag.consumer
    assert (consumer.job_timeout, consumer.watchdog_grace, consumer.min_idle) == (
        timedelta(seconds=5), timedelta(seconds=2), timedelta(seconds=10))
    assert (consumer.recovery_interval, consumer.consumer_cleanup_idle) == (timedelta(seconds=2), timedelta(seconds=15))
    assert load(REPO_ROOT / "deploy" / "config.e2e.toml").rag.call_timeout == timedelta(seconds=5)
