"""T-P1: the shared config.toml as the Python service reads it.

These mirror services/center/internal/config/config_test.go, so both services accept and reject
the same files, and both parse durations through contracts/config/duration_vectors.json.
"""

import dataclasses
import json
from datetime import timedelta
from pathlib import Path

import pytest

from rag.core import config_reader
from rag.core.config_reader import ConfigError, load, parse_duration
from support import VALID_TOML

REPO_ROOT = Path(__file__).resolve().parents[3]



def write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(body)
    return path


def broken(old: str, new: str) -> str:
    assert old in VALID_TOML, f"test bug: {old!r} not in the base config"
    return VALID_TOML.replace(old, new, 1)


# ---- parse_duration: the shared vectors

VECTORS = json.loads((REPO_ROOT / "contracts" / "config" / "duration_vectors.json").read_text())


@pytest.mark.parametrize("vector", VECTORS["valid"], ids=lambda v: v["text"])
def test_parse_duration_accepts_every_shared_valid_vector(vector):
    assert parse_duration(vector["text"]) == timedelta(seconds=vector["seconds"])


@pytest.mark.parametrize("text", VECTORS["invalid"], ids=lambda t: repr(t))
def test_parse_duration_rejects_every_shared_invalid_vector(text):
    with pytest.raises(ConfigError, match="invalid duration"):
        parse_duration(text)


def test_vector_files_are_not_empty():
    assert VECTORS["valid"] and VECTORS["invalid"]


def test_parse_duration_is_not_fooled_by_a_trailing_newline():
    # Python's re.search lets "$" match before a final newline; the grammar must not.
    with pytest.raises(ConfigError):
        parse_duration("15m\n")


def test_parse_duration_rejects_non_ascii_digits():
    # Arabic-Indic digits match \d in Python but are not [0-9].
    with pytest.raises(ConfigError):
        parse_duration("١٥m")


@pytest.mark.parametrize("value", [15, 1.5, True, None, ["15m"]])
def test_parse_duration_rejects_non_strings(value):
    with pytest.raises(ConfigError, match="invalid duration"):
        parse_duration(value)


# ---- loading

def test_loads_the_repo_config():
    cfg = load(REPO_ROOT / "config.toml")
    assert cfg.unknown_keys == ()


def test_loads_every_deploy_config_when_present():
    for path in sorted((REPO_ROOT / "deploy").glob("*.toml")):
        load(path)  # raises ConfigError if invalid


def test_parses_all_sections(tmp_path):
    cfg = load(write(tmp_path, VALID_TOML))
    assert cfg.minio.url == "localhost:9000"
    assert cfg.minio.ingestion_bucket == "ingestion-bucket"
    assert cfg.sqlite.dir == "./sqlite_db"
    assert cfg.qdrant.url == "http://localhost:6333"
    assert cfg.rag.collection == "pdf-fixed"
    assert cfg.rag.embedding_model == "qwen3-embedding"
    assert cfg.rag.embedding_dim == 4096
    assert cfg.rag.call_timeout == timedelta(seconds=60)
    consumer = cfg.rag.consumer
    assert consumer.job_timeout == timedelta(minutes=15)
    assert consumer.watchdog_grace == timedelta(minutes=2)
    assert consumer.min_idle == timedelta(minutes=20)
    assert consumer.recovery_interval == timedelta(minutes=1)
    assert consumer.poison_threshold == 3
    assert consumer.consumer_cleanup_idle == timedelta(hours=1)
    assert cfg.rag_grpc.port == 50051
    assert (cfg.redis.stream, cfg.redis.consumer_group) == ("ingest.docs", "rag-pdf-fixed")
    assert (cfg.redis.status_stream, cfg.redis.status_consumer_group) == ("ingest.status", "center-status")
    assert cfg.redis.stream_max_len == 324000
    assert cfg.ingestion.max_attempts == 3
    assert cfg.ingestion.allowed_extensions == (".pdf", ".txt", ".md")


def test_parses_the_phase1_keys(tmp_path):
    cfg = load(write(tmp_path, VALID_TOML))
    assert cfg.ollama.url == "http://localhost:11434"
    assert (cfg.rag.chunk_size, cfg.rag.chunk_overlap, cfg.rag.top_k) == (1000, 150, 5)
    assert cfg.rag.min_score == 0.0
    assert cfg.rag.embed_batch_size == 16
    assert cfg.rag.query_instruction == "Given a question, retrieve passages that answer it"
    assert cfg.rag.reasoning_effort == "low"
    assert (cfg.rag.max_query_chars, cfg.rag.max_document_ids) == (2000, 100)
    assert cfg.rag.system_prompt == "Answer only from the numbered context."
    assert cfg.rag.consumer.max_file_bytes == 10485760
    assert cfg.rag.consumer.allow_skip_dim_check is False
    assert cfg.rag_grpc.max_workers == 8


def test_min_score_accepts_an_integer_and_a_fraction(tmp_path):
    assert load(write(tmp_path, broken("min_score=0.0", "min_score=0"))).rag.min_score == 0.0
    assert load(write(tmp_path, broken("min_score=0.0", "min_score=0.42"))).rag.min_score == 0.42


def test_skip_dim_check_marker_is_read_when_present(tmp_path):
    body = broken("max_file_bytes=10485760", "max_file_bytes=10485760\nallow_skip_dim_check=true")
    assert load(write(tmp_path, body)).rag.consumer.allow_skip_dim_check is True


def test_config_is_immutable(tmp_path):
    cfg = load(write(tmp_path, VALID_TOML))
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.rag.collection = "other"
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.rag.consumer.poison_threshold = 9


def test_missing_file_is_a_config_error(tmp_path):
    with pytest.raises(ConfigError, match="cannot read"):
        load(tmp_path / "nope.toml")


def test_invalid_toml_is_a_config_error(tmp_path):
    with pytest.raises(ConfigError, match="not valid TOML"):
        load(write(tmp_path, "[rag\n"))


def test_path_comes_from_rag_config_env(tmp_path, monkeypatch):
    path = write(tmp_path, broken('collection="pdf-fixed"', 'collection="from-env"'))
    monkeypatch.setenv("RAG_CONFIG", str(path))
    assert load().rag.collection == "from-env"


def test_default_path_is_the_repo_config(monkeypatch):
    monkeypatch.delenv("RAG_CONFIG", raising=False)
    assert config_reader.default_path() == REPO_ROOT / "config.toml"
    assert load().rag.collection  # the real file loads


def test_get_config_is_cached(monkeypatch):
    monkeypatch.delenv("RAG_CONFIG", raising=False)
    config_reader.get_config.cache_clear()
    assert config_reader.get_config() is config_reader.get_config()
    config_reader.get_config.cache_clear()


# ---- unknown keys warn, they do not fail

def test_unknown_keys_are_listed_not_fatal(tmp_path):
    cfg = load(write(tmp_path, broken("top_k=5", 'top_k=5\nprompt_template="x"')))
    assert cfg.unknown_keys == ("rag.prompt_template",)


def test_the_go_section_is_not_reported_as_unknown(tmp_path):
    assert load(write(tmp_path, VALID_TOML)).unknown_keys == ()


def test_a_stale_server_section_is_reported_and_the_renamed_key_is_required(tmp_path):
    body = broken("[rag_grpc]\nport=50051", "[server]\nport=50051")
    with pytest.raises(ConfigError, match=r"rag_grpc\.port"):
        load(write(tmp_path, body))


# ---- rejections: each names the full dotted key

REJECTIONS = [
    ("job_timeout + grace == min_idle", 'min_idle="20m"', 'min_idle="17m"', "rag.consumer.min_idle"),
    ("job_timeout + grace > min_idle", 'min_idle="20m"', 'min_idle="10m"', "rag.consumer.min_idle"),
    ("cleanup_idle == job_timeout", 'consumer_cleanup_idle="1h"', 'consumer_cleanup_idle="15m"', "rag.consumer.consumer_cleanup_idle"),
    ("cleanup_idle < job_timeout", 'consumer_cleanup_idle="1h"', 'consumer_cleanup_idle="5m"', "rag.consumer.consumer_cleanup_idle"),
    ("duration without unit", 'job_timeout="15m"', 'job_timeout="15"', "rag.consumer.job_timeout"),
    ("duration as integer", 'job_timeout="15m"', "job_timeout=15", "rag.consumer.job_timeout"),
    ("compound duration", 'job_timeout="15m"', 'job_timeout="14m60s"', "rag.consumer.job_timeout"),
    ("zero job_timeout", 'job_timeout="15m"', 'job_timeout="0s"', "rag.consumer.job_timeout"),
    ("zero poison_threshold", "poison_threshold=3\nconsumer_cleanup_idle", "poison_threshold=0\nconsumer_cleanup_idle", "rag.consumer.poison_threshold"),
    ("zero call_timeout", 'call_timeout="60s"', 'call_timeout="0s"', "rag.call_timeout"),
    ("missing collection", 'collection="pdf-fixed"', "", "rag.collection"),
    ("missing embedding_model", 'embedding_model="qwen3-embedding"', 'model="qwen3-embedding"', "rag.embedding_model"),
    ("negative embedding_dim", "embedding_dim=4096", "embedding_dim=-1", "rag.embedding_dim"),
    ("missing sqlite.dir", 'dir="./sqlite_db"', "", "sqlite.dir"),
    ("missing qdrant.url", 'url="http://localhost:6333"', "", "qdrant.url"),
    ("missing minio.url", 'url="localhost:9000"', "", "minio.url"),
    ("missing minio.access_key", 'access_key="minio_user"', "", "minio.access_key"),
    ("missing minio.secret_key", 'secret_key="minio_password"', "", "minio.secret_key"),
    ("missing ingestion_bucket", 'ingestion_bucket="ingestion-bucket"', "", "minio.ingestion_bucket"),
    ("misspelled redis.stream", 'stream="ingest.docs"', 'strem="ingest.docs"', "redis.stream"),
    ("missing redis.host", 'host="localhost"', "", "redis.host"),
    ("zero redis.port", "port=6379", "port=0", "redis.port"),
    ("missing consumer_group", 'consumer_group="rag-pdf-fixed"', "", "redis.consumer_group"),
    ("zero stream_max_len", "stream_max_len=324000", "stream_max_len=0", "redis.stream_max_len"),
    ("missing status_stream", 'status_stream="ingest.status"', "", "redis.status_stream"),
    ("missing status group", 'status_consumer_group="center-status"', "", "redis.status_consumer_group"),
    ("zero max_attempts", "max_attempts=3", "max_attempts=0", "ingestion.max_attempts"),
    ("no allowed extensions", 'allowed_extensions=[".pdf", ".txt", ".md"]', "allowed_extensions=[]", "ingestion.allowed_extensions"),
    ("extension without dot", 'allowed_extensions=[".pdf", ".txt", ".md"]', 'allowed_extensions=["pdf"]', "ingestion.allowed_extensions"),
    ("extension with second dot", 'allowed_extensions=[".pdf", ".txt", ".md"]', 'allowed_extensions=[".tar.gz"]', "ingestion.allowed_extensions"),
    ("extension with space", 'allowed_extensions=[".pdf", ".txt", ".md"]', 'allowed_extensions=[".p df"]', "ingestion.allowed_extensions"),
    ("extension not a string", 'allowed_extensions=[".pdf", ".txt", ".md"]', "allowed_extensions=[1]", "ingestion.allowed_extensions"),
    ("zero rag_grpc.port", "port=50051", "port=0", "rag_grpc.port"),
    ("boolean where an integer is expected", "top_k=5", "top_k=true", "rag.top_k"),
    ("string where an integer is expected", "top_k=5", 'top_k="5"', "rag.top_k"),
    ("missing ollama.url", 'url="http://localhost:11434"', "", "ollama.url"),
    ("zero chunk_size", "chunk_size=1000", "chunk_size=0", "rag.chunk_size"),
    ("overlap equal to chunk_size", "chunk_overlap=150", "chunk_overlap=1000", "rag.chunk_overlap"),
    ("negative overlap", "chunk_overlap=150", "chunk_overlap=-1", "rag.chunk_overlap"),
    ("zero top_k", "top_k=5", "top_k=0", "rag.top_k"),
    ("min_score of 1", "min_score=0.0", "min_score=1.0", "rag.min_score"),
    ("negative min_score", "min_score=0.0", "min_score=-0.1", "rag.min_score"),
    ("min_score as a string", "min_score=0.0", 'min_score="0.5"', "rag.min_score"),
    ("min_score as a boolean", "min_score=0.0", "min_score=true", "rag.min_score"),
    ("zero embed_batch_size", "embed_batch_size=16", "embed_batch_size=0", "rag.embed_batch_size"),
    ("empty query_instruction", 'query_instruction="Given a question, retrieve passages that answer it"', 'query_instruction=""', "rag.query_instruction"),
    ("unknown reasoning_effort", 'reasoning_effort="low"', 'reasoning_effort="max"', "rag.reasoning_effort"),
    ("zero max_query_chars", "max_query_chars=2000", "max_query_chars=0", "rag.max_query_chars"),
    ("zero max_document_ids", "max_document_ids=100", "max_document_ids=0", "rag.max_document_ids"),
    ("empty system_prompt", "system_prompt='''Answer only from the numbered context.'''", 'system_prompt=""', "rag.system_prompt"),
    ("zero max_file_bytes", "max_file_bytes=10485760", "max_file_bytes=0", "rag.consumer.max_file_bytes"),
    ("skip marker not a boolean", "max_file_bytes=10485760", 'max_file_bytes=10485760\nallow_skip_dim_check="yes"', "rag.consumer.allow_skip_dim_check"),
    ("zero max_workers", "max_workers=8", "max_workers=0", "rag_grpc.max_workers"),
]


@pytest.mark.parametrize("name,old,new,expected", REJECTIONS, ids=[r[0] for r in REJECTIONS])
def test_rejects(tmp_path, name, old, new, expected):
    with pytest.raises(ConfigError) as excinfo:
        load(write(tmp_path, broken(old, new)))
    assert expected in str(excinfo.value)


def test_boundaries_just_above_the_limits_are_valid(tmp_path):
    # 1021s = 15m + 2m + 1s; 901s = 15m + 1s. The Go test checks the same two values.
    load(write(tmp_path, broken('min_idle="20m"', 'min_idle="1021s"')))
    load(write(tmp_path, broken('consumer_cleanup_idle="1h"', 'consumer_cleanup_idle="901s"')))


def test_missing_consumer_section_reports_every_key(tmp_path):
    start, end = VALID_TOML.index("[rag.consumer]"), VALID_TOML.index("[redis]")
    with pytest.raises(ConfigError) as excinfo:
        load(write(tmp_path, VALID_TOML[:start] + VALID_TOML[end:]))
    for key in ("rag.consumer.job_timeout", "rag.consumer.min_idle", "rag.consumer.poison_threshold"):
        assert key in str(excinfo.value)


def test_reports_all_problems_together(tmp_path):
    body = broken("max_attempts=3", "max_attempts=0").replace('collection="pdf-fixed"', "")
    with pytest.raises(ConfigError) as excinfo:
        load(write(tmp_path, body))
    assert "ingestion.max_attempts" in str(excinfo.value)
    assert "rag.collection" in str(excinfo.value)
