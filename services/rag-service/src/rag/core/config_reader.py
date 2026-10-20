"""Reads the shared config.toml.

The Go center service reads the same file (services/center/internal/config). Both accept the
same durations (contracts/config/duration_vectors.json) and enforce the same timing rules, so a
file that starts one service cannot silently break the other.

Each service validates the keys it reads. Keys it does not model are not fatal: the other
service (or a later phase) may add some, so they are listed in Config.unknown_keys and the entry
point logs a warning.
"""

from __future__ import annotations

import functools
import logging
import os
import re
import tomllib
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any


class ConfigError(Exception):
    """The config file is unreadable or invalid. The message lists every problem found."""


# The one duration grammar both services accept: an integer and a single unit. It is narrower
# than Go's time.ParseDuration on purpose (no compound, decimal, negative or sub-second values).
# fullmatch, not search: "$" would also match before a trailing newline. [0-9], not \d, which
# matches other scripts' digits.
_DURATION = re.compile(r"[0-9]{1,6}(s|m|h)")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600}

# TOML integers are 64-bit; Go rejects the rest when it decodes them, so Python must too, or a
# file would start one service and break the other.
_MAX_INT64 = 2**63 - 1
_MAX_PORT = 65535

# Characters that cannot appear inside an extension: it is the text after the last dot of a
# filename, so ".tar.gz" could never match. The same set as the Go reader.
_EXTENSION_FORBIDDEN = ".//\\ \t"


def parse_duration(value: Any) -> timedelta:
    """Parse "15m" style durations. Anything but a string in the shared grammar is an error."""
    match = _DURATION.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise ConfigError(f'invalid duration {value!r}: want an integer and one unit (s, m or h), e.g. "15m"')
    return timedelta(seconds=int(value[:-1]) * _UNIT_SECONDS[value[-1]])


@dataclass(frozen=True)
class MinioConfig:
    url: str
    # Credentials stay out of the generated repr, so logging the config cannot leak them.
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    upload_bucket: str
    ingestion_bucket: str


@dataclass(frozen=True)
class SqliteConfig:
    dir: str


@dataclass(frozen=True)
class QdrantConfig:
    url: str


@dataclass(frozen=True)
class RagConsumerConfig:
    job_timeout: timedelta
    watchdog_grace: timedelta
    min_idle: timedelta
    recovery_interval: timedelta
    poison_threshold: int
    consumer_cleanup_idle: timedelta


@dataclass(frozen=True)
class RagConfig:
    collection: str
    embedding_model: str
    embedding_dim: int
    llm_model: str
    top_k: int
    chunk_size: int
    chunk_overlap: int
    call_timeout: timedelta
    consumer: RagConsumerConfig


@dataclass(frozen=True)
class RagGrpcConfig:
    port: int


@dataclass(frozen=True)
class RedisConfig:
    host: str
    port: int
    stream: str
    consumer_group: str
    stream_max_len: int
    status_stream: str
    status_consumer_group: str


@dataclass(frozen=True)
class IngestionConfig:
    max_attempts: int
    allowed_extensions: tuple[str, ...]


@dataclass(frozen=True)
class Config:
    minio: MinioConfig
    sqlite: SqliteConfig
    qdrant: QdrantConfig
    rag: RagConfig
    rag_grpc: RagGrpcConfig
    redis: RedisConfig
    ingestion: IngestionConfig
    # Dotted names of keys in the file that this reader does not model.
    unknown_keys: tuple[str, ...] = ()


def _leaves(*names: str) -> dict[str, None]:
    return {name: None for name in names}


# Keys this reader models, per table (None marks a leaf, a dict a nested table). Used to report
# the keys it does not model.
_KNOWN: dict[str, Any] = {
    "minio": _leaves("url", "access_key", "secret_key", "upload_bucket", "ingestion_bucket"),
    "sqlite": _leaves("dir"),
    "qdrant": _leaves("url"),
    "rag_grpc": _leaves("port"),
    "rag": {
        **_leaves("collection", "embedding_model", "embedding_dim", "llm_model", "top_k",
                  "chunk_size", "chunk_overlap", "call_timeout"),
        "consumer": _leaves("job_timeout", "watchdog_grace", "min_idle", "recovery_interval",
                            "poison_threshold", "consumer_cleanup_idle"),
    },
    "redis": _leaves("host", "port", "stream", "consumer_group", "stream_max_len", "status_stream",
                     "status_consumer_group"),
    "ingestion": _leaves("max_attempts", "allowed_extensions"),
}
# Tables that belong to the Go service. They are not this reader's business, so not "unknown".
_FOREIGN_TABLES = {"go"}


def default_path() -> Path:
    """config.toml at the repo root: services/rag-service/src/rag/core -> five levels up.

    That only holds for a checkout. Anywhere else (a container image, site-packages) set
    RAG_CONFIG. A relative sqlite.dir in the file is resolved against the working directory,
    not against the file."""
    here = Path(__file__).resolve()
    try:
        return here.parents[5] / "config.toml"
    except IndexError:
        raise ConfigError(
            f"cannot locate config.toml relative to {here}: set RAG_CONFIG to its path"
        ) from None


def load(path: str | os.PathLike[str] | None = None) -> Config:
    """Read and validate the config.

    path defaults to $RAG_CONFIG, else the repo's config.toml. Raises ConfigError listing every
    problem when the file is unreadable or invalid.
    """
    if path is None:
        path = os.environ.get("RAG_CONFIG") or default_path()
    path = Path(path)
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"cannot read {path}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{path} is not valid TOML: {exc}") from exc

    reader = _Reader(raw)
    config = reader.build()
    if reader.problems:
        raise ConfigError(f"invalid config {path}: " + "; ".join(reader.problems))
    return config


def warn_unknown_keys(config: Config, logger: logging.Logger) -> None:
    """Every entry point calls this once after loading, so a typo'd key is visible."""
    if config.unknown_keys:
        logger.warning("config has keys this service does not read: %s", ", ".join(config.unknown_keys))


@functools.cache
def get_config() -> Config:
    """The process-wide config, loaded once. Tests clear it with get_config.cache_clear()."""
    return load()


class _Reader:
    """Reads typed values out of the decoded TOML, recording every problem instead of stopping
    at the first, so one run reports everything that is wrong."""

    def __init__(self, raw: dict[str, Any]) -> None:
        self.raw = raw
        self.problems: list[str] = []
        # Dotted names of durations that failed to parse or were not positive. The timing rules
        # need valid durations, but must still run when an unrelated key is wrong.
        self.invalid_durations: set[str] = set()

    def _table(self, path: tuple[str, ...]) -> dict[str, Any]:
        node: Any = self.raw
        for part in path:
            node = node.get(part) if isinstance(node, dict) else None
        return node if isinstance(node, dict) else {}

    def _fail(self, path: tuple[str, ...], key: str, message: str) -> None:
        self.problems.append(f"{'.'.join((*path, key))} {message}")

    def text(self, path: tuple[str, ...], key: str) -> str:
        value = self._table(path).get(key)
        if not isinstance(value, str) or value == "":
            self._fail(path, key, "must be a non-empty string")
            return ""
        return value

    def optional_text(self, path: tuple[str, ...], key: str) -> str:
        value = self._table(path).get(key, "")
        if not isinstance(value, str):
            self._fail(path, key, "must be a string")
            return ""
        return value

    def integer(self, path: tuple[str, ...], key: str, minimum: int, maximum: int = _MAX_INT64) -> int:
        value = self._table(path).get(key)
        # bool is an int subclass in Python; TOML true/false must not pass as a number.
        if not isinstance(value, int) or isinstance(value, bool):
            self._fail(path, key, "must be an integer")
            return minimum
        if value < minimum:
            self._fail(path, key, f"must be >= {minimum}")
        elif value > maximum:
            self._fail(path, key, f"must be <= {maximum}")
        return value

    def duration(self, path: tuple[str, ...], key: str) -> timedelta:
        value = self._table(path).get(key)
        dotted = ".".join((*path, key))
        try:
            parsed = parse_duration(value)
        except ConfigError as exc:
            self._fail(path, key, str(exc))
            self.invalid_durations.add(dotted)
            return timedelta(0)
        if parsed <= timedelta(0):
            self._fail(path, key, "must be > 0")
            self.invalid_durations.add(dotted)
        return parsed

    def extensions(self, path: tuple[str, ...], key: str) -> tuple[str, ...]:
        value = self._table(path).get(key)
        if not isinstance(value, list) or not value:
            self._fail(path, key, "must be a non-empty list")
            return ()
        result: list[str] = []
        for ext in value:
            if (
                not isinstance(ext, str)
                or not ext.startswith(".")
                or len(ext) < 2
                or any(ch in _EXTENSION_FORBIDDEN for ch in ext[1:])
            ):
                self._fail(path, key, f'entry {ext!r} must be one extension such as ".pdf"')
            else:
                result.append(ext)
        return tuple(result)

    def build(self) -> Config:
        minio = MinioConfig(
            url=self.text(("minio",), "url"),
            access_key=self.text(("minio",), "access_key"),
            secret_key=self.text(("minio",), "secret_key"),
            upload_bucket=self.optional_text(("minio",), "upload_bucket"),  # Go's bucket; Python never reads it
            ingestion_bucket=self.text(("minio",), "ingestion_bucket"),
        )
        sqlite = SqliteConfig(dir=self.text(("sqlite",), "dir"))
        qdrant = QdrantConfig(url=self.text(("qdrant",), "url"))

        consumer_path = ("rag", "consumer")
        consumer = RagConsumerConfig(
            job_timeout=self.duration(consumer_path, "job_timeout"),
            watchdog_grace=self.duration(consumer_path, "watchdog_grace"),
            min_idle=self.duration(consumer_path, "min_idle"),
            recovery_interval=self.duration(consumer_path, "recovery_interval"),
            poison_threshold=self.integer(consumer_path, "poison_threshold", 1),
            consumer_cleanup_idle=self.duration(consumer_path, "consumer_cleanup_idle"),
        )
        rag = RagConfig(
            collection=self.text(("rag",), "collection"),
            embedding_model=self.text(("rag",), "embedding_model"),
            # 0 is allowed here: the Qdrant migration insists on a real size, the consumer does not.
            embedding_dim=self.integer(("rag",), "embedding_dim", 0),
            llm_model=self.text(("rag",), "llm_model"),
            top_k=self.integer(("rag",), "top_k", 0),
            chunk_size=self.integer(("rag",), "chunk_size", 0),
            chunk_overlap=self.integer(("rag",), "chunk_overlap", 0),
            call_timeout=self.duration(("rag",), "call_timeout"),
            consumer=consumer,
        )
        rag_grpc = RagGrpcConfig(port=self.integer(("rag_grpc",), "port", 1, _MAX_PORT))
        redis = RedisConfig(
            host=self.text(("redis",), "host"),
            port=self.integer(("redis",), "port", 1, _MAX_PORT),
            stream=self.text(("redis",), "stream"),
            consumer_group=self.text(("redis",), "consumer_group"),
            stream_max_len=self.integer(("redis",), "stream_max_len", 1),
            status_stream=self.text(("redis",), "status_stream"),
            status_consumer_group=self.text(("redis",), "status_consumer_group"),
        )
        ingestion = IngestionConfig(
            max_attempts=self.integer(("ingestion",), "max_attempts", 1),
            allowed_extensions=self.extensions(("ingestion",), "allowed_extensions"),
        )

        # The timing rules need valid durations, but not a clean file: an unrelated problem
        # elsewhere must not hide a violation, or the operator needs two runs to see both.
        timing_keys = {f"rag.consumer.{k}" for k in ("job_timeout", "watchdog_grace", "min_idle", "consumer_cleanup_idle")}
        if not (timing_keys & self.invalid_durations):
            self._check_timing(consumer)

        return Config(minio, sqlite, qdrant, rag, rag_grpc, redis, ingestion, tuple(self._unknown_keys()))

    def _check_timing(self, c: RagConsumerConfig) -> None:
        # A live job must never look idle enough to be reclaimed by XAUTOCLAIM.
        if c.job_timeout + c.watchdog_grace >= c.min_idle:
            self.problems.append(
                f"rag.consumer.min_idle ({c.min_idle}) must exceed job_timeout + watchdog_grace "
                f"({c.job_timeout + c.watchdog_grace})"
            )
        # A consumer still running a job must not be deleted as idle.
        if c.consumer_cleanup_idle <= c.job_timeout:
            self.problems.append(
                f"rag.consumer.consumer_cleanup_idle ({c.consumer_cleanup_idle}) must exceed "
                f"job_timeout ({c.job_timeout})"
            )

    def _unknown_keys(self) -> list[str]:
        unknown: list[str] = []

        def walk(node: dict[str, Any], known: dict[str, Any] | None, prefix: str) -> None:
            for key, value in node.items():
                dotted = f"{prefix}{key}"
                if known is not None and key in known:
                    nested = known[key]
                    if isinstance(value, dict) and nested is not None:
                        walk(value, nested, dotted + ".")
                    # A modelled key of the wrong shape is reported by the typed readers.
                elif isinstance(value, dict):
                    walk(value, None, dotted + ".")  # an unknown table: report its leaves
                else:
                    unknown.append(dotted)

        for table, value in self.raw.items():
            if table in _FOREIGN_TABLES:
                continue
            if isinstance(value, dict):
                walk({table: value}, _KNOWN, "")
            elif table not in _KNOWN:
                unknown.append(table)
        return unknown
