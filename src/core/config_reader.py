import os
import tomllib
from dataclasses import dataclass
from functools import lru_cache


CONFIG_FILE = "config.toml"


@dataclass(frozen=True)
class MinioConfig:
    url: str
    access_key: str
    secret_key: str
    bucket: str


@dataclass(frozen=True)
class SqliteConfig:
    dir: str


@dataclass(frozen=True)
class QdrantConfig:
    url: str


@dataclass(frozen=True)
class ServerConfig:
    port: int


@dataclass(frozen=True)
class RagConfig:
    collection: str
    model: str


@dataclass(frozen=True)
class RedisConfig:
    host: str
    port: int
    stream: str
    consumer_group: str


@dataclass(frozen=True)
class Config:
    minio: MinioConfig
    sqlite: SqliteConfig
    qdrant: QdrantConfig
    server: ServerConfig
    rag: RagConfig
    redis: RedisConfig


def _load_config() -> Config:
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Missing config file: {CONFIG_FILE}")

    with open(CONFIG_FILE, "rb") as f:
        data = tomllib.load(f)

    return Config(
        minio=MinioConfig(**data["minio"]),
        sqlite=SqliteConfig(**data["sqlite"]),
        qdrant=QdrantConfig(**data["qdrant"]),
        server=ServerConfig(**data["server"]),
        rag=RagConfig(**data["rag"]),
        redis=RedisConfig(**data["redis"]),
    )


@lru_cache(maxsize=1)
def get_config() -> Config:
    return _load_config()
