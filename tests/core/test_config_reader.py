import textwrap

from core import config_reader


def test_loads_redis_and_qdrant_url_config(tmp_path, monkeypatch):
    config_file = tmp_path / "config.toml"
    config_file.write_text(textwrap.dedent("""
        [minio]
        url="localhost:9000"
        access_key="minio_user"
        secret_key="minio_password"
        bucket="rag-pdf-source"

        [sqlite]
        dir="./sqlite_db"

        [qdrant]
        url="localhost:6333"

        [server]
        port=50051

        [rag]
        collection="pdf-fixed"
        model="qwen3-embedding"

        [redis]
        host="localhost"
        port=6379
        stream="ingest.docs"
        consumer_group="rag-pdf-fixed"
        """))
    monkeypatch.chdir(tmp_path)
    config_reader.get_config.cache_clear()

    config = config_reader.get_config()

    assert config.qdrant.url == "localhost:6333"
    assert config.redis.host == "localhost"
    assert config.redis.port == 6379
    assert config.redis.stream == "ingest.docs"
    assert config.redis.consumer_group == "rag-pdf-fixed"
