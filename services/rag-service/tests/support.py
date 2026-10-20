"""Shared test fixtures data."""

import json
from pathlib import Path

from rag.transport.status_event import IngestEvent

VALID_TOML = """
[minio]
url="localhost:9000"
access_key="minio_user"
secret_key="minio_password"
upload_bucket="upload-bucket"
ingestion_bucket="ingestion-bucket"

[sqlite]
dir="./sqlite_db"

[qdrant]
url="http://localhost:6333"

[rag_grpc]
port=50051

[rag]
collection="pdf-fixed"
embedding_model="qwen3-embedding"
embedding_dim=4096
llm_model="openai/gpt-oss-120b"
top_k=5
chunk_size=0
chunk_overlap=0
call_timeout="60s"

[rag.consumer]
job_timeout="15m"
watchdog_grace="2m"
min_idle="20m"
recovery_interval="1m"
poison_threshold=3
consumer_cleanup_idle="1h"

[redis]
host="localhost"
port=6379
stream="ingest.docs"
consumer_group="rag-pdf-fixed"
stream_max_len=324000
status_stream="ingest.status"
status_consumer_group="center-status"

[ingestion]
max_attempts=3
allowed_extensions=[".pdf", ".txt", ".md"]

[go]
port=8080
sqlite_dir="./go_sqlite_db"

[go.status_consumer]
entry_timeout="5s"
min_idle="60s"
recovery_interval="30s"
poison_threshold=3
consumer_cleanup_idle="1h"
"""


# ---- a real ingest event, parsed from the Go-generated fixture

FIXTURE_DIR = Path(__file__).resolve().parents[3] / "contracts" / "events" / "fixtures"


def ingest_event(**changes) -> IngestEvent:
    """The Go ingest fixture, optionally with fields replaced (attempts, hash, filename...)."""
    document = json.loads((FIXTURE_DIR / "ingest_event.json").read_text()) | changes
    return IngestEvent.parse(json.dumps(document))
