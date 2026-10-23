"""Builds the real adapters from a Config. The only module that knows which adapter backs which
port; the entry points, tools and the eval all wire through it."""

from __future__ import annotations

import os

import ollama
from groq import Groq

from rag.adapters.groq_llm import GroqLLM
from rag.adapters.minio_blob_store import MinioBlobStore
from rag.adapters.ollama_embedder import OllamaEmbedder
from rag.adapters.qdrant_index import QdrantIndex
from rag.adapters.sqlite_index_registry import SqliteIndexRegistry
from rag.core import minio_client, qdrant_manager
from rag.core.config_reader import Config
from rag.core.startup import StartupError
from rag.ingest.service import IngestService, IngestSettings
from rag.ports import LLM
from rag.query.service import QueryService, QuerySettings


def embedder(config: Config) -> OllamaEmbedder:
    client = ollama.Client(host=config.ollama.url, timeout=config.rag.call_timeout.total_seconds())
    return OllamaEmbedder(client, model=config.rag.embedding_model, query_instruction=config.rag.query_instruction)


def index(config: Config) -> QdrantIndex:
    return QdrantIndex(
        qdrant_manager.build_client(config), collection=config.rag.collection, batch_size=config.rag.embed_batch_size
    )


def registry(config: Config) -> SqliteIndexRegistry:
    return SqliteIndexRegistry(config.rag.collection, config.sqlite.dir)


def blob_store(config: Config) -> MinioBlobStore:
    return MinioBlobStore(minio_client.build_client(config), config.minio.ingestion_bucket)


def llm(config: Config) -> GroqLLM:
    # The key comes only from the environment, never from config.toml (PRD L7).
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        raise StartupError("GROQ_API_KEY is not set: export it in the environment (never put it in config.toml)")
    client = Groq(api_key=api_key, timeout=config.rag.call_timeout.total_seconds(), max_retries=0)
    return GroqLLM(client, model=config.rag.llm_model, reasoning_effort=config.rag.reasoning_effort)


def ingest_service(config: Config) -> IngestService:
    return IngestService(
        registry=registry(config),
        blobs=blob_store(config),
        embedder=embedder(config),
        index=index(config),
        settings=IngestSettings(
            chunk_size=config.rag.chunk_size,
            chunk_overlap=config.rag.chunk_overlap,
            embed_batch_size=config.rag.embed_batch_size,
            max_file_bytes=config.rag.consumer.max_file_bytes,
        ),
    )


def query_service(config: Config, llm_adapter: LLM | None = None) -> QueryService:
    """llm_adapter defaults to Groq; the retrieval eval passes its own (it never calls the LLM)."""
    return QueryService(
        embedder=embedder(config),
        index=index(config),
        llm=llm_adapter if llm_adapter is not None else llm(config),
        settings=QuerySettings(
            top_k=config.rag.top_k,
            min_score=config.rag.min_score,
            max_query_chars=config.rag.max_query_chars,
            max_document_ids=config.rag.max_document_ids,
            system_prompt=config.rag.system_prompt,
        ),
    )
