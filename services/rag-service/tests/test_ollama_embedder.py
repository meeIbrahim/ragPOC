"""R2 OllamaEmbedder: query prefix (D2), errors, dimension probe (A8, E12)."""

import os
from types import SimpleNamespace

import httpx
import pytest
from ollama import ResponseError

from rag.adapters.ollama_embedder import OllamaEmbedder
from rag.ports import DependencyUnavailable, ModelNotAvailable

INSTRUCTION = "Given a question, retrieve passages that answer it"


class FakeClient:
    def __init__(self, dim=3, error=None):
        self.dim = dim
        self.error = error
        self.calls = []

    def embed(self, model, input):
        self.calls.append((model, list(input)))
        if self.error:
            raise self.error
        return SimpleNamespace(embeddings=[[0.1] * self.dim for _ in input])


def embedder(client):
    return OllamaEmbedder(client, model="qwen3-embedding:4b", query_instruction=INSTRUCTION)


def test_documents_are_embedded_as_is_in_one_call():
    client = FakeClient()
    vectors = embedder(client).embed_documents(["first", "second"])
    assert vectors == [[0.1] * 3, [0.1] * 3]
    assert client.calls == [("qwen3-embedding:4b", ["first", "second"])]


def test_queries_get_the_qwen3_instruction_prefix():
    client = FakeClient()
    embedder(client).embed_query("What is X?")
    assert client.calls == [("qwen3-embedding:4b", [f"Instruct: {INSTRUCTION}\nQuery:What is X?"])]


def test_no_documents_makes_no_call():
    client = FakeClient()
    assert embedder(client).embed_documents([]) == []
    assert client.calls == []


def test_dimension_is_measured_with_a_probe():
    assert embedder(FakeClient(dim=2560)).dimension() == 2560


@pytest.mark.parametrize(
    "error",
    [
        ConnectionError("Failed to connect to Ollama"),
        httpx.ReadTimeout("timed out"),
        ResponseError("server busy", 503),
        ResponseError("model 'x' not found", 404),
    ],
    ids=["connect", "timeout", "503", "404-mid-run"],
)
def test_embedding_failures_are_dependency_unavailable(error):
    with pytest.raises(DependencyUnavailable):
        embedder(FakeClient(error=error)).embed_documents(["a"])
    with pytest.raises(DependencyUnavailable):
        embedder(FakeClient(error=error)).embed_query("a")


def test_a_wrong_number_of_vectors_is_an_error():
    class Short(FakeClient):
        def embed(self, model, input):
            return SimpleNamespace(embeddings=[[0.1]])

    with pytest.raises(DependencyUnavailable):
        embedder(Short()).embed_documents(["a", "b"])


def test_dimension_reports_a_missing_model():
    with pytest.raises(ModelNotAvailable) as excinfo:
        embedder(FakeClient(error=ResponseError("model not found", 404))).dimension()
    assert excinfo.value.model == "qwen3-embedding:4b"


def test_dimension_reports_an_unreachable_ollama_as_a_dependency_failure():
    with pytest.raises(DependencyUnavailable):
        embedder(FakeClient(error=ConnectionError("down"))).dimension()


# ---- the host Ollama


@pytest.mark.ollama
def test_the_configured_model_on_the_host():
    import ollama

    from rag.core import config_reader

    config = config_reader.load(os.environ.get("RAG_CONFIG"))
    real = OllamaEmbedder(
        ollama.Client(host=config.ollama.url, timeout=config.rag.call_timeout.total_seconds()),
        model=config.rag.embedding_model,
        query_instruction=config.rag.query_instruction,
    )
    assert real.dimension() == config.rag.embedding_dim
    vectors = real.embed_documents(["alpha", "beta", "gamma"])
    assert len(vectors) == 3 and all(len(v) == config.rag.embedding_dim for v in vectors)
    assert len(real.embed_query("what?")) == config.rag.embedding_dim
