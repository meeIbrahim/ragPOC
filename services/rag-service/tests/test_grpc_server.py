"""R7 gRPC transport: message mapping, status codes, reflection, cancel (spec E13, E16, E20)."""

import threading
import time

import grpc
import pytest
from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

from fakes import FakeEmbedder, FakeIndex, FakeLLM
from generated import rag_pb2, rag_pb2_grpc
from rag.ports import DependencyUnavailable, Hit
from rag.query.service import NO_RELEVANT_ANSWER, QueryService, QuerySettings
from rag.transport.grpc_server import SERVICE_NAME, serve

H1 = "a" * 64
HIT = Hit(H1, "guide.md", 2, "chunk text", 0.75)
SETTINGS = QuerySettings(top_k=5, min_score=0.0, max_query_chars=100, max_document_ids=3, system_prompt="S")


class SlowLLM:
    """Yields one delta every 50 ms and records when the stream was closed."""

    def __init__(self):
        self.closed = threading.Event()
        self.yielded = 0

    def stream(self, system, user):
        try:
            for i in range(200):
                time.sleep(0.05)
                self.yielded += 1
                yield f"d{i} "
        finally:
            self.closed.set()


@pytest.fixture
def make_stub():
    servers = []

    def start(hits=(HIT,), llm=None, index_error=None):
        index = FakeIndex(hits)
        index.error = index_error
        service = QueryService(FakeEmbedder(), index, llm or FakeLLM(["An ", "answer."]), SETTINGS)
        server, port = serve(service, port=0, max_workers=4, host="127.0.0.1")
        servers.append(server)
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        return rag_pb2_grpc.RagEngineServiceStub(channel), channel

    yield start
    for server in servers:
        server.stop(grace=None)


def test_ask_returns_the_answer_and_sources(make_stub):
    stub, _ = make_stub()
    response = stub.Ask(rag_pb2.AskRequest(query="What?", document_ids=[H1]), timeout=5)
    assert response.answer == "An answer."
    (source,) = response.context_used
    assert (source.document_id, source.filename, source.snippet) == (H1, "guide.md", "chunk text")
    assert source.similarity_score == pytest.approx(0.75)


def test_ask_stream_sends_sources_first_then_deltas(make_stub):
    stub, _ = make_stub()
    messages = list(stub.AskStream(rag_pb2.AskStreamRequest(query="What?"), timeout=5))
    assert messages[0].answer == "" and len(messages[0].context_used) == 1
    assert [m.answer for m in messages[1:]] == ["An ", "answer."]
    assert all(len(m.context_used) == 0 for m in messages[1:])


def test_nothing_relevant_is_not_an_error(make_stub):
    stub, _ = make_stub(hits=())
    messages = list(stub.AskStream(rag_pb2.AskStreamRequest(query="What?"), timeout=5))
    assert [(m.answer, len(m.context_used)) for m in messages] == [("", 0), (NO_RELEVANT_ANSWER, 0)]


@pytest.mark.parametrize("query", ["", "   "])
def test_an_empty_query_is_invalid_argument_E13(make_stub, query):
    stub, _ = make_stub()
    with pytest.raises(grpc.RpcError) as excinfo:
        stub.Ask(rag_pb2.AskRequest(query=query), timeout=5)
    assert excinfo.value.code() is grpc.StatusCode.INVALID_ARGUMENT
    with pytest.raises(grpc.RpcError) as excinfo:
        list(stub.AskStream(rag_pb2.AskStreamRequest(query=query), timeout=5))
    assert excinfo.value.code() is grpc.StatusCode.INVALID_ARGUMENT


def test_a_retrieval_outage_is_unavailable(make_stub):
    stub, _ = make_stub(index_error=DependencyUnavailable("qdrant", "ConnectError"))
    with pytest.raises(grpc.RpcError) as excinfo:
        stub.Ask(rag_pb2.AskRequest(query="What?"), timeout=5)
    assert excinfo.value.code() is grpc.StatusCode.UNAVAILABLE


def test_an_llm_failure_mid_stream_ends_unavailable_after_the_sent_messages_E20(make_stub):
    llm = FakeLLM(["An ", "x"], fail_after=1, failure=DependencyUnavailable("groq", "RateLimitError"))
    stub, _ = make_stub(llm=llm)
    received = []
    with pytest.raises(grpc.RpcError) as excinfo:
        for message in stub.AskStream(rag_pb2.AskStreamRequest(query="What?"), timeout=5):
            received.append(message)
    assert excinfo.value.code() is grpc.StatusCode.UNAVAILABLE
    assert [m.answer for m in received] == ["", "An "]


def test_an_unexpected_error_is_internal_without_details(make_stub):
    stub, _ = make_stub(index_error=RuntimeError("secret detail"))
    with pytest.raises(grpc.RpcError) as excinfo:
        stub.Ask(rag_pb2.AskRequest(query="What?"), timeout=5)
    assert excinfo.value.code() is grpc.StatusCode.INTERNAL
    assert "secret" not in excinfo.value.details()


def test_cancelling_the_stream_stops_the_llm_E16(make_stub):
    llm = SlowLLM()
    stub, _ = make_stub(llm=llm)
    call = stub.AskStream(rag_pb2.AskStreamRequest(query="What?"), timeout=10)
    next(call)  # sources
    next(call)  # first delta
    call.cancel()
    assert llm.closed.wait(3), "the LLM stream was not closed after the client cancelled"
    assert llm.yielded < 200


def test_a_cancelled_stream_is_logged(make_stub, caplog):
    llm = SlowLLM()
    stub, _ = make_stub(llm=llm)
    with caplog.at_level("INFO", logger="rag.transport.grpc_server"):
        call = stub.AskStream(rag_pb2.AskStreamRequest(query="What?"), timeout=10)
        next(call)
        next(call)
        call.cancel()
        assert llm.closed.wait(3)
        deadline = time.monotonic() + 3
        while "stream ended early" not in caplog.text and time.monotonic() < deadline:
            time.sleep(0.05)
    assert "stream ended early" in caplog.text


def test_calls_beyond_max_workers_are_rejected_not_queued():
    llm = SlowLLM()
    index = FakeIndex((HIT,))
    service = QueryService(FakeEmbedder(), index, llm, SETTINGS)
    server, port = serve(service, port=0, max_workers=1, host="127.0.0.1")
    try:
        stub = rag_pb2_grpc.RagEngineServiceStub(grpc.insecure_channel(f"127.0.0.1:{port}"))
        busy = stub.AskStream(rag_pb2.AskStreamRequest(query="What?"), timeout=10)
        next(busy)  # the only worker is now streaming
        with pytest.raises(grpc.RpcError) as excinfo:
            stub.Ask(rag_pb2.AskRequest(query="What?"), timeout=5)
        assert excinfo.value.code() is grpc.StatusCode.RESOURCE_EXHAUSTED
        busy.cancel()
    finally:
        server.stop(grace=None)


def test_reflection_lists_the_service(make_stub):
    _, channel = make_stub()
    stub = reflection_pb2_grpc.ServerReflectionStub(channel)
    responses = stub.ServerReflectionInfo(iter([reflection_pb2.ServerReflectionRequest(list_services="")]), timeout=5)
    names = {s.name for r in responses for s in r.list_services_response.service}
    assert SERVICE_NAME in names
    assert SERVICE_NAME == "rag.v1.RagEngineService"
