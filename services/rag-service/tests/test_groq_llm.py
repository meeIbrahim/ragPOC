"""R2 GroqLLM: answer deltas only (L16, D5, E17), errors (E15), stream closed on early stop (E16)."""

import os
from types import SimpleNamespace

import httpx
import pytest
from groq import APIConnectionError, APITimeoutError, AuthenticationError, RateLimitError

from rag.adapters.groq_llm import GroqLLM
from rag.ports import DependencyUnavailable

REQUEST = httpx.Request("POST", "https://api.groq.com/openai/v1/chat/completions")


def chunk(content=None, reasoning=None, choices=True):
    delta = SimpleNamespace(content=content, reasoning=reasoning)
    return SimpleNamespace(choices=[SimpleNamespace(delta=delta)] if choices else [])


class FakeStream:
    def __init__(self, chunks, fail_with=None):
        self.chunks = chunks
        self.fail_with = fail_with
        self.closed = False

    def __iter__(self):
        yield from self.chunks
        if self.fail_with:
            raise self.fail_with

    def close(self):
        self.closed = True


class FakeGroq:
    def __init__(self, stream=None, error=None):
        self.stream = stream
        self.error = error
        self.requests = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.requests.append(kwargs)
        if self.error:
            raise self.error
        return self.stream


def llm(client):
    return GroqLLM(client, model="openai/gpt-oss-120b", reasoning_effort="low")


def test_requests_a_stream_without_reasoning_at_the_configured_effort():
    client = FakeGroq(FakeStream([chunk("hi")]))
    list(llm(client).stream("system text", "user text"))
    (request,) = client.requests
    assert request["model"] == "openai/gpt-oss-120b"
    assert request["messages"] == [
        {"role": "system", "content": "system text"},
        {"role": "user", "content": "user text"},
    ]
    assert request["stream"] is True
    assert request["include_reasoning"] is False
    assert request["reasoning_effort"] == "low"
    assert "reasoning_format" not in request  # not supported for gpt-oss


def test_yields_only_non_empty_answer_content():
    stream = FakeStream([
        chunk(reasoning="thinking..."),  # a reasoning delta must never be forwarded
        chunk(content=""),
        chunk(choices=False),            # e.g. a final usage-only chunk
        chunk(content="The answer"),
        chunk(content=" [1]."),
    ])
    assert list(llm(FakeGroq(stream)).stream("s", "u")) == ["The answer", " [1]."]
    assert stream.closed


def test_stopping_early_closes_the_stream():
    stream = FakeStream([chunk("a"), chunk("b"), chunk("c")])
    deltas = llm(FakeGroq(stream)).stream("s", "u")
    assert next(deltas) == "a"
    deltas.close()  # what the gRPC servicer does when the client goes away
    assert stream.closed


RESPONSE_429 = httpx.Response(429, request=REQUEST)
RESPONSE_401 = httpx.Response(401, request=REQUEST)


@pytest.mark.parametrize(
    "error",
    [
        APIConnectionError(request=REQUEST),
        APITimeoutError(request=REQUEST),
        RateLimitError("rate limited", response=RESPONSE_429, body=None),
        AuthenticationError("bad key", response=RESPONSE_401, body=None),
    ],
    ids=["connection", "timeout", "429", "401"],
)
def test_request_failures_are_dependency_unavailable(error):
    with pytest.raises(DependencyUnavailable):
        list(llm(FakeGroq(error=error)).stream("s", "u"))


def test_a_failure_mid_stream_is_dependency_unavailable_after_the_deltas_already_sent():
    stream = FakeStream([chunk("partial")], fail_with=httpx.RemoteProtocolError("peer closed"))
    deltas = llm(FakeGroq(stream)).stream("s", "u")
    assert next(deltas) == "partial"
    with pytest.raises(DependencyUnavailable):
        next(deltas)
    assert stream.closed


# ---- the real Groq API (needs GROQ_API_KEY; sends one short, non-confidential prompt)


@pytest.mark.groq
def test_a_short_real_answer_has_no_reasoning_text():
    from groq import Groq

    if not os.environ.get("GROQ_API_KEY"):
        pytest.skip("set GROQ_API_KEY")
    real = GroqLLM(Groq(timeout=60, max_retries=0), model="openai/gpt-oss-120b", reasoning_effort="low")
    answer = "".join(real.stream("Reply with exactly the word: pong", "ping"))
    assert "pong" in answer.lower()
    assert len(answer) < 200


def test_full_width_citation_brackets_become_ascii_even_across_deltas():
    # gpt-oss sometimes cites as 【1】 despite the prompt; the brackets often arrive as separate deltas.
    stream = FakeStream([chunk("Govern 【"), chunk("1"), chunk("】【5】.")])
    assert "".join(llm(FakeGroq(stream)).stream("s", "u")) == "Govern [1][5]."
