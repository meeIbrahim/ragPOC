"""R6 QueryService with port fakes (PRD 4.4, L16; spec D1, D9, A9, E13, E14, E20, E21)."""

import pytest

from fakes import FakeEmbedder, FakeIndex, FakeLLM
from rag.ports import DependencyUnavailable, Hit
from rag.query.service import (
    NO_RELEVANT_ANSWER,
    InvalidQuery,
    Message,
    QueryService,
    QuerySettings,
    QueryUnavailable,
    build_user_message,
)

H1, H2 = "a" * 64, "b" * 64
HITS = [Hit(H1, "guide.md", 0, "low text", 0.3), Hit(H2, "notes.txt", 4, "high text", 0.8)]
SETTINGS = QuerySettings(top_k=5, min_score=0.25, max_query_chars=50, max_document_ids=2, system_prompt="SYSTEM")


def service(hits=HITS, llm=None, embedder=None):
    parts = {"embedder": embedder or FakeEmbedder(), "index": FakeIndex(hits), "llm": llm or FakeLLM(["The ", "answer [1]."])}
    return QueryService(settings=SETTINGS, **parts), parts


def test_stream_sends_sources_first_then_only_deltas():
    svc, _ = service()
    messages = list(svc.ask_stream("What is X?", []))
    assert messages[0] == Message(sources=[HITS[1], HITS[0]], answer="")
    assert messages[1:] == [Message([], "The "), Message([], "answer [1].")]


def test_ask_joins_the_deltas_and_returns_the_sources():
    svc, _ = service()
    assert svc.ask("What is X?", []) == ("The answer [1].", [HITS[1], HITS[0]])


def test_retrieval_uses_the_query_embedding_top_k_threshold_and_filter():
    svc, parts = service()
    list(svc.ask_stream("What is X?", [H1]))
    assert parts["embedder"].calls == [("embed_query", ("What is X?",))]
    (_, (vector, top_k, min_score, hashes)), = parts["index"].calls
    assert (top_k, min_score, hashes) == (5, 0.25, [H1])


def test_the_llm_gets_the_system_prompt_and_numbered_context_blocks():
    svc, parts = service()
    list(svc.ask_stream("What is X?", []))
    (_, (system, user)), = parts["llm"].calls
    assert system == "SYSTEM"
    assert user == build_user_message("What is X?", [HITS[1], HITS[0]])
    assert user == "Context:\n\n[1] (notes.txt)\nhigh text\n\n[2] (guide.md)\nlow text\n\nQuestion: What is X?"


@pytest.mark.parametrize("hits,ids", [([], []), (HITS, ["c" * 64]), ([Hit(H1, "g.md", 0, "t", 0.1)], [])],
                         ids=["empty-collection", "unindexed-hash", "below-threshold"])
def test_no_relevant_chunks_give_the_fixed_answer_without_calling_the_llm_E14(hits, ids):
    svc, parts = service(hits=hits)
    assert list(svc.ask_stream("What is X?", ids)) == [Message([], ""), Message([], NO_RELEVANT_ANSWER)]
    assert svc.ask("What is X?", ids) == (NO_RELEVANT_ANSWER, [])
    assert parts["llm"].calls == []


@pytest.mark.parametrize("query", ["", "   \n\t"])
def test_an_empty_query_is_invalid_E13(query):
    svc, parts = service()
    with pytest.raises(InvalidQuery):
        svc.ask_stream(query, [])
    assert parts["embedder"].calls == []


def test_limits_are_enforced_before_any_call_E21():
    svc, parts = service()
    with pytest.raises(InvalidQuery, match="50"):
        svc.ask_stream("x" * 51, [])
    with pytest.raises(InvalidQuery, match="2"):
        svc.ask_stream("ok", [H1, H2, "c" * 64])
    assert parts["embedder"].calls == [] and parts["index"].calls == []
    svc.ask_stream("x" * 50, [H1, H2])  # exactly at the limits is fine


@pytest.mark.parametrize("failing", ["embedder", "index"])
def test_retrieval_outages_are_unavailable_before_any_message(failing):
    svc, parts = service()
    parts[failing].error = DependencyUnavailable(failing, "ConnectError")
    with pytest.raises(QueryUnavailable):
        svc.ask_stream("What is X?", [])


def test_an_llm_failure_after_the_sources_ends_the_stream_unavailable_E20():
    svc, _ = service(llm=FakeLLM(["The ", "rest"], fail_after=1, failure=DependencyUnavailable("groq", "RateLimitError")))
    stream = svc.ask_stream("What is X?", [])
    assert next(stream).sources  # sources were already sent
    assert next(stream) == Message([], "The ")
    with pytest.raises(QueryUnavailable):
        next(stream)


def test_unavailable_messages_never_carry_chunk_text():
    svc, _ = service(llm=FakeLLM(["x"], fail_after=0, failure=DependencyUnavailable("groq", "APIError")))
    with pytest.raises(QueryUnavailable) as excinfo:
        svc.ask("What is X?", [])
    assert "high text" not in str(excinfo.value) and "low text" not in str(excinfo.value)


def test_retrieve_can_override_the_threshold_for_the_eval():
    svc, parts = service()
    assert svc.retrieve("What is X?", [], min_score=0.0) == [HITS[1], HITS[0]]
    assert parts["index"].calls[0][1][2] == 0.0
