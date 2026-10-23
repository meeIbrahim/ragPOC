"""evals/run_eval.py: question checks (D11, E24) and the per-document pass bar (D7)."""

import hashlib
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_EVALS = Path(__file__).resolve().parents[3] / "evals"
sys.path.insert(0, str(_EVALS))  # run_eval imports its sibling `leakage` module
_SPEC = importlib.util.spec_from_file_location("run_eval", _EVALS / "run_eval.py")
run_eval = importlib.util.module_from_spec(_SPEC)
sys.modules["run_eval"] = run_eval  # dataclasses look their module up while it executes
_SPEC.loader.exec_module(run_eval)
Question, Result = run_eval.Question, run_eval.Result

DOC = "The lighthouse keeper rowed to the island every Tuesday to fetch fresh water."


@pytest.fixture
def corpus(tmp_path):
    (tmp_path / "keeper.txt").write_text(DOC)
    return tmp_path, hashlib.sha256(DOC.encode()).hexdigest()


def test_a_clean_question_set_passes(corpus):
    path, digest = corpus
    assert run_eval.check_questions([Question("q1", "On which weekday did he travel by boat?", "keeper.txt", digest)], path) == []


def test_a_leaked_phrase_is_reported(corpus):
    path, digest = corpus
    problems = run_eval.check_questions([Question("q1", "Who rowed to the island every Tuesday?", "keeper.txt", digest)], path)
    assert problems and all(p.startswith("q1: copies 5+ words") for p in problems)


def test_a_wrong_hash_and_a_missing_file_are_reported(corpus):
    path, _ = corpus
    problems = run_eval.check_questions(
        [Question("q1", "On which weekday?", "keeper.txt", "0" * 64), Question("q2", "Who?", "gone.txt", "0" * 64)], path
    )
    assert any("q1: expected_hash" in p for p in problems)
    assert any("q2: file 'gone.txt'" in p for p in problems)


def results(file, hits, total):
    return [Result(Question(f"{file}{i}", "q", file, "h"), 1 if i < hits else None, 0.5 if i < hits else None, 0.5)
            for i in range(total)]


CONFIG = SimpleNamespace(rag=SimpleNamespace(embedding_model="m", embedding_dim=4, chunk_size=1000,
                                             chunk_overlap=150, top_k=5, query_instruction="i"))


@pytest.mark.parametrize("hits,passed", [(8, True), (7, False)])
def test_the_bar_is_eighty_percent_per_document(hits, passed):
    text, ok = run_eval.report(CONFIG, results("a.md", 10, 10) + results("b.md", hits, 10), [])
    assert ok is passed
    assert ("BELOW BAR" in text) is not passed


def test_an_overlap_between_negatives_and_correct_hits_is_reported():
    negatives = [Result(Question("n1", "q", "", None), None, None, 0.9)]
    text, _ = run_eval.report(CONFIG, results("a.md", 10, 10), negatives)
    assert "OVERLAP" in text


def test_an_unreadable_question_file_is_a_setup_error(tmp_path):
    bad = tmp_path / "q.jsonl"
    bad.write_text('{"id": "q1"}\n')  # no question, file or hash
    assert run_eval.main(["--questions", str(bad), "--negatives", str(tmp_path / "none.jsonl"), "--check-only"]) == 2


def test_an_outage_during_the_run_is_a_setup_error_not_below_bar(corpus, monkeypatch):
    path, digest = corpus
    questions = tmp = path / "q.jsonl"
    tmp.write_text('{"id": "q1", "question": "On which weekday?", "file": "keeper.txt", "expected_hash": "%s"}\n' % digest)
    monkeypatch.setattr(run_eval.config_reader, "load", lambda path: object())

    def down(config, files):
        raise run_eval.DependencyUnavailable("qdrant", "ConnectError")

    monkeypatch.setattr(run_eval, "ingest_corpus", down)
    assert run_eval.main(["--corpus", str(path), "--questions", str(questions),
                          "--negatives", str(path / "none.jsonl")]) == 2
