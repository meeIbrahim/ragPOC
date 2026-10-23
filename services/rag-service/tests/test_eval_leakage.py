"""D11 leakage gate (evals/leakage.py; spec E24)."""

import importlib.util
from pathlib import Path

_PATH = Path(__file__).resolve().parents[3] / "evals" / "leakage.py"
_SPEC = importlib.util.spec_from_file_location("eval_leakage", _PATH)
leakage = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(leakage)

DOC = "The Treaty of Westphalia, signed in 1648, ended the Thirty Years' War in Europe."


def test_a_copied_five_word_run_is_found():
    question = "Which agreement ended the Thirty Years' War in Europe?"
    assert leakage.shared_ngrams(question, DOC) == {"ended the thirty years war", "the thirty years war in",
                                                     "thirty years war in europe"}


def test_four_shared_words_are_allowed():
    assert leakage.shared_ngrams("Who ended the Thirty Years conflict?", DOC) == set()


def test_case_and_punctuation_do_not_hide_a_copy():
    assert leakage.shared_ngrams("SIGNED IN 1648, ENDED -- the war?", "signed in 1648 ended the war") != set()


def test_a_paraphrase_passes():
    assert leakage.shared_ngrams("What peace settlement closed a long European conflict in the 17th century?", DOC) == set()


def test_normalise_splits_on_anything_but_letters_and_digits():
    assert leakage.normalise("Don't  stop—now!\n2026") == ["don", "t", "stop", "now", "2026"]


def test_n_is_configurable():
    assert leakage.shared_ngrams("ended the thirty", DOC, n=3) == {"ended the thirty"}
