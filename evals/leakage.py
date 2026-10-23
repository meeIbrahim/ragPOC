"""Question leakage check for the retrieval eval (spec D11).

A question that copies a run of words from its source document is found by word overlap alone,
so it inflates hit@k without testing retrieval. The eval refuses to run while any question shares
an n-word sequence (default 5) with its document's parsed text.
"""

from __future__ import annotations

import re

DEFAULT_N = 5

_NON_WORD = re.compile(r"[^0-9a-z]+")


def normalise(text: str) -> list[str]:
    """Lowercase words with punctuation and whitespace removed."""
    return [word for word in _NON_WORD.split(text.lower()) if word]


def _ngrams(words: list[str], n: int) -> set[tuple[str, ...]]:
    return {tuple(words[i : i + n]) for i in range(len(words) - n + 1)}


def shared_ngrams(question: str, document_text: str, n: int = DEFAULT_N) -> set[str]:
    """The n-word sequences the question shares with the document, as plain strings."""
    shared = _ngrams(normalise(question), n) & _ngrams(normalise(document_text), n)
    return {" ".join(gram) for gram in shared}
