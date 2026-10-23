"""Checks the consumer and the server run before doing any work (spec R5, R7, A7, A8).

Each failure is a StartupError whose message says how to fix it; entry points log it and exit.
"""

from __future__ import annotations

from rag.ports import DependencyUnavailable, Embedder, ModelNotAvailable, VectorIndex


class StartupError(Exception):
    """The process cannot start; the message says how to fix it.

    transient: a dependency is unreachable, so a restart may succeed (entry points exit 1 and
    compose restarts them). Otherwise the setup is wrong and only a fix helps (exit 2)."""

    def __init__(self, message: str, transient: bool = False) -> None:
        super().__init__(message)
        self.transient = transient


def check_embedding_dim(embedder: Embedder, expected: int, model: str) -> None:
    """The model's output size must match the collection's, or every upsert would fail later."""
    try:
        actual = embedder.dimension()
    except ModelNotAvailable:
        raise StartupError(f"embedding model {model!r} is not available in Ollama: run `ollama pull {model}`") from None
    except DependencyUnavailable as exc:
        raise StartupError(f"cannot reach the embedder to check its dimension ({exc})", transient=True) from None
    if actual != expected:
        raise StartupError(
            f"embedding model {model!r} outputs {actual} dimensions but [rag].embedding_dim is {expected}: "
            "set embedding_dim to the model's size, then `make reset_index ARGS=--yes` (or the documents "
            "are skipped as already indexed), drop the Qdrant collection and `make run_migrations`; "
            "or use the model the collection was built for"
        )


def check_collection(index: VectorIndex, name: str) -> None:
    try:
        exists = index.collection_exists()
    except DependencyUnavailable as exc:
        raise StartupError(f"cannot reach Qdrant to check collection {name!r} ({exc})", transient=True) from None
    if not exists:
        raise StartupError(f"Qdrant collection {name!r} does not exist: run `make run_migrations`")
