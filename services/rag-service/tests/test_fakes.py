"""The fakes match the ports, so a port change breaks these tests instead of silently drifting."""

import inspect

import pytest

from fakes import FakeBlobStore, FakeEmbedder, FakeIndex, FakeLLM, FakeParser, FakeRegistry
from rag import ports

PAIRS = [
    (ports.BlobStore, FakeBlobStore),
    (ports.Parser, FakeParser),
    (ports.Embedder, FakeEmbedder),
    (ports.VectorIndex, FakeIndex),
    (ports.LLM, FakeLLM),
    (ports.IndexRegistry, FakeRegistry),
]


def _methods(cls):
    return {
        name: inspect.signature(member)
        for name, member in vars(cls).items()
        if callable(member) and not name.startswith("_")
    }


@pytest.mark.parametrize("port,fake", PAIRS, ids=[p.__name__ for p, _ in PAIRS])
def test_each_fake_has_every_port_method_with_the_same_parameters(port, fake):
    for name, signature in _methods(port).items():
        assert hasattr(fake, name), f"{fake.__name__} lacks {name}"
        assert list(inspect.signature(getattr(fake, name)).parameters) == list(signature.parameters)


def test_an_injected_error_is_raised_and_the_call_is_still_recorded():
    blobs = FakeBlobStore({"k": b"x"})
    blobs.error = OSError("down")
    with pytest.raises(OSError):
        blobs.get("k", 10)
    assert blobs.names() == ["get"]


def test_fake_llm_fails_after_the_given_number_of_deltas():
    llm = FakeLLM(["a", "b", "c"], fail_after=1)
    stream = llm.stream("s", "u")
    assert next(stream) == "a"
    with pytest.raises(RuntimeError):
        next(stream)
