"""R8b `python -m rag.tools.reset_index`: dry run without --yes, clear with it (spec D10, E25)."""

from fakes import FakeIndex, FakeRegistry
from rag.ports import Point
from rag.tools import reset_index


def filled():
    index = FakeIndex()
    index.upsert([Point("a" * 64, "f.md", i, "t", [0.1]) for i in range(3)])
    registry = FakeRegistry(["a" * 64])
    index.calls.clear()
    registry.calls.clear()
    return index, registry


def test_without_yes_it_reports_and_deletes_nothing_E25(capsys):
    index, registry = filled()
    assert reset_index.run(index, registry, collection="pdf-fixed", yes=False) == 1
    out = capsys.readouterr().out
    assert "3 points" in out and "1 indexed_documents rows" in out and "--yes" in out
    assert "clear" not in index.names() and "clear" not in registry.names()


def test_with_yes_it_clears_both(capsys):
    index, registry = filled()
    assert reset_index.run(index, registry, collection="pdf-fixed", yes=True) == 0
    assert index.count() == 0 and registry.count() == 0
    assert "deleted 3 points" in capsys.readouterr().out


def test_the_registry_rows_go_before_the_points():
    # A crash in between then leaves points without a row, which the next ingest of that document
    # replaces (delete-by-hash). The reverse order could leave rows that skip ingest for documents
    # whose points are gone.
    index, registry = filled()
    order = []
    index.clear = lambda: order.append("points")
    registry.clear = lambda: order.append("rows")
    reset_index.run(index, registry, collection="pdf-fixed", yes=True)
    assert order == ["rows", "points"]


def test_a_missing_collection_still_clears_the_registry(capsys):
    # After a collection drop (embedding_dim change) the rows would make every re-ingest skip.
    index, registry = filled()
    index.exists = False
    assert reset_index.run(index, registry, collection="pdf-fixed", yes=True) == 0
    assert registry.count() == 0
    assert "count" not in index.names() and "clear" not in index.names()
    assert "does not exist" in capsys.readouterr().out
