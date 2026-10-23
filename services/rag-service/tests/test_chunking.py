"""R4 step 6: chunking by extension (spec A3)."""

from rag.ingest.chunking import split


def test_chunks_respect_the_size_and_overlap():
    text = " ".join(f"word{i}" for i in range(400))
    chunks = split(text, ".txt", size=100, overlap=20)
    assert len(chunks) > 1
    assert all(len(c) <= 100 for c in chunks)
    # Overlap: each chunk starts with text the previous one ended with.
    assert chunks[1].split()[0] in chunks[0]


def test_short_text_is_one_chunk():
    assert split("one short line", ".txt", size=100, overlap=10) == ["one short line"]


def test_whitespace_only_chunks_are_dropped():
    assert split("   \n\n  \t ", ".txt", size=10, overlap=2) == []


def test_markdown_splits_on_headings_before_paragraphs():
    section = "x " * 30
    text = f"# One\n\n{section}\n\n## Two\n\n{section}"
    chunks = split(text, ".md", size=80, overlap=0)
    assert chunks[0].startswith("# One")
    assert any(c.startswith("## Two") for c in chunks)


def test_the_extension_is_case_insensitive():
    text = f"# One\n\n{'x ' * 30}\n\n## Two\n\n{'y ' * 30}"
    assert split(text, ".MD", size=80, overlap=0) == split(text, ".md", size=80, overlap=0)
