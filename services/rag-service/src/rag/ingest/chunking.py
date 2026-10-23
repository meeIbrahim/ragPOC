"""Text to chunks (PRD 5.2, spec R4 step 6). Sizes are characters, not tokens."""

from __future__ import annotations

from langchain_text_splitters import Language, RecursiveCharacterTextSplitter


def split(text: str, extension: str, size: int, overlap: int) -> list[str]:
    """Markdown splits on headings first, so a section stays with its heading; other text uses
    the default paragraph, line, word order. Whitespace-only chunks are dropped."""
    if extension.lower() == ".md":
        splitter = RecursiveCharacterTextSplitter.from_language(
            Language.MARKDOWN, chunk_size=size, chunk_overlap=overlap
        )
    else:
        splitter = RecursiveCharacterTextSplitter(chunk_size=size, chunk_overlap=overlap)
    return [chunk for chunk in splitter.split_text(text) if chunk.strip()]
