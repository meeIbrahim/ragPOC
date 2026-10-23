"""R2 parsers and extension dispatch (PRD L10, 5.2.5; spec E4-E7, A2)."""

from pathlib import Path

import pytest

from rag.adapters.parsers import MarkdownParser, PdfParser, TextParser, parser_for
from rag.ingest.errors import PermanentError
from rag.transport.status_event import Reason

FIXTURES = Path(__file__).resolve().parent / "fixtures"
TEXT = "The quick brown fox jumps over the lazy dog."


def data(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def test_pdf_text_is_extracted():
    assert PdfParser().parse(data("text.pdf")).strip() == TEXT


def test_an_image_only_pdf_has_no_text():
    # EMPTY_TEXT is decided by the ingest service, not the parser: it only reports what it found.
    assert PdfParser().parse(data("scanned.pdf")).strip() == ""


@pytest.mark.parametrize("name", ["corrupt.pdf", "encrypted.pdf"])
def test_unreadable_pdfs_are_corrupt_files(name):
    with pytest.raises(PermanentError) as excinfo:
        PdfParser().parse(data(name))
    assert excinfo.value.reason is Reason.CORRUPT_FILE


def test_garbage_bytes_are_a_corrupt_pdf():
    with pytest.raises(PermanentError) as excinfo:
        PdfParser().parse(b"not a pdf at all")
    assert excinfo.value.reason is Reason.CORRUPT_FILE


def test_text_drops_a_utf8_bom():
    assert TextParser().parse(data("bom.txt")) == TEXT


def test_markdown_is_read_as_utf8_text():
    assert MarkdownParser().parse(data("doc.md")).startswith("# Title")


@pytest.mark.parametrize("parser", [TextParser(), MarkdownParser()])
def test_invalid_utf8_is_a_corrupt_file(parser):
    with pytest.raises(PermanentError) as excinfo:
        parser.parse(data("bad_utf8.txt"))
    assert excinfo.value.reason is Reason.CORRUPT_FILE


def test_errors_never_carry_document_text():
    with pytest.raises(PermanentError) as excinfo:
        TextParser().parse(b"secret contents \xff")
    assert "secret" not in str(excinfo.value)


@pytest.mark.parametrize(
    "filename,kind",
    [("a.pdf", PdfParser), ("a.PDF", PdfParser), ("notes.txt", TextParser), ("README.Md", MarkdownParser),
     ("archive.tar.md", MarkdownParser)],
)
def test_dispatch_is_by_extension_case_insensitive(filename, kind):
    assert type(parser_for(filename)) is kind


@pytest.mark.parametrize("filename", ["report.docx", "noextension", ".pdf", "a.pdf.exe", ""])
def test_unsupported_extensions_fail_permanently(filename):
    with pytest.raises(PermanentError) as excinfo:
        parser_for(filename)
    assert excinfo.value.reason is Reason.UNSUPPORTED_TYPE
