"""Document bytes to text, one parser per supported extension (PRD L10).

A parser reports what it found: deciding that empty text is a failure (EMPTY_TEXT) is the ingest
service's job. Input that cannot be read is CORRUPT_FILE, a permanent failure: retrying the same
bytes cannot help. Error details never include document text.
"""

from __future__ import annotations

import io
import logging
import os

from pypdf import PdfReader

from rag.ingest.errors import PermanentError
from rag.transport.status_event import Reason

logger = logging.getLogger(__name__)

# An extension in an error detail comes from the event's filename: keep it short.
_MAX_EXTENSION_IN_DETAIL = 16


class PdfParser:
    def parse(self, data: bytes) -> str:
        # pypdf raises many exception types on malformed input (PdfReadError, ValueError,
        # KeyError, ...), so any failure to read the file counts as a corrupt file.
        try:
            reader = PdfReader(io.BytesIO(data))
            if reader.is_encrypted and not reader.decrypt(""):
                raise PermanentError(Reason.CORRUPT_FILE, "encrypted PDF")
            return "\n".join(page.extract_text() or "" for page in reader.pages)
        except PermanentError:
            raise
        except Exception as exc:
            logger.info("PDF could not be read: %s", type(exc).__name__)
            raise PermanentError(Reason.CORRUPT_FILE, f"unreadable PDF ({type(exc).__name__})") from None


class TextParser:
    def parse(self, data: bytes) -> str:
        try:
            return data.decode("utf-8-sig")  # tolerates a UTF-8 byte order mark
        except UnicodeDecodeError:
            raise PermanentError(Reason.CORRUPT_FILE, "not valid UTF-8") from None


class MarkdownParser(TextParser):
    """Markdown is read as UTF-8 text; the chunker splits it on its headings."""


_PARSERS: dict[str, type[PdfParser] | type[TextParser]] = {
    ".pdf": PdfParser,
    ".txt": TextParser,
    ".md": MarkdownParser,
}


def extension(filename: str) -> str:
    """The lowercase extension: ".PDF" -> ".pdf"; "notes" and ".pdf" (a dotfile) -> ""."""
    return os.path.splitext(filename)[1].lower()


def parser_for(filename: str) -> PdfParser | TextParser:
    """The parser for the filename's extension (case-insensitive). Anything else is a permanent
    UNSUPPORTED_TYPE: the Python service is authoritative on supported types (PRD L10)."""
    ext = extension(filename)
    parser = _PARSERS.get(ext)
    if parser is None:
        raise PermanentError(Reason.UNSUPPORTED_TYPE, f"unsupported extension {ext[:_MAX_EXTENSION_IN_DETAIL]!r}")
    return parser()
