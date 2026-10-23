"""Writes the parser fixtures next to this file. The outputs are committed; rerun only to change them.

    uv run python tests/fixtures/make_fixtures.py
"""

from __future__ import annotations

import io
from pathlib import Path

from pypdf import PdfReader, PdfWriter

HERE = Path(__file__).resolve().parent
TEXT = "The quick brown fox jumps over the lazy dog."


def _pdf(content_stream: bytes) -> bytes:
    """A one-page PDF with a Helvetica font and the given content stream, with a correct xref."""
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length %d >>\nstream\n" % len(content_stream) + content_stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = io.BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, start=1):
        offsets.append(out.tell())
        out.write(b"%d 0 obj\n" % number + body + b"\nendobj\n")
    xref = out.tell()
    out.write(b"xref\n0 %d\n0000000000 65535 f \n" % (len(objects) + 1))
    for offset in offsets:
        out.write(b"%010d 00000 n \n" % offset)
    out.write(b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objects) + 1, xref))
    return out.getvalue()


def main() -> None:
    text_pdf = _pdf(b"BT /F1 12 Tf 72 720 Td (" + TEXT.encode() + b") Tj ET")
    (HERE / "text.pdf").write_bytes(text_pdf)
    # A page that only draws a rectangle: what a scanned, image-only page looks like to a text
    # extractor (no OCR in v1).
    (HERE / "scanned.pdf").write_bytes(_pdf(b"0 0 0 rg 72 72 200 200 re f"))
    (HERE / "corrupt.pdf").write_bytes(b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog /Pages")

    writer = PdfWriter(clone_from=PdfReader(io.BytesIO(text_pdf)))
    writer.encrypt(user_password="secret", algorithm="RC4-128")
    encrypted = io.BytesIO()
    writer.write(encrypted)
    (HERE / "encrypted.pdf").write_bytes(encrypted.getvalue())

    (HERE / "bom.txt").write_bytes("﻿".encode() + TEXT.encode())
    (HERE / "bad_utf8.txt").write_bytes(b"caf\xe9 au lait")  # Latin-1, not UTF-8
    (HERE / "doc.md").write_text(f"# Title\n\n{TEXT}\n\n## Section\n\nMore text.\n", encoding="utf-8")


if __name__ == "__main__":
    main()
