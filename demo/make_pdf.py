"""Builds demo/pucho-demo.pdf, the synthetic PDF for `make demo` (compose-demo D3, A4).

Stdlib only. The page text is a short intro written for the demo plus the opening of "The
Carew Murder Case" from evals/corpus/jekyll-and-hyde.txt (public domain, Project Gutenberg).
The intro gives the demo a question only this PDF answers, so it ranks first even with the
whole book indexed.

Deterministic: uncompressed content streams, no timestamps and no /ID, so a rebuild gives the
same bytes. Usage: python demo/make_pdf.py [OUT] (default demo/pucho-demo.pdf).
"""

import sys
import textwrap
from pathlib import Path

HERE = Path(__file__).resolve().parent
CORPUS = HERE.parent / "evals" / "corpus" / "jekyll-and-hyde.txt"
DEFAULT_OUT = HERE / "pucho-demo.pdf"

TITLE = "Pucho demo sample"
INTRO = (
    "This two-page sample was made for the Pucho demo. It reproduces the opening of the chapter "
    '"The Carew Murder Case" from The Strange Case of Dr Jekyll and Mr Hyde by Robert Louis '
    "Stevenson (1886, public domain, text from Project Gutenberg). The file is synthetic: "
    "demo/make_pdf.py generates it, so it indexes in seconds."
)
EXCERPT_START = "Nearly a year later, in the month of October"
EXCERPT_PARAGRAPHS = 2

# US Letter, 1 inch margins, Helvetica 11/15. About 84 characters fit the 468 pt line.
PAGE_W, PAGE_H, MARGIN = 612, 792, 72
FONT_SIZE, LEADING, WRAP = 11, 15, 84
LINES_PER_PAGE = (PAGE_H - 2 * MARGIN) // LEADING


def excerpt() -> list[str]:
    text = CORPUS.read_text(encoding="utf-8").replace("\r\n", "\n")
    start = text.index(EXCERPT_START)
    paragraphs = text[start:].split("\n\n")[:EXCERPT_PARAGRAPHS]
    return [" ".join(p.split()) for p in paragraphs]


def lines() -> list[str]:
    out = [TITLE, ""]
    for paragraph in [INTRO, *excerpt()]:
        out += textwrap.wrap(paragraph, WRAP)
        out.append("")
    return out[:-1]


def pdf_string(s: str) -> bytes:
    # WinAnsiEncoding is cp1252 for every character this text uses (curly quotes, em dash).
    raw = s.encode("cp1252")
    return b"(" + raw.replace(b"\\", b"\\\\").replace(b"(", b"\\(").replace(b")", b"\\)") + b")"


def content(page_lines: list[str]) -> bytes:
    ops = [b"BT", b"/F1 %d Tf" % FONT_SIZE, b"%d TL" % LEADING, b"%d %d Td" % (MARGIN, PAGE_H - MARGIN)]
    for line in page_lines:
        ops.append(pdf_string(line) + b" Tj T*")
    ops.append(b"ET")
    return b"\n".join(ops) + b"\n"


def build() -> bytes:
    all_lines = lines()
    pages = [all_lines[i : i + LINES_PER_PAGE] for i in range(0, len(all_lines), LINES_PER_PAGE)]
    # 1 catalog, 2 pages, 3 font, then a page object and its content stream per page.
    objects: list[bytes] = []
    kids = " ".join(f"{4 + 2 * i} 0 R" for i in range(len(pages)))
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(f"<< /Type /Pages /Kids [{kids}] /Count {len(pages)} >>".encode())
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>")
    for i, page in enumerate(pages):
        stream = content(page)
        objects.append(
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {PAGE_W} {PAGE_H}] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {5 + 2 * i} 0 R >>".encode()
        )
        objects.append(b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"endstream")

    out = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = []
    for n, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % n + body + b"\nendobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objects) + 1)
    for off in offsets:
        out += b"%010d 00000 n \n" % off
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objects) + 1, xref)
    return bytes(out)


def main() -> None:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUT
    out.write_bytes(build())
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
