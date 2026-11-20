"""compose-demo D3, A4: the synthetic demo PDF is readable by the ingest parser and reproducible."""

import subprocess
import sys
from pathlib import Path

from pypdf import PdfReader

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / "demo" / "make_pdf.py"
COMMITTED = REPO / "demo" / "pucho-demo.pdf"


def _text(path: Path) -> str:
    pages = [p.extract_text() or "" for p in PdfReader(path).pages]
    return " ".join(" ".join(pages).split())


def test_pypdf_extracts_the_intro_and_the_excerpt():
    text = _text(COMMITTED)
    # The intro is what the demo's PDF question asks about; it must survive extraction.
    assert 'reproduces the opening of the chapter "The Carew Murder Case"' in text, text[:400]
    # Curly quotes and the em dash come through WinAnsiEncoding intact.
    assert "in the month of October, 18—, London was startled by a crime of singular ferocity" in text
    assert "which the maid’s window overlooked" in text


def test_the_pdf_has_one_or_two_pages():
    assert 1 <= len(PdfReader(COMMITTED).pages) <= 2


def test_the_script_reproduces_the_committed_bytes(tmp_path):
    out = tmp_path / "pucho-demo.pdf"
    subprocess.run([sys.executable, "-I", str(SCRIPT), str(out)], check=True, capture_output=True)
    assert out.read_bytes() == COMMITTED.read_bytes(), "rerun python demo/make_pdf.py and commit the result"
