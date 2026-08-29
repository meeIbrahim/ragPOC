from pathlib import Path

from langchain_community.document_loaders import PyPDFLoader
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter


def load_pdf_chunks(document_path: Path, chunk_size: int = 1000, chunk_overlap: int = 200) -> list[Document]:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    )
    loader = PyPDFLoader(str(document_path))
    documents = loader.load()
    return splitter.split_documents(documents)


def iter_pdfs(pdf_dir: Path):
    yield from sorted(pdf_dir.glob("*.pdf"))
