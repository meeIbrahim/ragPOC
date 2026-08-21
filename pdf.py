from pathlib import Path

from langchain_community.document_loaders import PyPDFLoader
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from db import DocDB

def fixedChunks(pdf_dir: str, chunk_size: int = 1000, chunk_overlap: int = 200) -> list[Document]:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    )

    chunks: list[Document] = []
    for pdf_path in Path(pdf_dir).glob("*.pdf"):
        print(f"Loading: {pdf_path}")
        db =  DocDB(pdf_path.name)
        pdf_chunks, doc_id = ingest_pdf(pdf_path, db, splitter)
        chunks.extend(pdf_chunks)
    return chunks

def ingest_pdf(path: Path, db: DocDB, splitter: RecursiveCharacterTextSplitter) -> tuple[list[Document], int]:
    to_index, doc_id, hash = db.needs_indexing(path)
    if not to_index:
        print(f"Skipping unchanged: {path}")
    if doc_id is not None:
        print(f"Document changed: {path}")

    loader = PyPDFLoader(str(path))
    documents = loader.load()
    chunks = splitter.split_documents(documents)
    if doc_id is None:
        doc_id = db.save_document(path, hash)
    else:
        db.save_document(path, hash)
    return (chunks, doc_id)
