import os
import re
import shutil
from pathlib import Path

import chromadb
from chromadb.utils import embedding_functions
from langchain_text_splitters import RecursiveCharacterTextSplitter
from dotenv import load_dotenv

load_dotenv()

DOCS_DIR = Path("docs")
CHROMA_DIR = Path("chroma_db")
COLLECTION_NAME = "portfolio"
CHUNK_SIZE = 500
CHUNK_OVERLAP = 50


def find_current_section(text_before):
    headings = re.findall(r"^#{1,2} .+", text_before, re.MULTILINE)
    if headings:
        return headings[-1].lstrip("#").strip()
    return "Introduction"


def get_doc_type(filepath):
    parts = filepath.parts
    if "projects" in parts:
        return "project"
    if "profile" in parts:
        return "profile"
    if "modelling" in parts:
        return "modelling"
    return "general"


KNOWLEDGE_BASE_DIRS = ["projects", "profile", "modelling"]


def load_documents():
    docs = []
    for subdir in KNOWLEDGE_BASE_DIRS:
        for md_file in (DOCS_DIR / subdir).rglob("*.md"):
            text = md_file.read_text(encoding="utf-8")
            docs.append({
                "text": text,
                "source": md_file.stem,
                "type": get_doc_type(md_file),
            })
            print(f"  Loaded: {md_file.name} ({len(text)} chars)")
    return docs


def chunk_documents(docs):
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
    )
    chunks = []
    for doc in docs:
        splits = splitter.split_text(doc["text"])
        for i, split_text in enumerate(splits):
            position = doc["text"].find(split_text)
            text_before = doc["text"][:position]
            section = find_current_section(text_before)
            chunks.append({
                "text": split_text,
                "source": doc["source"],
                "section": section,
                "type": doc["type"],
                "chunk_id": f"{doc['source']}_chunk_{i}",
            })
    return chunks


def build_index(chunks):
    # DefaultEmbeddingFunction uses all-MiniLM-L6-v2 via ONNX (no torch needed)
    embedding_fn = embedding_functions.DefaultEmbeddingFunction()

    if CHROMA_DIR.exists():
        shutil.rmtree(CHROMA_DIR)
        print("  Cleared existing chroma_db directory.")

    client = chromadb.PersistentClient(path=str(CHROMA_DIR))

    collection = client.create_collection(
        name=COLLECTION_NAME,
        embedding_function=embedding_fn,
        metadata={"hnsw:space": "cosine"},
    )

    texts = [c["text"] for c in chunks]
    ids = [c["chunk_id"] for c in chunks]
    metadatas = [
        {"source": c["source"], "section": c["section"], "type": c["type"]}
        for c in chunks
    ]

    collection.add(documents=texts, ids=ids, metadatas=metadatas)
    print(f"  Indexed {len(chunks)} chunks into ChromaDB.")


def main():
    print("Loading documents...")
    docs = load_documents()
    print(f"  {len(docs)} documents loaded.\n")

    print("Chunking documents...")
    chunks = chunk_documents(docs)
    print(f"  {len(chunks)} chunks created.\n")

    print("Building ChromaDB index...")
    build_index(chunks)
    print(f"\nDone. Vector store saved to: {CHROMA_DIR}/")


if __name__ == "__main__":
    main()
