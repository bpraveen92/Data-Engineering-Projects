# Execution Guide

## Prerequisites

- Python 3.11+
- All markdown documentation files placed inside the `docs/` folder

---

## 1. Install Dependencies

```bash
cd portfolio-rag-assistant
pip install -r requirements.txt
# or
make install
```

---

## 2. Configure Environment

```bash
cp .env.example .env
# Edit .env and add the key:
# GEMINI_API_KEY=your_key_here
```

---

## 3. Build the Vector Index

This is a one-time step that reads the docs, chunks them, embeds them, and saves the vector store to `chroma_db/`.

```bash
make index
# or
python scripts/build_index.py
```

What happens under the hood:

**Step 1 — Load documents**
`build_index.py` recursively reads all `.md` files from `docs/` and records the source filename and document type (project / profile / modelling) for each.

```
Loading documents...
  Loaded: professional-bio.md (9212 chars)
  Loaded: databricks-f1-intelligence-PIPELINE_DEEP_DIVE.md (83240 chars)
  ...
  9 documents loaded.
```

**Step 2 — Chunk**
Each document is split into 500-character chunks with a 50-character overlap using `RecursiveCharacterTextSplitter`. The overlap ensures context isn't lost at section boundaries. Each chunk also captures its nearest heading as a `section` metadata field.

```
Chunking documents...
  1249 chunks created.
```

**Step 3 — Embed and store**
Each chunk is converted into a 384-dimensional vector using `all-MiniLM-L6-v2` via ChromaDB's built-in `DefaultEmbeddingFunction`.

I picked this model for a few practical reasons. It's a sentence-transformer specifically trained for semantic similarity — meaning it maps text with similar meaning to nearby points in vector space, which is exactly what retrieval depends on. It's also very lightweight at 22M parameters, runs via ONNX without needing PyTorch, and has zero cost or rate limits since everything runs locally. For a project at this scale (under 1500 chunks of short technical text), it hits a good balance between quality and simplicity. Larger models like `text-embedding-3-large` would produce richer vectors but would require an OpenAI API key, add cost, and introduce a runtime dependency — none of which I wanted for what is essentially a personal project.

ChromaDB stores the vectors using cosine similarity as the distance metric, which works well here because we care about the angle between vectors (semantic direction) rather than their magnitude.

```
Building ChromaDB index...
  Deleted existing collection.
  Indexed 1249 chunks into ChromaDB.

Done. Vector store saved to: chroma_db/
```

The `chroma_db/` directory is now ready to be deployed alongside the app.

---

## 4. Run the App Locally

```bash
make run
# or
streamlit run app.py
# Opens at http://localhost:8501
```

**What happens on startup:**
`app.py` calls `load_vector_store()` which initialises the ChromaDB `PersistentClient` and loads the existing `portfolio` collection. The Gemini client is also initialised at this point using the `GEMINI_API_KEY` from `.env`. Startup is fast because the index is already built — no embedding model download happens at runtime.

---

## 5. Query Flow (per question)

When a question is submitted in the chat UI, `answer()` in `rag.py` runs the following sequence:

**Step 1 — Guardrail check**
`is_off_limits()` in `guardrails.py` scans the question for blocked keywords. If matched, a fixed deflection response is returned immediately — the LLM is never called.

**Step 2 — Retrieve chunks**
The question is embedded using the same `all-MiniLM-L6-v2` model and a cosine similarity search is run against the 1249 stored vectors. The top 6 most relevant chunks are returned with their similarity scores (`1 - cosine_distance`).

**Step 3 — Confidence check**
`is_low_confidence()` checks whether all 6 chunk scores fall below the 0.20 threshold. If so, the app returns an honest "I don't have that information" rather than passing low-quality context to the LLM.

**Step 4 — Prompt construction**
`build_prompt()` assembles a structured prompt containing:
- The system prompt (role definition + rules)
- The last 5 conversation turns for context continuity
- The 6 retrieved chunks with source and section metadata
- The question

**Step 5 — Generate answer**
The prompt is sent to `gemini-2.5-flash` via the `google-genai` client. The model generates an answer grounded strictly in the retrieved context. Any error (rate limit, network) returns a friendly fallback message.

---

## 6. Run Tests

```bash
pytest tests/ -v
```

| Test file | Coverage |
|---|---|
| `test_guardrails.py` | 11 tests — off-limits keyword detection, confidence threshold logic |
| `test_rag.py` | 6 tests — prompt construction, history capping, chunk retrieval |

---

## 7. Rebuild the Index

To rebuild after updating any docs:

```bash
make clean    # removes chroma_db/
make index    # rebuilds from scratch
```

---

## 8. Deployment (Hugging Face Spaces)

The `chroma_db/` directory is committed to the repo and loaded at startup on HF Spaces — no re-indexing happens at runtime.

```bash
# Push everything (including rebuilt chroma_db/) to HF Spaces
hf upload bpraveen92/portfolio-assistant . . --repo-type=space
```

The `GEMINI_API_KEY` is set as a repository secret in HF Spaces settings (Settings → Repository secrets) and never stored in code or git history.
