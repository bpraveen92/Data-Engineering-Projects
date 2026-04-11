---
title: Praveen's Portfolio Assistant
emoji: 🤖
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
pinned: true
---

# Portfolio RAG Assistant

A RAG-based chatbot I built so recruiters and engineers can ask questions about my data engineering projects, technical background, and career experience — without reading thousands of lines of documentation.

I've been reading a lot about RAG, vector databases, and embeddings recently and wanted to use my own portfolio documentation as a hands-on way to see how it all fits together — chunking, embedding, retrieval, prompt construction, and generation as one end-to-end flow.

**Live demo:** https://huggingface.co/spaces/bpraveen92/portfolio-assistant
**Portfolio:** https://bpraveen92.github.io

---

## How It Works

```
User question
    │
    ▼
Guardrail check (off-limits keyword detection)
    │
    ▼
ChromaDB similarity search — top 6 chunks retrieved
    │
    ▼
Confidence check (low score → honest "I don't know")
    │
    ▼
Prompt construction (system prompt + history + chunks + question)
    │
    ▼
Gemini 2.5 Flash generates a grounded answer
    │
    ▼
Answer displayed in Streamlit chat UI
```

---

## Tech Stack

| Component | Tool | Cost |
|---|---|---|
| Chat UI | Streamlit | Free |
| Hosting | Hugging Face Spaces (Docker) | Free |
| LLM | Google Gemini 2.5 Flash | Free tier (AI Studio) |
| Embeddings | all-MiniLM-L6-v2 via ONNX (ChromaDB built-in) | Free |
| Vector store | ChromaDB (persistent, file-based) | Free |
| Knowledge base | Sanitised markdown docs | Already written |

---

## Project Structure

```
portfolio-rag-assistant/
├── app.py                  # Streamlit chat UI, session state, message rendering
├── rag.py                  # load vector store, retrieve chunks, build prompt, call Gemini
├── guardrails.py           # off-limits keyword detection + confidence threshold check
├── Dockerfile              # Python 3.11-slim, exposes port 7860 for HF Spaces
├── requirements.txt        # streamlit, chromadb, google-genai, python-dotenv
├── Makefile                # index, run, clean targets
├── pyproject.toml          # hatchling build backend, ruff line-length=100
├── .env.example            # GEMINI_API_KEY=your_key_here
├── chroma_db/              # compiled vector store (~9MB, deployed to HF Spaces)
├── scripts/
│   └── build_index.py      # one-time local script: read docs → chunk → embed → save
├── docs/                   # sanitised knowledge base (gitignored — never deployed)
│   ├── projects/           # 6 pipeline deep-dive docs
│   ├── profile/            # professional bio + career achievements
│   └── modelling/          # dimensional modelling expertise doc
└── tests/
    ├── test_guardrails.py  # 11 unit tests for off-limits and confidence checks
    └── test_rag.py         # 6 unit tests for prompt construction and retrieval
```

---

## Architecture

### Phase 1 — Indexing (runs once, locally)

The `scripts/build_index.py` script reads all sanitised markdown docs, splits them into 500-character chunks with 50-character overlap using `langchain-text-splitters`, and embeds each chunk using the `all-MiniLM-L6-v2` model (384 dimensions) via ChromaDB's built-in ONNX embedding function. The resulting vector store (~9MB) is committed to the repository and deployed to Hugging Face Spaces alongside the application code.

### Phase 2 — Query (runs on every user question)

On each question, the app runs a three-layer guardrail check before touching the LLM:

1. **Keyword check** — blocks off-limits topics (salary, interview status, notice period) before any retrieval
2. **Confidence check** — if all retrieved chunks score below the similarity threshold, returns an honest "I don't have that information" rather than hallucinating
3. **System prompt instruction** — Gemini is explicitly instructed never to reveal sensitive information regardless of how the question is framed

If the question passes all checks, the top 6 most relevant chunks are injected into a structured prompt along with the last 5 conversation turns, and Gemini generates a grounded answer.

---

## Knowledge Base

The knowledge base covers:

| Document | Content |
|---|---|
| 6 pipeline deep-dive docs | Full architecture, design decisions, tech stack, and implementation details for each portfolio project |
| Professional bio | Career narrative, employment history, education, certifications, technical skills |
| Career achievements | Impact metrics and career highlights from Amazon Prime Video, Social Tables, and Groupon |
| Dimensional modelling doc | Data modelling patterns, design decisions, and SQL examples from supply chain analytics work |

All documents are sanitised before indexing — internal system names, compensation figures, and private prep notes are removed. The `docs/` folder is gitignored and never deployed.

---

## Local Development

```bash
# 1. Clone and install
git clone https://github.com/bpraveen92/Data-Engineering-Projects
cd portfolio-rag-assistant
pip install -r requirements.txt

# 2. Add your Gemini API key (get one free at aistudio.google.com/app/apikey)
cp .env.example .env
# Edit .env and add your key

# 3. Place sanitised docs in docs/ (see docs/ folder structure above)

# 4. Build the vector store
make index

# 5. Run the app
make run
# Opens at http://localhost:8501
```

---

## Deployment

The app is deployed to Hugging Face Spaces using the Docker SDK. The `chroma_db/` vector store is committed to the repo and loaded at startup — no embedding model download required at runtime since ChromaDB's ONNX-based `DefaultEmbeddingFunction` handles query-time embedding inline.

The `GEMINI_API_KEY` is set as a repository secret in HF Spaces settings and never appears in code or git history.

