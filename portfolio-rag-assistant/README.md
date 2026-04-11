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

A RAG-based chatbot that lets you have a natural conversation about Praveen
Balasubramanian's data engineering projects, technical background, and career
experience — without reading thousands of lines of documentation.

**Live demo:** https://huggingface.co/spaces/bpraveen92/portfolio-assistant
**Portfolio:** https://bpraveen92.github.io

## How It Works

```
User question
    │
    ▼
Guardrail check (off-limits keyword detection)
    │
    ▼
ChromaDB similarity search → top 4 chunks retrieved
    │
    ▼
Confidence check (low score → honest "I don't know")
    │
    ▼
Gemini 1.5 Flash generates a grounded answer
    │
    ▼
Answer displayed with source attribution
```

## Tech Stack

| Component | Tool |
|---|---|
| Chat UI | Streamlit |
| Hosting | Hugging Face Spaces (Docker) |
| LLM | Gemini 1.5 Flash (free tier) |
| Embeddings | all-MiniLM-L6-v2 via ONNX (ChromaDB built-in) |
| Vector store | ChromaDB (file-based, ~9MB) |
