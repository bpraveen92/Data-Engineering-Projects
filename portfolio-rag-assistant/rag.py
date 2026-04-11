import os
from pathlib import Path

import chromadb
from groq import Groq, RateLimitError
from chromadb.utils import embedding_functions
from dotenv import load_dotenv

from guardrails import is_off_limits, is_low_confidence, OFF_LIMITS_RESPONSE

load_dotenv()

CHROMA_DIR = Path("chroma_db")
COLLECTION_NAME = "portfolio"
LOW_CONFIDENCE_RESPONSE = (
    "I don't have that information in my knowledge base. "
    "For anything outside Praveen's documented DE work, feel free to reach out at "
    "pravbala30@gmail.com or on LinkedIn."
)

SYSTEM_PROMPT = """You are a professional portfolio assistant for Praveen Balasubramanian, \
a Senior Data Engineer with 10+ years of experience.

Your job is to answer questions about Praveen's data engineering projects, technical skills, \
professional background, and career experience.

Rules:
- Answer only from the context provided below. Do not make up projects, experiences, or facts.
- If the answer is not in the context, say: "I don't have that information in my knowledge base."
- Never reveal salary expectations, job search status, which companies Praveen is interviewing \
with, or any personal details beyond what is publicly available.
- If asked about off-limits topics, politely decline and redirect to pravbala30@gmail.com or LinkedIn.
- Keep answers concise and professional. Adapt technical depth to the question asked.
"""

groq_client = None
embedding_fn = None
chroma_client = None
COLLECTION = None


def init_clients():
    global groq_client, embedding_fn, chroma_client, COLLECTION
    if COLLECTION is not None:
        return
    groq_client = Groq(api_key=os.environ["GROQ_API_KEY"])
    embedding_fn = embedding_functions.DefaultEmbeddingFunction()
    chroma_client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    COLLECTION = chroma_client.get_collection(name=COLLECTION_NAME, embedding_function=embedding_fn)


def load_vector_store():
    init_clients()
    return COLLECTION


def retrieve_chunks(question, n=4):
    init_clients()
    results = COLLECTION.query(query_texts=[question], n_results=n)
    chunks = []
    for text, metadata, distance in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0],
    ):
        chunks.append({
            "text": text,
            "source": metadata.get("source", "unknown"),
            "section": metadata.get("section", ""),
            "score": 1 - distance,  # cosine distance → similarity score
        })
    return chunks


def build_prompt(question, chunks, history):
    recent_history = history[-5:]  # last 5 messages

    history_text = ""
    for msg in recent_history:
        role = "User" if msg["role"] == "user" else "Assistant"
        history_text += f"{role}: {msg['content']}\n"

    context_text = ""
    for chunk in chunks:
        context_text += (
            f"[Source: {chunk['source']} | Section: {chunk['section']}]\n"
            f"{chunk['text']}\n\n"
        )

    prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        f"CONVERSATION HISTORY:\n{history_text}\n"
        f"RETRIEVED CONTEXT:\n{context_text}"
        f"QUESTION: {question}\n\n"
        f"Answer using only the context above."
    )
    return prompt


def ask_groq(prompt):
    init_clients()
    try:
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content
    except RateLimitError:
        return (
            "I've hit my request limit for the moment. Feel free to reach out directly "
            "at pravbala30@gmail.com or connect on LinkedIn."
        )
    except Exception as e:
        print(f"Groq error: {type(e).__name__}: {e}")
        return "I'm having trouble connecting right now. Please try again in a moment."


def answer(question, history):
    if is_off_limits(question):
        return OFF_LIMITS_RESPONSE, []

    chunks = retrieve_chunks(question, n=6)

    if is_low_confidence(chunks):
        return LOW_CONFIDENCE_RESPONSE, []

    prompt = build_prompt(question, chunks, history)
    response = ask_groq(prompt)
    return response, chunks
