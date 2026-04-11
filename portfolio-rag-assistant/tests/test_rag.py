import pytest
from unittest.mock import MagicMock, patch
from rag import build_prompt, load_vector_store, retrieve_chunks


def test_build_prompt_contains_question():
    chunks = [
        {"text": "Delta MERGE handles upserts by comparing keys.", "source": "f1", "section": "8. Delta MERGE"}
    ]
    history = []
    prompt = build_prompt("How do upserts work?", chunks, history)
    assert "How do upserts work?" in prompt


def test_build_prompt_contains_chunk_text():
    chunks = [
        {"text": "Delta MERGE handles upserts by comparing keys.", "source": "f1", "section": "8. Delta MERGE"}
    ]
    history = []
    prompt = build_prompt("How do upserts work?", chunks, history)
    assert "Delta MERGE handles upserts" in prompt


def test_build_prompt_contains_source_attribution():
    chunks = [
        {"text": "Some relevant content.", "source": "databricks-f1-intelligence", "section": "8. Delta MERGE"}
    ]
    history = []
    prompt = build_prompt("Tell me about Delta MERGE", chunks, history)
    assert "databricks-f1-intelligence" in prompt


def test_build_prompt_includes_conversation_history():
    chunks = [{"text": "Some content.", "source": "f1", "section": "intro"}]
    history = [
        {"role": "user", "content": "Tell me about the F1 project"},
        {"role": "assistant", "content": "The F1 project uses Delta Lake."},
    ]
    prompt = build_prompt("What about the streaming project?", chunks, history)
    assert "Tell me about the F1 project" in prompt


def test_build_prompt_caps_history_at_five_exchanges():
    chunks = [{"text": "content", "source": "src", "section": "sec"}]
    history = [
        {"role": "user", "content": f"question {i}"}
        for i in range(10)
    ]
    prompt = build_prompt("new question", chunks, history)
    # only last 5 user messages should appear (10 messages = 5 exchanges)
    assert "question 9" in prompt
    assert "question 0" not in prompt


def test_retrieve_chunks_returns_list():
    with patch("rag.COLLECTION") as mock_collection:
        mock_collection.query.return_value = {
            "documents": [["chunk text"]],
            "metadatas": [[{"source": "f1", "section": "intro"}]],
            "distances": [[0.1]],
        }
        results = retrieve_chunks("test question", n=4)
        assert isinstance(results, list)
        assert len(results) == 1
        assert results[0]["text"] == "chunk text"
        assert results[0]["score"] == pytest.approx(0.9, 0.01)
