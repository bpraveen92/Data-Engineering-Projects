import streamlit as st
from rag import answer, load_vector_store

st.set_page_config(
    page_title="Praveen's Portfolio Assistant",
    page_icon="🤖",
    layout="centered",
)

load_vector_store()

st.title("🤖 Praveen's Portfolio Assistant")
st.caption(
    "Ask me anything about Praveen's data engineering projects, "
    "technical skills, and professional background."
)
st.divider()

if "history" not in st.session_state:
    st.session_state.history = []

if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": (
                "Hi! I'm Praveen's portfolio assistant. I can answer questions about his "
                "6 data engineering projects, technical skills, and professional background. "
                "What would you like to know?"
            ),
        }
    ]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if user_input := st.chat_input("Ask a question..."):
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            response_text, sources = answer(user_input, st.session_state.history)
        st.markdown(response_text)

    st.session_state.history.append({"role": "user", "content": user_input})
    st.session_state.history.append({"role": "assistant", "content": response_text})
    st.session_state.messages.append(
        {"role": "assistant", "content": response_text, "sources": sources}
    )
