# app.py

import streamlit as st
from rag.llm_pipeline import generate_answer
from graph.neo4j_connection import driver


# =================================================
# Page Config
# =================================================
st.set_page_config(
    page_title="Cloud Cost Knowledge Graph",
    layout="wide"
)

st.title("☁️ Cloud Cost Knowledge Graph + RAG Engine")
st.markdown("Graph-Augmented Retrieval + Vector Search + LLM Explanation")

st.divider()

# =================================================
# Session State
# =================================================
if "result" not in st.session_state:
    st.session_state.result = None

# =================================================
# User Input
# =================================================
query = st.text_input(
    "🔎 Ask a cloud cost question (e.g., compute costs, FOCUS columns, commitment usage)"
)

if st.button("Analyze"):

    if not query:
        st.warning("Please enter a query.")
    else:
        with st.spinner("Running Graph + Vector + LLM pipeline..."):
            st.session_state.result = generate_answer(query)

# =================================================
# Display Result Section
# =================================================
if st.session_state.result:

    result = st.session_state.result

    # ----------------------------
    # AI Answer
    # ----------------------------
    st.subheader("🧠 AI Answer")
    st.write(result.get("answer", "No answer generated."))

    st.divider()

    # ----------------------------
    # Provenance
    # ----------------------------
    st.subheader("🔎 Provenance Paths")

    provenance = result.get("provenance", [])

    if provenance:
        for path in provenance:
            if isinstance(path, dict):
                st.write(
                    f"• {path.get('from')} "
                    f"→ {path.get('relationship')} "
                    f"→ {path.get('to')}"
                )
            else:
                st.write("•", path)
    else:
        st.info("No provenance paths available.")

    st.divider()

    # ----------------------------
    # Retrieval Method
    # ----------------------------
    st.subheader("🧩 Retrieval Method")
    st.write(result.get("retrieval_method", "Unknown"))

    # ----------------------------
    # Confidence Score
    # ----------------------------
    st.subheader("📈 Confidence Score")
    confidence = result.get("confidence", 0)
    st.progress(confidence)
    st.write(f"{int(confidence * 100)}%")

    # ----------------------------
    # Allocation Explanation
    # ----------------------------
    allocation_explanation = result.get("allocation_explanation")

    if allocation_explanation:
        st.divider()
        st.subheader("🧾 Allocation Explanation")
        st.write(allocation_explanation)

# =================================================
# Graph Statistics (Always Visible)
# =================================================
st.divider()
st.subheader("📊 Graph Statistics")

with driver.session() as session:
    node_count = session.run(
        "MATCH (n) RETURN count(n) AS count"
    ).single()["count"]

    rel_count = session.run(
        "MATCH ()-[r]->() RETURN count(r) AS count"
    ).single()["count"]

st.write(f"Total Nodes: {node_count}")
st.write(f"Total Relationships: {rel_count}")