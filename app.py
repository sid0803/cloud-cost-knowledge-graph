# app.py — Premium Streamlit UI for Cloud Cost Knowledge Graph

import streamlit as st
import json
import os
import re
from datetime import datetime

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Cloud Cost Knowledge Graph",
    page_icon="☁️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .main { background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%); }

    .hero-header {
        background: linear-gradient(90deg, #6a11cb 0%, #2575fc 100%);
        border-radius: 16px;
        padding: 28px 36px;
        margin-bottom: 24px;
        box-shadow: 0 8px 32px rgba(106,17,203,0.4);
    }
    .hero-header h1 {
        color: white; font-size: 2.2rem; font-weight: 700; margin: 0;
        text-shadow: 0 2px 8px rgba(0,0,0,0.3);
    }
    .hero-header p {
        color: rgba(255,255,255,0.85); font-size: 1rem; margin: 6px 0 0 0;
    }

    .metric-card {
        background: linear-gradient(135deg, #1e2140 0%, #252a4a 100%);
        border: 1px solid rgba(106,17,203,0.3);
        border-radius: 12px;
        padding: 16px 20px;
        text-align: center;
        box-shadow: 0 4px 16px rgba(0,0,0,0.3);
    }
    .metric-card .metric-value {
        font-size: 2rem; font-weight: 700;
        background: linear-gradient(90deg, #6a11cb, #2575fc);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .metric-card .metric-label {
        color: rgba(255,255,255,0.6); font-size: 0.8rem; margin-top: 4px;
    }

    .answer-box {
        background: linear-gradient(135deg, #1a2035 0%, #1e2a4a 100%);
        border: 1px solid rgba(37,117,252,0.4);
        border-left: 4px solid #2575fc;
        border-radius: 12px;
        padding: 20px 24px;
        margin: 12px 0;
        box-shadow: 0 4px 20px rgba(37,117,252,0.15);
    }

    .prov-path {
        background: rgba(106,17,203,0.15);
        border: 1px solid rgba(106,17,203,0.3);
        border-radius: 8px;
        padding: 8px 14px;
        margin: 4px 0;
        font-family: 'Courier New', monospace;
        font-size: 0.85rem;
        color: #a78bfa;
    }

    .badge {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 6px;
    }
    .badge-graph  { background: rgba(16,185,129,0.2); color: #10b981; border: 1px solid #10b981; }
    .badge-hybrid { background: rgba(245,158,11,0.2); color: #f59e0b; border: 1px solid #f59e0b; }
    .badge-vector { background: rgba(99,102,241,0.2); color: #6366f1; border: 1px solid #6366f1; }

    .query-btn {
        background: linear-gradient(90deg, rgba(106,17,203,0.2), rgba(37,117,252,0.2));
        border: 1px solid rgba(106,17,203,0.4);
        border-radius: 8px;
        padding: 8px 12px;
        margin: 3px 0;
        width: 100%;
        text-align: left;
        color: #c4b5fd;
        cursor: pointer;
        font-size: 0.8rem;
    }

    .stButton > button {
        background: linear-gradient(90deg, #6a11cb, #2575fc) !important;
        color: white !important;
        border: none !important;
        border-radius: 10px !important;
        font-weight: 600 !important;
        padding: 10px 28px !important;
        box-shadow: 0 4px 15px rgba(106,17,203,0.4) !important;
        transition: all 0.3s ease !important;
    }
    .stButton > button:hover {
        box-shadow: 0 6px 25px rgba(106,17,203,0.6) !important;
        transform: translateY(-1px) !important;
    }

    .stTextInput > div > div > input {
        background: rgba(30,33,64,0.9) !important;
        border: 1px solid rgba(106,17,203,0.5) !important;
        border-radius: 10px !important;
        color: white !important;
        font-size: 1rem !important;
        padding: 12px 16px !important;
    }

    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #6a11cb, #2575fc) !important;
    }

    div[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0d0d1a 0%, #1a1a2e 100%) !important;
        border-right: 1px solid rgba(106,17,203,0.3) !important;
    }

    .sidebar-section {
        background: rgba(30,33,64,0.6);
        border: 1px solid rgba(106,17,203,0.2);
        border-radius: 10px;
        padding: 12px 14px;
        margin: 8px 0;
    }
</style>
""", unsafe_allow_html=True)

# ── Imports (after page config) ──────────────────────────────────────────────
try:
    from rag.llm_pipeline import generate_answer
    from graph.neo4j_connection import driver
    BACKEND_OK = True
except Exception as e:
    BACKEND_OK = False
    BACKEND_ERROR = str(e)

# ── Session state ─────────────────────────────────────────────────────────────
if "result"       not in st.session_state: st.session_state.result       = None
if "query_input"  not in st.session_state: st.session_state.query_input  = ""
if "query_field"  not in st.session_state: st.session_state.query_field  = st.session_state.query_input
if "history"      not in st.session_state: st.session_state.history      = []


def _parse_provider_summary(answer_text: str):
    pattern = re.compile(
        r"\*\*(?P<provider>[A-Za-z0-9_ -]+)\s+Storage:\*\*\s*"
        r"-\s*Total Cost:\s*\$(?P<total>[0-9,]+(?:\.[0-9]+)?)\s*"
        r"-\s*Records:\s*(?P<records>[0-9,]+)\s*"
        r"-\s*Services:\s*(?P<services>[^\n]+)",
        re.IGNORECASE,
    )
    summaries = []
    for m in pattern.finditer(answer_text):
        summaries.append({
            "provider": m.group("provider").strip(),
            "total": m.group("total").strip(),
            "records": m.group("records").strip(),
            "services": m.group("services").strip(),
        })
    return summaries


def _parse_service_breakdown_rows(answer_text: str):
    if "Service Breakdown" not in answer_text:
        return []

    part = answer_text.split("Service Breakdown", 1)[1]
    rows = []
    for line in part.splitlines():
        line = line.strip()
        if not (line.startswith("|") and line.endswith("|")):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) < 5:
            continue
        rows.append({
            "Provider": cells[0],
            "Service": cells[1],
            "Records": cells[2],
            "Total Cost": cells[3],
            "Avg Cost": cells[4],
        })
    return rows

# ── Hero Header ───────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero-header">
    <h1>☁️ Cloud Cost Knowledge Graph</h1>
    <p>FOCUS 1.0 · Neo4j Graph Database · Hybrid RAG (Vector + Graph) · Gemini LLM · Sentence Transformers</p>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🧠 Knowledge Graph")

    # Graph stats
    if BACKEND_OK:
        try:
            with driver.session() as session:
                node_count = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
                rel_count  = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
                svc_count  = session.run("MATCH (s:Service) RETURN count(s) AS c").single()["c"]
                focus_count= session.run("MATCH (f:FOCUSColumn) RETURN count(f) AS c").single()["c"]

            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"""<div class="metric-card"><div class="metric-value">{node_count:,}</div><div class="metric-label">Total Nodes</div></div>""", unsafe_allow_html=True)
            with col2:
                st.markdown(f"""<div class="metric-card"><div class="metric-value">{rel_count:,}</div><div class="metric-label">Relationships</div></div>""", unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)
            col3, col4 = st.columns(2)
            with col3:
                st.markdown(f"""<div class="metric-card"><div class="metric-value">{svc_count}</div><div class="metric-label">Services</div></div>""", unsafe_allow_html=True)
            with col4:
                st.markdown(f"""<div class="metric-card"><div class="metric-value">{focus_count}</div><div class="metric-label">FOCUS Columns</div></div>""", unsafe_allow_html=True)
        except Exception:
            st.warning("⚠️ Neo4j not connected")

    st.divider()

    # Preset test queries
    st.markdown("### 🎯 Assignment Test Queries")
    st.markdown("*Click to load a query:*")

    PRESET_QUERIES = [
        ("1️⃣", "Which are the core FOCUS columns and how do they differ from vendor specific columns?"),
        ("2️⃣", "Find all AWS compute services"),
        ("3️⃣", "What is the Azure equivalent of AWS S3?"),
        ("4️⃣", "Compare storage costs between AWS and Azure"),
        ("5️⃣", "Find the top 5 most expensive resources tagged as Production in Azure"),
        ("6️⃣", "When calculating commitment utilization, which charge categories must be excluded to avoid double counting?"),
        ("7️⃣", "Why does my total increase when I include commitment purchases and usage together?"),
        ("8️⃣", "Which cost type should be used to analyze cloud spend?"),
        ("9️⃣", "Can ContractedCost differ from ContractedUnitPrice × PricingQuantity for a normal Usage charge?"),
        ("🔟", "What is EffectiveCost and how is it derived?"),
        ("🔟", "Show total AWS cost vs Azure cost breakdown by service category"),
    ]

    for emoji, q in PRESET_QUERIES:
        if st.button(f"{emoji} {q[:55]}...", key=f"preset_{q[:20]}", use_container_width=True):
            st.session_state.query_field = q
            st.session_state.query_input = q
            st.rerun()

    st.divider()
    st.markdown("### ⚙️ System Info")
    st.markdown("""
    <div class="sidebar-section" style="font-size:0.8rem; color: rgba(255,255,255,0.7);">
    <b>Stack:</b><br>
    • 🗄 Neo4j Knowledge Graph<br>
    • 🔍 all-MiniLM-L6-v2 Embeddings<br>
    • 🤖 Gemini 2.x / OpenAI / Ollama<br>
    • 📊 FOCUS 1.0 Ontology<br>
    • ⚡ Hybrid RAG Pipeline
    </div>
    """, unsafe_allow_html=True)

    if st.button("🗑️ Clear History", use_container_width=True):
        st.session_state.history = []
        st.session_state.result  = None


# ─────────────────────────────────────────────────────────────────────────────
# MAIN CONTENT
# ─────────────────────────────────────────────────────────────────────────────
# ── Backend status banner (soft warning, no hard stop) ───────────────────────
if not BACKEND_OK:
    st.warning(
        f"⚠️ **Backend Warning:** {BACKEND_ERROR}\n\n"
        "Graph-based queries may not work. Ensure Neo4j is running at `bolt://127.0.0.1:7687` "
        "and `NEO4J_PASSWORD` is set in your `.env` file. LLM-only queries will still work."
    )

# ── Query Input ───────────────────────────────────────────────────────────────
col_input, col_btn = st.columns([5, 1])
with col_input:
    query = st.text_input(
        "🔎 Ask a cloud cost question",
        placeholder="e.g. Which FOCUS columns differ from vendor columns? | Compare AWS vs Azure storage costs",
        key="query_field",
        label_visibility="collapsed",
    )

with col_btn:
    analyze_clicked = st.button("⚡ Analyze", use_container_width=True)

if analyze_clicked:
    if not query.strip():
        st.warning("Please enter a question.")
    else:
        with st.spinner("🔄 Running Hybrid Graph + Vector + LLM pipeline..."):
            result = generate_answer(query)
            st.session_state.result = result
            st.session_state.history.append({
                "query": query,
                "result": result,
                "time": datetime.now().strftime("%H:%M:%S"),
            })
            st.session_state.query_input = ""

# ── Results ───────────────────────────────────────────────────────────────────
if st.session_state.result:
    result = st.session_state.result

    # ── Metadata row ─────────────────────────────────────────────────────────
    method     = result.get("retrieval_method", "hybrid")
    confidence = result.get("confidence", 0.0)
    badge_cls  = f"badge-{method}" if method in ("graph", "hybrid", "vector") else "badge-hybrid"

    col_m1, col_m2, col_m3 = st.columns([2, 2, 6])
    with col_m1:
        st.markdown(f'<span class="badge {badge_cls}">🔍 {method.upper()}</span>', unsafe_allow_html=True)
    with col_m2:
        conf_pct = int(confidence * 100)
        color = "#10b981" if conf_pct >= 70 else "#f59e0b" if conf_pct >= 50 else "#ef4444"
        st.markdown(f'<span style="color:{color}; font-weight:600;">📈 Confidence: {conf_pct}%</span>', unsafe_allow_html=True)
    with col_m3:
        st.progress(confidence)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Tabs ─────────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs(["🧠 Answer", "🔎 Provenance", "📊 Graph Viz", "📋 Query History"])

    # TAB 1 — ANSWER
    with tab1:
        answer = result.get("answer", "No answer generated.")
        summary_rows = _parse_provider_summary(answer)
        breakdown_rows = _parse_service_breakdown_rows(answer)

        if breakdown_rows:
            intro = answer.split("### Service Breakdown", 1)[0].strip()
            if intro:
                st.markdown(f'<div class="answer-box">{intro}</div>', unsafe_allow_html=True)

            if summary_rows:
                st.markdown("#### Provider Summary")
                cols = st.columns(len(summary_rows))
                for col, row in zip(cols, summary_rows):
                    with col:
                        st.markdown(
                            f"""
                            <div class="metric-card" style="text-align:left; min-height:130px;">
                                <div style="color:white; font-weight:700; font-size:1rem;">{row['provider']}</div>
                                <div style="color:rgba(255,255,255,0.85); margin-top:8px;">
                                    <b>Total:</b> ${row['total']}<br>
                                    <b>Records:</b> {row['records']}
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

            st.markdown("#### Service Breakdown")
            st.dataframe(breakdown_rows, use_container_width=True, hide_index=True)
        else:
            st.markdown(f'<div class="answer-box">{answer}</div>', unsafe_allow_html=True)

        alloc = result.get("allocation_explanation")
        if alloc:
            st.divider()
            st.markdown("### 🧾 Allocation Explanation")
            st.info(alloc)

    # TAB 2 — PROVENANCE
    with tab2:
        provenance = result.get("provenance", [])
        if provenance:
            st.markdown("#### Graph Traversal Paths")
            for i, path in enumerate(provenance, 1):
                if isinstance(path, dict):
                    frm   = path.get("from", "")
                    rel   = path.get("relationship", "→")
                    to    = path.get("to", "")
                    st.markdown(
                        f'<div class="prov-path">#{i} &nbsp; {frm} &nbsp;─[{rel}]→&nbsp; {to}</div>',
                        unsafe_allow_html=True
                    )
                else:
                    st.markdown(f'<div class="prov-path">#{i} &nbsp; {path}</div>', unsafe_allow_html=True)
        else:
            st.info("No provenance paths. This answer was generated from graph-level metadata.")

    # TAB 3 — GRAPH VISUALIZATION
    with tab3:
        st.markdown("#### Live Graph Sample (Cost Records → Services → Locations)")
        try:
            from pyvis.network import Network
            import tempfile

            net = Network(height="480px", bgcolor="#0f0f1a", font_color="white")
            net.set_options("""
            {
              "nodes": {"shape": "dot","size": 18,"font": {"size": 13, "color": "white"}},
              "edges": {"arrows": {"to": {"enabled": true, "scaleFactor": 0.7}},
                        "color": {"color": "#6a11cb"},
                        "font": {"size": 10, "color": "#a78bfa"}},
              "physics": {"stabilization": {"iterations": 150}},
              "interaction": {"hover": true}
            }
            """)

            COLORS = {
                "CostRecord": "#6a11cb",
                "Service": "#2575fc",
                "Resource": "#10b981",
                "Account": "#f59e0b",
                "Location": "#ef4444",
                "Charge": "#8b5cf6",
                "Tag": "#06b6d4",
            }

            with driver.session() as session:
                records = session.run("""
                    MATCH (c:CostRecord)-[:USES_SERVICE]->(s:Service)
                    OPTIONAL MATCH (c)-[:INCURRED_BY]->(r:Resource)
                    OPTIONAL MATCH (r)-[:DEPLOYED_IN]->(l:Location)
                    OPTIONAL MATCH (c)-[:HAS_CHARGE]->(ch:Charge)
                    RETURN c.id AS cid, c.cloudProvider AS provider,
                           c.effectiveCost AS cost,
                           s.name AS service, s.serviceCategory AS cat,
                           r.id AS resource,
                           l.regionName AS region,
                           ch.category AS charge
                    LIMIT 30
                """).data()

            added_nodes = set()

            for row in records:
                cid     = (row.get("cid") or "")[:12]
                service = row.get("service")
                resource= row.get("resource")
                region  = row.get("region")
                charge  = row.get("charge")
                cost    = row.get("cost") or 0
                provider= row.get("provider") or "?"
                cat     = row.get("cat") or "Other"

                if cid not in added_nodes:
                    net.add_node(cid, label=f"💰 {cid}\n${cost:.1f}",
                                 color=COLORS["CostRecord"], title=f"CostRecord | {provider} | ${cost:.2f}")
                    added_nodes.add(cid)

                if service and service not in added_nodes:
                    net.add_node(service, label=f"⬡ {service[:20]}",
                                 color=COLORS["Service"], title=f"Service: {service}\nCategory: {cat}")
                    added_nodes.add(service)

                if service:
                    net.add_edge(cid, service, label="USES_SERVICE")

                if resource:
                    rid = str(resource)[:12]
                    if rid not in added_nodes:
                        net.add_node(rid, label=f"📦 {rid}",
                                     color=COLORS["Resource"], title=f"Resource: {resource}")
                        added_nodes.add(rid)
                    net.add_edge(cid, rid, label="INCURRED_BY")

                if region and region not in added_nodes:
                    net.add_node(region, label=f"📍 {region}",
                                 color=COLORS["Location"], title=f"Region: {region}")
                    added_nodes.add(region)

                if resource and region:
                    rid = str(resource)[:12]
                    net.add_edge(rid, region, label="DEPLOYED_IN")

                if charge and charge not in added_nodes:
                    net.add_node(charge, label=f"⚡ {charge}",
                                 color=COLORS["Charge"], title=f"ChargeCategory: {charge}")
                    added_nodes.add(charge)

                if charge:
                    net.add_edge(cid, charge, label="HAS_CHARGE")

            with tempfile.NamedTemporaryFile(delete=False, suffix=".html", mode="w") as f:
                net.write_html(f.name)
                html_path = f.name

            with open(html_path, "r", encoding="utf-8") as f:
                html_content = f.read()

            st.components.v1.html(html_content, height=500, scrolling=False)

            # Legend
            st.markdown("**Legend:**")
            leg_cols = st.columns(7)
            icons = ["💰 CostRecord", "⬡ Service", "📦 Resource",
                     "🏦 Account", "📍 Location", "⚡ Charge", "🏷️ Tag"]
            cols_list = ["#6a11cb", "#2575fc", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4"]
            for col, icon, color in zip(leg_cols, icons, cols_list):
                with col:
                    st.markdown(f'<span style="color:{color}; font-size:0.75rem;">● {icon}</span>', unsafe_allow_html=True)

        except ImportError:
            st.info("📦 Install `pyvis` to enable graph visualization: `pip install pyvis`")
        except Exception as e:
            st.warning(f"Graph visualization unavailable: {e}")

    # TAB 4 — HISTORY
    with tab4:
        history = st.session_state.history
        if not history:
            st.info("No queries yet. Run a few queries to see the history here.")
        else:
            st.markdown(f"**{len(history)} queries in this session:**")
            for i, h in enumerate(reversed(history)):
                with st.expander(f"[{h['time']}] {h['query'][:70]}...", expanded=(i == 0)):
                    hr = h["result"]
                    method_h = hr.get("retrieval_method", "hybrid")
                    conf_h   = int(hr.get("confidence", 0) * 100)
                    st.markdown(f"**Method:** `{method_h}` | **Confidence:** `{conf_h}%`")
                    st.markdown(hr.get("answer", ""))

            # Load evaluation log
            if os.path.exists("evaluation_log.json"):
                st.divider()
                st.markdown("#### 📋 Evaluation Log (from disk)")
                try:
                    with open("evaluation_log.json") as f:
                        lines = f.readlines()
                    log_data = [json.loads(l) for l in lines[-10:] if l.strip()]
                    st.dataframe(log_data, use_container_width=True)
                except Exception:
                    pass

# ── Empty state ───────────────────────────────────────────────────────────────
if not st.session_state.result:
    st.markdown("""
    <div style="text-align:center; padding: 60px 20px; color: rgba(255,255,255,0.4);">
        <div style="font-size: 4rem; margin-bottom: 16px;">🔭</div>
        <div style="font-size: 1.2rem; font-weight: 600; color: rgba(255,255,255,0.6);">
            Ask a question to explore the knowledge graph
        </div>
        <div style="font-size: 0.9rem; margin-top: 8px;">
            Try: <i>Compare storage costs between AWS and Azure</i>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Show feature cards
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("""
        <div class="metric-card" style="text-align:left; padding:20px;">
            <div style="font-size:1.8rem;">🗺️</div>
            <div style="color:white; font-weight:600; margin:8px 0 4px;">FOCUS 1.0 Ontology</div>
            <div style="color:rgba(255,255,255,0.6);font-size:0.82rem;">
            Models 31 FOCUS columns and ontology classes as first-class graph entities, including derivation rules, validation constraints, and AWS/Azure normalization mappings.
            </div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown("""
        <div class="metric-card" style="text-align:left; padding:20px;">
            <div style="font-size:1.8rem;">⚡</div>
            <div style="color:white; font-weight:600; margin:8px 0 4px;">Hybrid Retrieval</div>
            <div style="color:rgba(255,255,255,0.6);font-size:0.82rem;">
            Combines semantic vector search with multi-hop Cypher traversal to return grounded answers with explicit provenance paths from source graph nodes.
            </div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        st.markdown("""
        <div class="metric-card" style="text-align:left; padding:20px;">
            <div style="font-size:1.8rem;">🤖</div>
            <div style="color:white; font-weight:600; margin:8px 0 4px;">LLM Chain</div>
            <div style="color:rgba(255,255,255,0.6);font-size:0.82rem;">
            Multi-model fallback chain for robust natural-language generation, while all financial calculations remain deterministic and executed directly in Neo4j.
            </div>
        </div>
        """, unsafe_allow_html=True)
