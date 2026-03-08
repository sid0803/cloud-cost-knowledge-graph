# api.py  — FastAPI REST API (Part F Bonus)

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any
import time

from rag.llm_pipeline import generate_answer
from graph.neo4j_connection import driver

app = FastAPI(
    title="Cloud Cost Knowledge Graph API",
    description="Hybrid RAG + Graph-Augmented retrieval for FOCUS 1.0 cloud cost intelligence",
    version="1.0.0",
)


# ─────────────────────────────────────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────────────────────────────────────
class QueryRequest(BaseModel):
    question: str


class QueryResponse(BaseModel):
    answer: str
    concepts: list[str]
    paths: list[Any]
    confidence: float
    retrieval_method: str


# ─────────────────────────────────────────────────────────────────────────────
# GET /health
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "cloud-cost-knowledge-graph"}


# ─────────────────────────────────────────────────────────────────────────────
# POST /query
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    result = generate_answer(req.question)

    # Extract concept names from provenance paths
    concepts = []
    paths = result.get("provenance", [])
    for p in paths:
        if isinstance(p, dict):
            for v in p.values():
                if v and isinstance(v, str) and len(v) < 60:
                    concepts.append(v)
        elif isinstance(p, str):
            concepts.append(p)

    return QueryResponse(
        answer=result.get("answer", ""),
        concepts=list(dict.fromkeys(concepts))[:10],
        paths=paths,
        confidence=result.get("confidence", 0.0),
        retrieval_method=result.get("retrieval_method", "hybrid"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# GET /concept/{name}
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/concept/{name}")
def get_concept(name: str):
    with driver.session() as session:

        # Try FOCUSColumn first
        fc = session.run("""
            MATCH (f:FOCUSColumn {name: $name})
            OPTIONAL MATCH (f)-[:DERIVED_BY]->(dr:DerivationRule)
            OPTIONAL MATCH (f)-[:BELONGS_TO_CLASS]->(cls:OntologyClass)
            OPTIONAL MATCH (aws:AWSColumn)-[:MAPS_TO]->(f)
            OPTIONAL MATCH (az:AzureColumn)-[:MAPS_TO]->(f)
            RETURN
                f.name AS name, f.description AS description,
                f.dataType AS dataType, f.nullable AS nullable,
                f.validationRule AS validationRule, f.category AS category,
                f.standard AS standard,
                dr.formula AS formula,
                cls.name AS ontologyClass,
                collect(DISTINCT aws.name) AS awsMappings,
                collect(DISTINCT az.name) AS azureMappings
        """, name=name).single()

        if fc:
            return {
                "type": "FOCUSColumn",
                "name": fc["name"],
                "description": fc["description"],
                "dataType": fc["dataType"],
                "nullable": fc["nullable"],
                "validationRule": fc["validationRule"],
                "category": fc["category"],
                "standard": fc["standard"],
                "derivationFormula": fc["formula"],
                "ontologyClass": fc["ontologyClass"],
                "awsMappings": fc["awsMappings"],
                "azureMappings": fc["azureMappings"],
            }

        # Try Service node
        svc = session.run("""
            MATCH (s:Service {name: $name})
            OPTIONAL MATCH (s)<-[:USES_SERVICE]-(c:CostRecord)
            OPTIONAL MATCH (s)-[:EQUIVALENT_TO]-(eq:Service)
            RETURN
                s.name AS name, s.cloudProvider AS provider,
                s.serviceCategory AS category,
                count(DISTINCT c) AS records,
                sum(c.effectiveCost) AS totalCost,
                collect(DISTINCT eq.name) AS equivalents
            ORDER BY records DESC
            LIMIT 1
        """, name=name).single()

        if svc:
            return {
                "type": "Service",
                "name": svc["name"],
                "cloudProvider": svc["provider"],
                "serviceCategory": svc["category"],
                "totalRecords": svc["records"],
                "totalEffectiveCost": round(svc["totalCost"] or 0, 2),
                "equivalentServices": svc["equivalents"],
            }

        # Try OntologyClass
        oc = session.run("""
            MATCH (c:OntologyClass {name: $name})
            OPTIONAL MATCH (c)<-[:SUBCLASS_OF]-(child:OntologyClass)
            OPTIONAL MATCH (c)-[:SUBCLASS_OF]->(parent:OntologyClass)
            RETURN
                c.name AS name, c.description AS description,
                collect(DISTINCT child.name) AS subclasses,
                parent.name AS parent
        """, name=name).single()

        if oc:
            return {
                "type": "OntologyClass",
                "name": oc["name"],
                "description": oc["description"],
                "parent": oc["parent"],
                "subclasses": oc["subclasses"],
            }

    raise HTTPException(status_code=404, detail=f"Concept '{name}' not found in knowledge graph")


# ─────────────────────────────────────────────────────────────────────────────
# GET /stats
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/stats")
def stats():
    with driver.session() as session:
        node_count = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        rel_count  = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]

        label_counts = session.run("""
            CALL apoc.meta.stats()
            YIELD labels
            RETURN labels
        """)
        label_data = {}
        try:
            row = label_counts.single()
            label_data = dict(row["labels"]) if row else {}
        except Exception:
            # APOC not available — do manual count
            labels_result = session.run("""
                MATCH (n)
                UNWIND labels(n) AS lbl
                RETURN lbl, count(*) AS cnt
                ORDER BY cnt DESC
            """).data()
            label_data = {r["lbl"]: r["cnt"] for r in labels_result}

        indexes = session.run("SHOW INDEXES").data()
        index_summary = [
            {"name": idx.get("name"), "type": idx.get("type"), "state": idx.get("state")}
            for idx in indexes
        ]

    return {
        "total_nodes": node_count,
        "total_relationships": rel_count,
        "node_labels": label_data,
        "indexes": index_summary,
        "status": "healthy",
    }
