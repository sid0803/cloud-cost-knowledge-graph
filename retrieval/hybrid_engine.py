#retrieval/hybrid_engine.py

import os
import sqlite3
from graph.neo4j_connection import driver

# Project root = two levels up from this file (retrieval/ → root)
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_BILLING_DB = os.path.join(_ROOT, "billing.db")

_model = None

def get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model


# -------------------------------------------------
# Get Equivalent Services (WITH PROVIDER)
# -------------------------------------------------
def get_service_and_equivalents(service_name, provider=None):
    with driver.session() as session:
        result = session.run("""
            MATCH (s:Service)
            WHERE s.name = $service
              AND ($provider IS NULL OR s.cloudProvider = $provider)
            OPTIONAL MATCH (s)-[:EQUIVALENT_TO]-(eq:Service)
            RETURN collect(DISTINCT {
                name: eq.name,
                provider: eq.cloudProvider
            }) AS equivalents
        """, service=service_name, provider=provider)

        record = result.single()
        return record["equivalents"] if record else []


# -------------------------------------------------
# Get Resources for a Service
# -------------------------------------------------
def get_resources_for_service(service_name, provider=None):
    with driver.session() as session:
        result = session.run("""
            MATCH (r:Resource)-[:BELONGS_TO]->(s:Service)
            WHERE s.name = $service
              AND ($provider IS NULL OR s.cloudProvider = $provider)
            RETURN r.id AS resource_id
        """, service=service_name, provider=provider)

        return [r["resource_id"] for r in result]


# -------------------------------------------------
# Calculate Cost from SQLite
# -------------------------------------------------
def calculate_cost_for_resources(resource_ids):
    conn = sqlite3.connect(_BILLING_DB)
    cursor = conn.cursor()

    total_cost = 0

    for rid in resource_ids:
        aws_cost = cursor.execute("""
            SELECT SUM(BilledCost)
            FROM aws_billing
            WHERE ResourceId = ?
        """, (rid,)).fetchone()[0]

        azure_cost = cursor.execute("""
            SELECT SUM(BilledCost)
            FROM azure_billing
            WHERE ResourceId = ?
        """, (rid,)).fetchone()[0]

        total_cost += (aws_cost or 0)
        total_cost += (azure_cost or 0)

    conn.close()
    return round(total_cost, 2)


# -------------------------------------------------
# Hybrid Query (Vector + Graph + Cost)
# -------------------------------------------------
def hybrid_query(user_query, top_k=5):

    # 1️⃣ Vector Search
    query_embedding = get_model().encode(user_query).tolist()

    with driver.session() as session:
        result = session.run("""
            CALL db.index.vector.queryNodes(
                'service_embedding_index',
                $top_k,
                $embedding
            )
            YIELD node, score
            RETURN node.name AS service,
                   node.cloudProvider AS provider,
                   score
            ORDER BY score DESC
        """, embedding=query_embedding, top_k=top_k)

        matches = result.data()

    if not matches:
        return "No relevant service found."

    # 🔥 Keyword Boost Re-ranking
    query_lower = user_query.lower()
    boosted_scores = []

    for match in matches:
        service_name = match["service"]
        provider = match["provider"]
        score = match["score"]

        boost = 0

        if "compute" in query_lower and "compute" in service_name.lower():
            boost += 0.15

        if "database" in query_lower and "database" in service_name.lower():
            boost += 0.15

        if "storage" in query_lower and "storage" in service_name.lower():
            boost += 0.15

        if "network" in query_lower and (
            "network" in service_name.lower() or
            "vpc" in service_name.lower() or
            "bandwidth" in service_name.lower()
        ):
            boost += 0.15

        boosted_scores.append({
            "service": service_name,
            "provider": provider,
            "score": score + boost
        })

    # Sort by boosted score
    boosted_scores.sort(key=lambda x: x["score"], reverse=True)

    primary_service = boosted_scores[0]["service"]
    primary_provider = boosted_scores[0]["provider"]

    # 2️⃣ Get Equivalent Services
    equivalents = get_service_and_equivalents(primary_service, primary_provider)

    # 3️⃣ Cost for Primary Service
    primary_resources = get_resources_for_service(primary_service, primary_provider)
    primary_cost = calculate_cost_for_resources(primary_resources)

    # 4️⃣ Cost for Equivalent Services
    equivalent_costs = {}

    for eq in equivalents:
        eq_name = eq["name"]
        eq_provider = eq["provider"]

        eq_resources = get_resources_for_service(eq_name, eq_provider)
        eq_cost = calculate_cost_for_resources(eq_resources)

        equivalent_costs[eq_name] = {
            "provider": eq_provider,
            "resource_count": len(eq_resources),
            "total_cost": eq_cost
        }

    return {
        "primary_service": primary_service,
        "primary_provider": primary_provider,
        "primary_resource_count": len(primary_resources),
        "primary_total_cost": primary_cost,
        "equivalent_services": equivalent_costs
    }


# -------------------------------------------------
# CLI Test Mode
# -------------------------------------------------
if __name__ == "__main__":
    query = input("Ask about a cloud service: ")
    result = hybrid_query(query)

    print("\n🔎 Cross-Cloud Comparison Result:\n")
    print(result)
