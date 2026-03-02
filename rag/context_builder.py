# rag/context_builder.py

import re
from datetime import datetime

from graph.neo4j_connection import driver
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")


# =================================================
# VECTOR SEARCH
# =================================================
def vector_search(index_name, query, top_k=5):

    embedding = model.encode(query).tolist()

    with driver.session() as session:
        result = session.run(
            f"""
            CALL db.index.vector.queryNodes(
                '{index_name}',
                $top_k,
                $embedding
            )
            YIELD node, score
            RETURN node, score
            ORDER BY score DESC
            """,
            embedding=embedding,
            top_k=top_k,
        )

        return result.data()


# =================================================
# SERVICE EXPANSION (TRUE GRAPH TRAVERSAL)
# =================================================
def expand_service_context(service_name):

    with driver.session() as session:
        result = session.run("""
            MATCH (s:Service {name:$service})
            OPTIONAL MATCH (s)<-[:USES_SERVICE]-(c:CostRecord)
            OPTIONAL MATCH (c)-[:HAS_CHARGE]->(ch:Charge)
            RETURN
                s.name AS service,
                count(DISTINCT c) AS record_count,
                sum(c.effectiveCost) AS total_cost,
                collect(DISTINCT ch.category) AS charge_types
        """, service=service_name)

        return result.single()


# =================================================
# CONTEXT BUILDER
# =================================================
def build_context(query):

    query_lower = query.lower()
    context_chunks = []
    provenance_paths = []

    # =================================================
    # 🔥 Billing Period Extraction
    # =================================================
    billing_period = None

    match = re.search(r"\b(20\d{2})-(0[1-9]|1[0-2])\b", query_lower)
    if match:
        billing_period = match.group(0)

    if not billing_period:
        month_match = re.search(
            r"\b(january|february|march|april|may|june|july|august|"
            r"september|october|november|december)\s+(20\d{2})\b",
            query_lower,
        )

        if month_match:
            month_str = month_match.group(1)
            year = month_match.group(2)
            month_number = datetime.strptime(month_str, "%B").month
            billing_period = f"{year}-{month_number:02d}"

    # =================================================
    # 1️⃣ FOCUS SCHEMA INTENT
    # =================================================
    if "focus" in query_lower and "column" in query_lower:

        focus_cols = vector_search("focus_embedding_index", query, top_k=10)

        columns = []
        for item in focus_cols:
            col_name = item["node"].get("name")
            if col_name:
                columns.append(col_name)
                provenance_paths.append(
                    f"Schema → FOCUSColumn({col_name})"
                )

        return {
            "intent": "focus_schema",
            "columns": sorted(set(columns)),
            "provenance": sorted(set(provenance_paths)),
        }

    # =================================================
    # 2️⃣ COLUMN DEFINITION INTENT
    # =================================================
    focus_column_mapping = {
        "billedcost": "BilledCost",
        "effectivecost": "EffectiveCost",
        "contractedcost": "ContractedCost",
        "consumedquantity": "ConsumedQuantity",
        "pricingquantity": "PricingQuantity",
        "chargecategory": "ChargeCategory",
        "chargefrequency": "ChargeFrequency",
        "chargedescription": "ChargeDescription",
        "regionname": "RegionName",
        "resourceid": "ResourceId",
        "servicename": "ServiceName",
    }

    matched_columns = [
        proper
        for key, proper in focus_column_mapping.items()
        if key in query_lower
    ]

    if matched_columns:
        return {
            "intent": "column_definition",
            "columns": sorted(set(matched_columns)),
            "provenance": [
                f"Schema → FOCUSColumn({col})"
                for col in sorted(set(matched_columns))
            ],
        }

    # =================================================
    # 3️⃣ KEYWORD INTENTS
    # =================================================
    if any(k in query_lower for k in ["highest cost", "top services", "most expensive"]):
        return {
            "intent": "top_services",
            "billing_period": billing_period,
            "provenance": [],
        }

    if any(k in query_lower for k in ["compare", "cross cloud", "which cloud"]):
        return {
            "intent": "cross_cloud_comparison",
            "billing_period": billing_period,
            "provenance": [],
        }

    if any(k in query_lower for k in ["cost", "spend", "total", "expense"]):
        return {
            "intent": "cost_aggregation",
            "billing_period": billing_period,
            "query": query,
            "provenance": [],
        }

    # =================================================
    # 4️⃣ TRUE HYBRID GENERAL SEARCH
    # =================================================

    services = vector_search("service_embedding_index", query, top_k=3)

    for item in services:
        service_name = item["node"].get("name")
        if not service_name:
            continue

        # Expand graph context
        expansion = expand_service_context(service_name)

        if expansion:
            total_cost = expansion.get("total_cost") or 0
            record_count = expansion.get("record_count") or 0
            charge_types = expansion.get("charge_types") or []

            context_chunks.append(
                f"Service: {service_name}\n"
                f"- Total EffectiveCost: ${total_cost:.2f}\n"
                f"- CostRecords: {record_count}\n"
                f"- Charge Types: {', '.join([c for c in charge_types if c])}"
            )

            provenance_paths.append(
                f"CostRecord → USES_SERVICE → Service({service_name})"
            )

    # Also enrich with FOCUS column matches
    focus_cols = vector_search("focus_embedding_index", query, top_k=3)

    for item in focus_cols:
        col_name = item["node"].get("name")
        if col_name:
            context_chunks.append(f"FOCUS Column: {col_name}")
            provenance_paths.append(
                f"Schema → FOCUSColumn({col_name})"
            )

    return {
        "intent": "general",
        "billing_period": billing_period,
        "context": "\n\n".join(sorted(set(context_chunks))),
        "provenance": sorted(set(provenance_paths)),
    }