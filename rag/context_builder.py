# rag/context_builder.py
# Hybrid context builder: intent detection + multi-hop graph traversal + vector search

import re
from datetime import datetime
from graph.neo4j_connection import driver
_model = None

def get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model


# ─────────────────────────────────────────────────────────────────────────────
# VECTOR SEARCH (multi-index)
# ─────────────────────────────────────────────────────────────────────────────
def vector_search(index_name, query, top_k=5):
    model = get_model()
    embedding = model.encode(query).tolist()
    try:
        with driver.session() as session:
            result = session.run(
                f"""
                CALL db.index.vector.queryNodes(
                    '{index_name}', $top_k, $embedding
                )
                YIELD node, score
                RETURN node, score
                ORDER BY score DESC
                """,
                embedding=embedding, top_k=top_k,
            )
            return result.data()
    except Exception:
        return []


def multi_index_search(query, top_k=3):
    """Search across FOCUSColumn + Service + Charge indexes for richer context."""
    results = []
    for idx in ["focus_embedding_index", "service_embedding_index", "charge_embedding_index"]:
        hits = vector_search(idx, query, top_k=top_k)
        results.extend(hits)
    # Deduplicate and sort by score
    seen = set()
    deduped = []
    for r in sorted(results, key=lambda x: x["score"], reverse=True):
        node = r["node"]
        key = str(dict(node))
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    return deduped[:top_k * 2]


# ─────────────────────────────────────────────────────────────────────────────
# GRAPH CONTEXT EXPANSION (multi-hop traversal)
# ─────────────────────────────────────────────────────────────────────────────
def expand_service_context(service_name):
    with driver.session() as session:
        result = session.run("""
            MATCH (s:Service {name: $service})
            OPTIONAL MATCH (s)<-[:USES_SERVICE]-(c:CostRecord)
            OPTIONAL MATCH (c)-[:HAS_CHARGE]->(ch:Charge)
            OPTIONAL MATCH (s)-[:EQUIVALENT_TO]-(eq:Service)
            RETURN
                s.name AS service,
                s.serviceCategory AS category,
                s.cloudProvider AS provider,
                count(DISTINCT c) AS record_count,
                sum(c.effectiveCost) AS total_cost,
                collect(DISTINCT ch.category) AS charge_types,
                collect(DISTINCT eq.name) AS equivalents
        """, service=service_name)
        return result.single()


def get_focus_column_context(col_names):
    """Multi-hop: FOCUSColumn → DerivationRule + OntologyClass."""
    context_parts = []
    with driver.session() as session:
        for col in col_names:
            result = session.run("""
                MATCH (f:FOCUSColumn {name: $name})
                OPTIONAL MATCH (f)-[:DERIVED_BY]->(dr:DerivationRule)
                OPTIONAL MATCH (f)-[:BELONGS_TO_CLASS]->(cls:OntologyClass)
                OPTIONAL MATCH (f)-[:DEFINED_BY]->(std:Standard)
                RETURN
                    f.name AS name,
                    f.description AS description,
                    f.dataType AS dataType,
                    f.nullable AS nullable,
                    f.validationRule AS rule,
                    f.category AS category,
                    dr.formula AS formula,
                    dr.description AS derivation,
                    cls.name AS ontologyClass,
                    std.name AS standard
            """, name=col).single()

            if result:
                text = (
                    f"FOCUS Column: {result['name']}\n"
                    f"  Description: {result['description']}\n"
                    f"  DataType: {result['dataType']} | Nullable: {result['nullable']} | Validation: {result['rule']}\n"
                    f"  Category: {result['category']} | OntologyClass: {result['ontologyClass']}\n"
                )
                if result["formula"]:
                    text += f"  DerivationFormula: {result['formula']}\n"
                    text += f"  DerivationNote: {result['derivation']}\n"
                context_parts.append(text)
    return "\n".join(context_parts)


# ─────────────────────────────────────────────────────────────────────────────
# INTENT DETECTION
# ─────────────────────────────────────────────────────────────────────────────
FOCUS_COLUMN_MAP = {
    "billedcost":       "BilledCost",
    "effectivecost":    "EffectiveCost",
    "listcost":         "ListCost",
    "contractedcost":   "ContractedCost",
    "consumedquantity": "ConsumedQuantity",
    "consumedunit":     "ConsumedUnit",
    "pricingquantity":  "PricingQuantity",
    "pricingunit":      "PricingUnit",
    "chargecategory":   "ChargeCategory",
    "chargefrequency":  "ChargeFrequency",
    "chargedescription":"ChargeDescription",
    "chargeclass":      "ChargeClass",
    "resourceid":       "ResourceId",
    "resourcename":     "ResourceName",
    "resourcetype":     "ResourceType",
    "servicename":      "ServiceName",
    "servicecategory":  "ServiceCategory",
    "billingaccountid": "BillingAccountId",
    "subaccountid":     "SubAccountId",
    "regionid":         "RegionId",
    "regionname":       "RegionName",
    "billingcurrency":  "BillingCurrency",
    "tags":             "Tags",
    "commitmentdiscountid": "CommitmentDiscountId",
}


def detect_intent(query: str):
    q = query.lower()

    # FOCUS schema intent
    if ("focus" in q and ("column" in q or "spec" in q or "differ" in q)):
        return "focus_schema"

    # ContractedCost QUESTION — must come BEFORE column_definition
    # (ContractedCost is in FOCUS_COLUMN_MAP and would be caught as column_definition otherwise)
    if ("contracted" in q and "unit" in q) or ("contractedcost" in q.replace(" ", "").lower() and ("differ" in q or "when" in q or "?" in q or "can" in q)):
        return "contracted_cost_question"

    # Column definition (check BEFORE generic cost checks)
    matched = [FOCUS_COLUMN_MAP[k] for k in FOCUS_COLUMN_MAP if k in q]
    if matched:
        return "column_definition", matched

    # Storage comparison — must be BEFORE cross_cloud_comparison (both have AWS+Azure+cost)
    if ("storage" in q and "compare" in q) or ("storage" in q and ("aws" in q or "azure" in q) and "cost" in q):
        return "storage_comparison"

    # Cross-cloud comparison — MUST be before cost_aggregation which also catches 'total'/'cost'
    if (
        ("aws" in q and "azure" in q) and
        ("cost" in q or "breakdown" in q or "compare" in q or "total" in q)
    ) or ("cross" in q and "cloud" in q) or ("compare" in q and "provider" in q):
        return "cross_cloud_comparison"

    # AWS compute
    if "aws" in q and ("compute" in q or "ec2" in q or "lambda" in q):
        return "aws_compute"

    # Azure equivalent
    if ("azure" in q and "equivalent" in q) or ("equivalent" in q and "s3" in q) or ("azure" in q and "s3" in q):
        return "azure_equivalent"


    if ("top" in q and "expensive" in q) or ("top" in q and "production" in q) or ("most expensive" in q):
        return "top_resources"

    # Commitment double counting
    if "commitment" in q and ("double" in q or "utilization" in q or "exclude" in q or "excluded" in q or "charge categor" in q):
        return "commitment_double_counting"

    # Why total increases — commitment purchases cause total to grow
    if (("why" in q and "total" in q) or
            ("total" in q and "increase" in q) or
            ("commitment" in q and "purchase" in q) or
            ("include" in q and "commitment" in q)):
        return "why_total_increases"

    # Which cost type / analyze cloud spend
    if ("cost type" in q and "analyze" in q) or ("which cost" in q) or ("analyze cloud spend" in q) or ("cloud spend" in q):
        return "cost_type_analysis"

    # Compare (generic cross-cloud)
    if "compare" in q:
        return "cross_cloud_comparison"

    # Cost aggregation (generic)
    if any(k in q for k in ["cost", "spend", "total", "expense", "bill"]):
        return "cost_aggregation"

    return "general"


def extract_billing_period(query: str):
    q = query.lower()
    match = re.search(r"\b(20\d{2})-(0[1-9]|1[0-2])\b", q)
    if match:
        return match.group(0)
    month_match = re.search(
        r"\b(january|february|march|april|may|june|july|august|"
        r"september|october|november|december)\s+(20\d{2})\b",
        q,
    )
    if month_match:
        month_str = month_match.group(1)
        year = month_match.group(2)
        month_number = datetime.strptime(month_str, "%B").month
        return f"{year}-{month_number:02d}"
    return None


# ─────────────────────────────────────────────────────────────────────────────
# MAIN CONTEXT BUILDER
# ─────────────────────────────────────────────────────────────────────────────
def build_context(query: str) -> dict:
    billing_period = extract_billing_period(query)
    intent_result  = detect_intent(query)

    # Unpack intent (may be a tuple if column_definition)
    if isinstance(intent_result, tuple):
        intent, matched_cols = intent_result
    else:
        intent = intent_result
        matched_cols = []

    # ── Short-circuit for structured intents ─────────────────────────────────
    if intent == "focus_schema":
        return _focus_schema_context(billing_period)

    if intent == "column_definition" and matched_cols:
        col_context = get_focus_column_context(matched_cols)
        return {
            "intent": intent,
            "columns": matched_cols,
            "context": col_context,
            "billing_period": billing_period,
            "provenance": [f"FOCUSColumn({c})" for c in matched_cols],
        }

    if intent in ("aws_compute", "azure_equivalent", "storage_comparison",
                   "top_resources", "commitment_double_counting",
                   "why_total_increases", "cost_type_analysis",
                   "contracted_cost_question", "cross_cloud_comparison",
                   "cost_aggregation"):
        return {
            "intent": intent,
            "billing_period": billing_period,
            "context": "",
            "provenance": [],
        }

    # ── Hybrid vector + graph for general queries ────────────────────────────
    hits = multi_index_search(query, top_k=4)
    context_chunks = []
    provenance = []

    for item in hits:
        node = item["node"]
        score = item["score"]
        labels = list(node.labels) if hasattr(node, "labels") else []

        if "Service" in labels:
            sname = node.get("name")
            if sname:
                exp = expand_service_context(sname)
                if exp:
                    total = exp.get("total_cost") or 0
                    count = exp.get("record_count") or 0
                    cats  = exp.get("charge_types") or []
                    eqs   = exp.get("equivalents") or []
                    chunk = (
                        f"Service: {sname} ({node.get('cloudProvider','?')}) | "
                        f"Category: {node.get('serviceCategory','?')}\n"
                        f"  Total EffectiveCost: ${total:,.2f} | Records: {count}\n"
                        f"  Charge Types: {', '.join([c for c in cats if c])}\n"
                        f"  Equivalent Services: {', '.join(eqs) or 'None'}"
                    )
                    context_chunks.append(chunk)
                    provenance.append(f"CostRecord → USES_SERVICE → Service({sname})")

        elif "FOCUSColumn" in labels:
            cname = node.get("name")
            if cname:
                col_ctx = get_focus_column_context([cname])
                if col_ctx:
                    context_chunks.append(col_ctx)
                    provenance.append(f"FOCUSColumn({cname}) [score={score:.3f}]")

        elif "Charge" in labels:
            cat  = node.get("category")
            desc = node.get("description")
            if cat:
                context_chunks.append(
                    f"Charge Category: {cat}\n"
                    f"  Description: {desc}\n"
                    f"  Frequency: {node.get('frequency','?')}"
                )
                provenance.append(f"Charge({cat}) [score={score:.3f}]")

    return {
        "intent": "general",
        "billing_period": billing_period,
        "context": "\n\n".join(dict.fromkeys(context_chunks)),
        "provenance": list(dict.fromkeys(provenance)),
    }


def _focus_schema_context(billing_period):
    """Retrieve all FOCUS columns vs vendor columns for focus_schema intent."""
    with driver.session() as session:
        focus_cols = session.run("""
            MATCH (f:FOCUSColumn)
            RETURN f.name AS name, f.description AS desc,
                   f.dataType AS dtype, f.category AS cat
            ORDER BY f.category, f.name
        """).data()

        aws_maps = session.run("""
            MATCH (a:AWSColumn)-[r:MAPS_TO]->(f:FOCUSColumn)
            RETURN a.name AS aws, f.name AS focus, r.transformationType AS trans
        """).data()

        azure_maps = session.run("""
            MATCH (z:AzureColumn)-[r:MAPS_TO]->(f:FOCUSColumn)
            RETURN z.name AS azure, f.name AS focus, r.transformationType AS trans
        """).data()

    columns = [c["name"] for c in focus_cols]
    col_detail = "\n".join(
        f"  - {c['name']} ({c['cat']}): {c['desc'][:80]}..." for c in focus_cols
    ) if focus_cols else "No columns found."

    prov = [
        {"from": "FOCUSColumn", "relationship": "MAPS_TO (AWS)", "to": f"{m['aws']}→{m['focus']}"}
        for m in aws_maps[:3]
    ]

    return {
        "intent": "focus_schema",
        "columns": columns,
        "aws_mappings": aws_maps,
        "azure_mappings": azure_maps,
        "context": col_detail,
        "billing_period": billing_period,
        "provenance": prov,
    }