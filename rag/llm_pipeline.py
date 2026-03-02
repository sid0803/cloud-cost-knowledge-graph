import requests
import json
from datetime import datetime

from rag.context_builder import build_context
from graph.neo4j_connection import driver


# =================================================
# LOCAL LLM
# =================================================
def call_local_llm(prompt: str) -> str:
    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": "phi",
            "prompt": prompt,
            "stream": False,
        },
        timeout=60,
    )

    response.raise_for_status()
    return response.json().get("response", "").strip()


# =================================================
# CONFIDENCE SCORING
# =================================================
def compute_confidence(intent, provenance, used_llm=False):
    base = 0.5

    if intent != "general":
        base += 0.2  # deterministic graph query

    evidence_score = min(len(provenance) * 0.1, 0.3)
    llm_penalty = -0.2 if used_llm else 0

    score = base + evidence_score + llm_penalty
    return round(max(0.0, min(score, 1.0)), 2)


# =================================================
# EVALUATION LOGGER
# =================================================
def log_evaluation(question, intent, retrieval_method,
                   provenance, confidence, billing_period):

    log_entry = {
        "query": question,
        "intent": intent,
        "retrieval_method": retrieval_method,
        "billing_period": billing_period,
        "provenance_count": len(provenance),
        "confidence": confidence,
        "timestamp": datetime.now().isoformat()
    }

    with open("evaluation_log.json", "a") as f:
        json.dump(log_entry, f)
        f.write("\n")


# =================================================
# MAIN ANSWER FUNCTION
# =================================================
def generate_answer(question: str):

    data = build_context(question)
    intent = data.get("intent")
    billing_period = data.get("billing_period")
    query_lower = question.lower()

    retrieval_method = "graph" if intent != "general" else "hybrid"

    # =================================================
    # FOCUS SCHEMA
    # =================================================
    if intent == "focus_schema":

        columns = data.get("columns", [])
        answer = "Core FOCUS columns include:\n\n"
        answer += "\n".join(f"- {col}" for col in columns)

        provenance = data.get("provenance", [])
        confidence = compute_confidence(intent, provenance)

        log_evaluation(question, intent, retrieval_method,
                       provenance, confidence, billing_period)

        return {
            "answer": answer,
            "provenance": provenance,
            "retrieval_method": retrieval_method,
            "confidence": confidence
        }

    # =================================================
    # COLUMN DEFINITIONS
    # =================================================
    if intent == "column_definition":

        columns = data.get("columns", [])
        lines = []
        provenance = []

        with driver.session() as session:
            for col in columns:
                result = session.run("""
                    MATCH (f:FOCUSColumn {name:$col})
                    RETURN f.description AS description,
                           f.dataType AS dataType,
                           f.validationRule AS rule
                """, col=col).single()

                if result:
                    lines.append(
                        f"{col}:\n"
                        f"- Description: {result.get('description')}\n"
                        f"- DataType: {result.get('dataType')}\n"
                        f"- Validation Rule: {result.get('rule')}"
                    )
                    provenance.append({
                        "from": "Schema",
                        "relationship": "DESCRIBES",
                        "to": f"FOCUSColumn({col})"
                    })

        if not lines:
            lines = ["Definition not available in ontology."]

        confidence = compute_confidence(intent, provenance)

        log_evaluation(question, intent, retrieval_method,
                       provenance, confidence, billing_period)

        return {
            "answer": "\n\n".join(lines),
            "provenance": provenance,
            "retrieval_method": retrieval_method,
            "confidence": confidence
        }

    # =================================================
    # COST AGGREGATION (with Commitment Exclusion)
    # =================================================
    if intent == "cost_aggregation":

        exclude_commitment = (
            "exclude commitment" in query_lower or
            "without commitment" in query_lower
        )

        with driver.session() as session:

            provider = None
            if "aws" in query_lower:
                provider = "AWS"
            elif "azure" in query_lower:
                provider = "Azure"

            query = """
                MATCH (c:CostRecord)
                OPTIONAL MATCH (c)-[:HAS_CHARGE]->(ch:Charge)
                OPTIONAL MATCH (c)-[:IN_PERIOD]->(p:BillingPeriod)
                WHERE 1=1
            """

            params = {}

            if provider:
                query += " AND c.cloudProvider = $provider"
                params["provider"] = provider

            if billing_period:
                query += " AND p.start STARTS WITH $billing_period"
                params["billing_period"] = billing_period

            if exclude_commitment:
                query += " AND (ch.category IS NULL OR ch.category <> 'Commitment')"

            query += """
                RETURN
                    sum(c.effectiveCost) AS total_effective,
                    sum(c.billedCost) AS total_billed,
                    count(c) AS record_count
            """

            result = session.run(query, **params).single()

            total_effective = result["total_effective"] or 0
            total_billed = result["total_billed"] or 0
            record_count = result["record_count"] or 0

        answer = (
            f"{provider or 'Cloud'} Cost Summary:\n\n"
            f"Total EffectiveCost: ${total_effective:.2f}\n"
            f"Total BilledCost: ${total_billed:.2f}\n"
            f"Records: {record_count}"
        )

        if exclude_commitment:
            answer += "\n\n(Commitment charges excluded)"

        provenance = [{
            "from": "CostRecord",
            "relationship": "AGGREGATED",
            "to": provider or "Cloud"
        }]

        confidence = compute_confidence(intent, provenance)

        log_evaluation(question, intent, retrieval_method,
                       provenance, confidence, billing_period)

        return {
            "answer": answer,
            "provenance": provenance,
            "retrieval_method": retrieval_method,
            "confidence": confidence
        }

    # =================================================
    # LLM FALLBACK
    # =================================================
    prompt = f"""
You are a cloud cost ontology expert.

Answer strictly using provided context.
If answer not found, say:
"Not available in graph."

Context:
{data.get('context', '')}

Question:
{question}
"""

    answer = call_local_llm(prompt)
    provenance = data.get("provenance", [])

    confidence = compute_confidence(intent, provenance, used_llm=True)

    log_evaluation(question, intent, retrieval_method,
                   provenance, confidence, billing_period)

    return {
        "answer": answer,
        "provenance": provenance,
        "retrieval_method": retrieval_method,
        "confidence": confidence
    }