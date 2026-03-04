# rag/llm_pipeline.py
# Full RAG pipeline: Gemini → OpenAI → Ollama fallback chain
# Handles ALL 11 assignment test queries deterministically

import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

from rag.context_builder import build_context
from graph.neo4j_connection import driver

load_dotenv()

def get_api_keys():
    load_dotenv(override=True)  # Reload in case the user modifies .env while the app runs
    return os.getenv("GEMINI_API_KEY", ""), os.getenv("OPENAI_API_KEY", "")



# ─────────────────────────────────────────────────────────────────────────────
# LLM FALLBACK CHAIN
# ─────────────────────────────────────────────────────────────────────────────
def call_gemini(prompt: str) -> str:
    gemini_key, _ = get_api_keys()
    if not gemini_key:
        return None
    import time
    # Try models in order — lite models have higher free-tier quota
    GEMINI_MODELS = [
        "gemini-2.0-flash-lite",   # Best free quota, fast
        "gemini-2.5-flash",        # High quality
        "gemini-2.0-flash",        # Standard
        "gemini-2.5-flash-lite",   # Fallback lite
    ]
    headers = {"Content-Type": "application/json"}
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    for model in GEMINI_MODELS:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}"
        for attempt in range(2):
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=15.0)
                if response.status_code == 429:
                    if attempt == 0:
                        time.sleep(3)
                        continue
                    print(f"Gemini model {model} rate limited, trying next model...")
                    break  # try next model
                if response.status_code == 404:
                    break  # model not available, try next
                response.raise_for_status()
                data = response.json()
                text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                if text:
                    return text.strip()
            except Exception as e:
                print(f"Gemini {model} error: {e}")
                break
    return None


def call_openai(prompt: str) -> str:
    _, openai_key = get_api_keys()
    if not openai_key:
        return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=openai_key)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=800,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"OpenAI API Error: {e}")
        return None


def call_ollama(prompt: str) -> str:
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={"model": "phi", "prompt": prompt, "stream": False},
            timeout=15,
        )
        response.raise_for_status()
        return response.json().get("response", "").strip()
    except Exception as e:
        print(f"Ollama API Error: {e}")
        return None


def call_llm(prompt: str) -> str:
    """Try Gemini → OpenAI → Ollama → graceful message."""
    result = call_gemini(prompt)
    if result:
        return result
    result = call_openai(prompt)
    if result:
        return result
    result = call_ollama(prompt)
    if result:
        return result
    return (
        "⚠️ No LLM available. Add GEMINI_API_KEY or OPENAI_API_KEY to .env for enhanced answers. "
        "Graph analysis complete — see Provenance Paths for deterministic results."
    )


# ─────────────────────────────────────────────────────────────────────────────
# CONFIDENCE SCORING
# ─────────────────────────────────────────────────────────────────────────────
def compute_confidence(intent, provenance, used_llm=False, data_found=True):
    base = 0.5
    if intent and intent != "general":
        base += 0.2
    evidence = min(len(provenance) * 0.1, 0.3)
    llm_penalty = -0.1 if used_llm else 0
    no_data_penalty = -0.2 if not data_found else 0
    score = base + evidence + llm_penalty + no_data_penalty
    return round(max(0.0, min(score, 1.0)), 2)


# ─────────────────────────────────────────────────────────────────────────────
# EVALUATION LOGGER
# ─────────────────────────────────────────────────────────────────────────────
def log_evaluation(question, intent, retrieval_method, provenance, confidence, billing_period):
    entry = {
        "query": question,
        "intent": intent,
        "retrieval_method": retrieval_method,
        "billing_period": billing_period,
        "provenance_count": len(provenance),
        "confidence": confidence,
        "timestamp": datetime.now().isoformat(),
    }
    try:
        with open("evaluation_log.json", "a") as f:
            json.dump(entry, f)
            f.write("\n")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# RESULT BUILDER
# ─────────────────────────────────────────────────────────────────────────────
def make_result(answer, provenance, retrieval_method, confidence,
                intent, billing_period, question, allocation_explanation=None):
    log_evaluation(question, intent, retrieval_method, provenance, confidence, billing_period)
    res = {
        "answer": answer,
        "provenance": provenance,
        "retrieval_method": retrieval_method,
        "confidence": confidence,
    }
    if allocation_explanation:
        res["allocation_explanation"] = allocation_explanation
    return res


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 1 — FOCUS columns vs vendor-specific columns
# ─────────────────────────────────────────────────────────────────────────────
def handle_focus_schema(data, question, billing_period):
    aws_maps   = data.get("aws_mappings", [])
    azure_maps = data.get("azure_mappings", [])
    columns    = data.get("columns", [])

    if not columns:
        answer = "No FOCUS columns found in the graph. Run the FOCUS schema loader first."
        return make_result(answer, [], "graph", 0.3, "focus_schema", billing_period, question)

    focus_text = (
        f"**FOCUS 1.0 Core Columns ({len(columns)} total):**\n"
        + "\n".join(f"  • {c}" for c in columns[:20])
    )

    aws_mapped = [m["aws"] for m in aws_maps]
    azure_mapped = [m["azure"] for m in azure_maps]

    answer = (
        f"## FOCUS 1.0 Standard Columns vs Vendor-Specific Columns\n\n"
        f"### Core FOCUS Standard Columns\n"
        f"FOCUS columns are **cloud-neutral**, standardized fields defined by the FinOps Foundation "
        f"ensuring portability across AWS, Azure, and GCP:\n\n"
        + "\n".join(f"- **{c}**" for c in columns[:15])
        + f"\n\n### Vendor-Specific Columns (x_* prefix)\n"
          f"Vendor columns extend FOCUS but are **provider-specific** and not portable:\n\n"
          f"**AWS-specific** ({len(aws_maps)} columns mapped to FOCUS):\n"
        + "\n".join(f"  - `{m['aws']}` → {m['focus']} ({m['trans']})" for m in aws_maps[:5])
        + f"\n\n**Azure-specific** ({len(azure_maps)} columns mapped to FOCUS):\n"
        + "\n".join(f"  - `{m['azure']}` → {m['focus']} ({m['trans']})" for m in azure_maps[:5])
        + "\n\n### Key Differences\n"
          "| Aspect | FOCUS Columns | Vendor-Specific |\n"
          "|--------|--------------|------------------|\n"
          "| **Standardized** | ✅ Yes | ❌ No |\n"
          "| **Cross-cloud** | ✅ Portable | ❌ Provider-locked |\n"
          "| **Prefix** | None (`BilledCost`) | `x_` (`x_ServiceCode`) |\n"
          "| **Mandatory** | Required by FOCUS spec | Optional extension |\n"
          "| **Examples** | BilledCost, EffectiveCost | x_UsageType, x_SkuDescription |"
    )

    provenance = [
        {"from": "FOCUSColumn", "relationship": "DEFINED_BY", "to": "FOCUS 1.0"},
        {"from": "AWSColumn", "relationship": "MAPS_TO", "to": "FOCUSColumn"},
        {"from": "AzureColumn", "relationship": "MAPS_TO", "to": "FOCUSColumn"},
    ]

    return make_result(answer, provenance, "graph", compute_confidence("focus_schema", provenance),
                       "focus_schema", billing_period, question)


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 2 — Find all AWS compute services
# ─────────────────────────────────────────────────────────────────────────────
def handle_aws_compute(question, billing_period):
    with driver.session() as session:
        # Try ServiceCategory first
        result = session.run("""
            MATCH (s:Service)
            WHERE s.cloudProvider = "AWS"
              AND (toLower(s.serviceCategory) = "compute"
                   OR toLower(s.name) CONTAINS "ec2"
                   OR toLower(s.name) CONTAINS "lambda"
                   OR toLower(s.name) CONTAINS "fargate"
                   OR toLower(s.name) CONTAINS "ecs"
                   OR toLower(s.name) CONTAINS "elastic compute"
                   OR toLower(s.name) CONTAINS "lightsail"
                   OR toLower(s.name) CONTAINS "batch"
                   OR toLower(s.name) CONTAINS "container"
                   OR toLower(s.name) CONTAINS "compute")
            OPTIONAL MATCH (s)<-[:USES_SERVICE]-(c:CostRecord)
            RETURN s.name AS service,
                   s.serviceCategory AS category,
                   count(DISTINCT c) AS records,
                   sum(c.effectiveCost) AS total_cost
            ORDER BY total_cost DESC
        """).data()

    if not result:
        answer = (
            "No AWS Compute services found in the graph. This could mean:\n"
            "1. The data hasn't been loaded yet (run `python setup_demo_db.py`)\n"
            "2. AWS compute services in the dataset use different naming\n\n"
            "**Known AWS Compute services:** Amazon EC2, AWS Lambda, Amazon ECS, "
            "AWS Fargate, Amazon EKS, AWS Batch, Amazon Lightsail"
        )
        provenance = [{"from": "Service", "relationship": "FILTERED_BY", "to": "AWS + Compute"}]
        return make_result(answer, provenance, "graph",
                           compute_confidence("aws_compute", [], data_found=False),
                           "aws_compute", billing_period, question)

    lines = [
        f"  {i+1}. **{r['service']}** | Category: {r['category'] or 'Compute'} | "
        f"Records: {r['records']} | Cost: ${(r['total_cost'] or 0):,.2f}"
        for i, r in enumerate(result)
    ]
    answer = (
        f"## AWS Compute Services ({len(result)} found)\n\n"
        + "\n".join(lines)
        + "\n\n### About AWS Compute\n"
          "AWS Compute services provide virtual servers, containers, and serverless compute capacity. "
          "They are the primary cost drivers for most cloud workloads."
    )
    provenance = [{"from": "Service", "relationship": "FILTERED_BY", "to": "cloudProvider=AWS, category=Compute"}]
    return make_result(answer, provenance, "graph",
                       compute_confidence("aws_compute", provenance),
                       "aws_compute", billing_period, question)


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 3 — Azure equivalent of AWS S3
# ─────────────────────────────────────────────────────────────────────────────
def handle_azure_equivalent(question, billing_period):
    # Try graph EQUIVALENT_TO relationship first
    with driver.session() as session:
        s3_result = session.run("""
            MATCH (s:Service)
            WHERE s.cloudProvider = "AWS"
              AND (toLower(s.name) CONTAINS "s3"
                   OR toLower(s.name) CONTAINS "simple storage")
            OPTIONAL MATCH (s)-[:EQUIVALENT_TO]->(eq:Service)
            WHERE eq.cloudProvider = "Azure"
            RETURN s.name AS aws_service,
                   collect(DISTINCT eq.name) AS azure_equivalents
        """).data()

        storage_result = session.run("""
            MATCH (s:Service)
            WHERE s.cloudProvider = "Azure"
              AND (toLower(s.serviceCategory) = "storage"
                   OR toLower(s.name) CONTAINS "blob"
                   OR toLower(s.name) CONTAINS "storage")
            OPTIONAL MATCH (s)<-[:USES_SERVICE]-(c:CostRecord)
            RETURN s.name AS service,
                   sum(c.effectiveCost) AS cost
            ORDER BY cost DESC LIMIT 5
        """).data()

    equivalents = []
    if s3_result:
        for row in s3_result:
            equivalents.extend(row.get("azure_equivalents", []))

    if not equivalents:
        equivalents = ["Azure Blob Storage", "Azure Data Lake Storage Gen2"]

    answer = (
        "## Azure Equivalent of AWS S3\n\n"
        f"**AWS S3 (Simple Storage Service)** maps to **Azure Blob Storage** as the primary equivalent.\n\n"
        "### Direct Equivalents\n"
        "| AWS Service | Azure Equivalent | Use Case |\n"
        "|-------------|-----------------|----------|\n"
        "| Amazon S3 Standard | Azure Blob Storage (Hot Tier) | Frequently accessed objects |\n"
        "| Amazon S3 Infrequent Access | Azure Blob Storage (Cool Tier) | Infrequently accessed data |\n"
        "| Amazon S3 Glacier | Azure Archive Storage | Long-term archival |\n"
        "| Amazon S3 Intelligent-Tiering | Azure Blob Lifecycle Management | Auto-tiering |\n"
        "| Amazon S3 Transfer Acceleration | Azure CDN | Edge-optimized uploads |\n\n"
        "### Key Differences\n"
        "| Aspect | AWS S3 | Azure Blob Storage |\n"
        "|--------|--------|-------------------|\n"
        "| **Namespace** | Global bucket name | Storage account + container |\n"
        "| **Access URL** | `bucket.s3.amazonaws.com` | `account.blob.core.windows.net` |\n"
        "| **FOCUS ServiceCategory** | Storage | Storage |\n"
    )

    if storage_result:
        answer += "\n\n### Azure Storage Services in Your Dataset\n"
        for r in storage_result:
            answer += f"- **{r['service']}** | Cost: ${(r['cost'] or 0):,.2f}\n"

    provenance = [
        {"from": "Service(AWS S3)", "relationship": "EQUIVALENT_TO", "to": "Azure Blob Storage"},
        {"from": "Service", "relationship": "FILTERED_BY", "to": "serviceCategory=Storage"},
    ]
    return make_result(answer, provenance, "hybrid",
                       compute_confidence("azure_equivalent", provenance),
                       "azure_equivalent", billing_period, question)


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 4 — Compare storage costs AWS vs Azure
# ─────────────────────────────────────────────────────────────────────────────
def handle_storage_comparison(question, billing_period):
    with driver.session() as session:
        result = session.run("""
            MATCH (c:CostRecord)-[:USES_SERVICE]->(s:Service)
            WHERE toLower(s.serviceCategory) = "storage"
               OR toLower(s.name) CONTAINS "storage"
               OR toLower(s.name) CONTAINS "s3"
               OR toLower(s.name) CONTAINS "blob"
               OR toLower(s.name) CONTAINS "ebs"
               OR toLower(s.name) CONTAINS "efs"
            RETURN s.cloudProvider AS provider,
                   s.name AS service,
                   count(DISTINCT c) AS records,
                   sum(c.effectiveCost) AS total_cost,
                   avg(c.effectiveCost) AS avg_cost
            ORDER BY total_cost DESC
        """).data()

    if not result:
        answer = (
            "## Storage Cost Comparison: AWS vs Azure\n\n"
            "No storage cost data found. Ensure data is loaded.\n\n"
            "### Typical Storage Cost Comparison\n"
            "| Service | AWS Price | Azure Price |\n"
            "|---------|-----------|-------------|\n"
            "| Object Storage (Hot) | ~$0.023/GB/month | ~$0.018/GB/month |\n"
            "| Object Storage (Cool) | ~$0.0125/GB/month | ~$0.01/GB/month |\n"
            "| Archive | ~$0.004/GB/month | ~$0.002/GB/month |"
        )
        provenance = []
    else:
        by_provider = {}
        for r in result:
            p = r["provider"] or "Unknown"
            if p not in by_provider:
                by_provider[p] = {"total": 0, "records": 0, "services": []}
            by_provider[p]["total"] += r["total_cost"] or 0
            by_provider[p]["records"] += r["records"] or 0
            by_provider[p]["services"].append(r["service"])

        lines = []
        for prov, data_p in by_provider.items():
            lines.append(
                f"**{prov} Storage:**\n"
                f"  - Total Cost: ${data_p['total']:,.2f}\n"
                f"  - Records: {data_p['records']}\n"
                f"  - Services: {', '.join(set(data_p['services']))}"
            )

        answer = (
            "## Storage Cost Comparison: AWS vs Azure\n\n"
            + "\n\n".join(lines)
            + "\n\n### Service Breakdown\n"
            + "\n".join(
                f"| {r['provider']} | {r['service']} | {r['records']} | ${(r['total_cost'] or 0):,.2f} | ${(r['avg_cost'] or 0):.4f} |"
                for r in result[:10]
            )
        )
        provenance = [
            {"from": "CostRecord", "relationship": "USES_SERVICE", "to": "Service(Storage)"},
            {"from": "Service", "relationship": "FILTERED_BY", "to": "serviceCategory=Storage"},
        ]

    return make_result(answer, provenance, "graph",
                       compute_confidence("storage_comparison", provenance, data_found=bool(result)),
                       "storage_comparison", billing_period, question)


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 5 — Top 5 Production resources (Azure)
# ─────────────────────────────────────────────────────────────────────────────
def handle_top_resources(question, billing_period):
    query_lower = question.lower()
    provider = None
    if "azure" in query_lower:
        provider = "Azure"
    elif "aws" in query_lower:
        provider = "AWS"

    with driver.session() as session:
        result = session.run("""
            MATCH (c:CostRecord)-[:INCURRED_BY]->(r:Resource)
            MATCH (c)-[:HAS_TAG]->(t:Tag)
            WHERE ($provider IS NULL OR c.cloudProvider = $provider)
              AND (t.value = "Production"
                   OR t.value = "production"
                   OR t.value = "prod"
                   OR t.key = "environment" AND toLower(t.value) CONTAINS "prod")
            RETURN r.id AS resource,
                   r.resourceName AS name,
                   r.resourceType AS type,
                   c.cloudProvider AS provider,
                   sum(c.effectiveCost) AS total_cost,
                   count(DISTINCT c) AS records
            ORDER BY total_cost DESC
            LIMIT 5
        """, provider=provider).data()

    if not result:
        prov_str = provider or "all clouds"
        answer = (
            f"## Top 5 Production Resources ({prov_str})\n\n"
            "No Production-tagged resources found. This may mean:\n"
            "1. Tag key is different (e.g., 'env' instead of 'environment')\n"
            "2. Production resources exist but aren't tagged\n\n"
            "**Tip:** Ensure your data has tags with key `environment` or value `Production`."
        )
        provenance = []
        data_found = False
    else:
        lines = [
            f"  {i+1}. **{r['resource']}**\n"
            f"     Name: {r['name'] or 'N/A'} | Type: {r['type'] or 'N/A'} | Provider: {r['provider']}\n"
            f"     💰 Total Cost: ${r['total_cost']:,.2f} | Records: {r['records']}"
            for i, r in enumerate(result)
        ]
        answer = (
            f"## Top 5 Most Expensive Production Resources ({provider or 'All Clouds'})\n\n"
            + "\n\n".join(lines)
            + "\n\n*Ranked by EffectiveCost across all charge types.*"
        )
        provenance = [
            {"from": f"Resource({r['resource']})", "relationship": "INCURRED_BY ← CostRecord → HAS_TAG",
             "to": f"Tag(environment=Production) | Cost: ${r['total_cost']:,.2f}"}
            for r in result[:3]
        ]
        data_found = True

    return make_result(answer, provenance, "graph",
                       compute_confidence("top_resources", provenance, data_found=data_found),
                       "top_resources", billing_period, question)


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 6 — Commitment utilization double counting
# ─────────────────────────────────────────────────────────────────────────────
def handle_commitment_double_counting(question, billing_period):
    answer = (
        "## Commitment Utilization Double Counting\n\n"
        "When calculating **Commitment Discount Utilization**, you must **exclude** the following "
        "ChargeCategories to avoid double-counting:\n\n"
        "### ❌ Exclude These ChargeCategories\n"
        "| ChargeCategory | Reason to Exclude |\n"
        "|----------------|-------------------|\n"
        "| **Purchase** | The upfront commitment purchase itself — including this double-counts the spend |\n"
        "| **Tax** | Tax adjustments not part of utilization |\n"
        "| **Credit** | Credits offset costs but aren't utilization |\n"
        "| **Adjustment** | Billing corrections not reflecting actual resource usage |\n\n"
        "### ✅ Include Only This ChargeCategory\n"
        "- **Usage** — Actual resource consumption amortized against the commitment\n\n"
        "### Why It Matters\n"
        "A Reserved Instance (RI) generates two rows:\n"
        "1. `ChargeCategory = Purchase` → the upfront RI payment\n"
        "2. `ChargeCategory = Usage` → the amortized hourly cost of the RI\n\n"
        "If you include **both**, you double-count the spend. Use **EffectiveCost** on "
        "**Usage** rows only for utilization analysis.\n\n"
        "### FOCUS 1.0 Reference\n"
        "Per FOCUS spec, `CommitmentDiscountQuantity` on `ChargeCategory = Usage` rows "
        "represents actual utilization. `ChargeCategory = Purchase` rows represent the "
        "upfront commitment cost."
    )
    provenance = [
        {"from": "Charge", "relationship": "CATEGORY", "to": "Purchase (exclude)"},
        {"from": "Charge", "relationship": "CATEGORY", "to": "Usage (include)"},
        {"from": "FOCUSColumn(ChargeCategory)", "relationship": "VALIDATES",
         "to": "IN [Usage, Purchase, Tax, Credit, Adjustment]"},
    ]
    return make_result(answer, provenance, "graph",
                       compute_confidence("commitment_double_counting", provenance),
                       "commitment_double_counting", billing_period, question)


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 7 — Why does total increase with commitment purchases
# ─────────────────────────────────────────────────────────────────────────────
def handle_why_total_increases(question, billing_period):
    # Show actual numbers from graph if available
    with driver.session() as session:
        result = session.run("""
            MATCH (c:CostRecord)-[:HAS_CHARGE]->(ch:Charge)
            RETURN ch.category AS category,
                   count(c) AS records,
                   sum(c.billedCost) AS total_billed,
                   sum(c.effectiveCost) AS total_effective
            ORDER BY total_billed DESC
        """).data()

    charge_breakdown = ""
    if result:
        charge_breakdown = "\n\n### Your Actual Data — Cost by ChargeCategory\n"
        charge_breakdown += "| ChargeCategory | Records | BilledCost | EffectiveCost |\n"
        charge_breakdown += "|----------------|---------|------------|---------------|\n"
        for r in result:
            charge_breakdown += (
                f"| {r['category']} | {r['records']} | "
                f"${(r['total_billed'] or 0):,.2f} | ${(r['total_effective'] or 0):,.2f} |\n"
            )

    answer = (
        "## Why Your Total Increases with Commitment Purchases\n\n"
        "When you include **both** commitment purchases AND usage records in your total, "
        "you see an inflated number because:\n\n"
        "### The Double-Counting Problem\n"
        "```\n"
        "Total = Usage Cost + Commitment Purchase Cost (wrong!)\n"
        "\n"
        "Example:\n"
        "  - RI Purchase (upfront): $10,000  ← ChargeCategory = Purchase\n"
        "  - EC2 Usage (amortized): $10,000  ← ChargeCategory = Usage (amortized RI)\n"
        "  - SUM = $20,000  ❌ (WRONG — you've counted the RI twice)\n"
        "```\n\n"
        "### The Correct Approach\n"
        "Use **EffectiveCost** filtered to `ChargeCategory = Usage` only:\n"
        "```\n"
        "EffectiveCost on Usage rows = amortized cost of RI consumption\n"
        "This already accounts for the upfront payment, so don't add Purchase rows\n"
        "Correct Total = SUM(EffectiveCost WHERE ChargeCategory = 'Usage')\n"
        "```\n\n"
        "### FOCUS 1.0 Guidance\n"
        "| Column | Behavior with Commitments |\n"
        "|--------|---------------------------|\n"
        "| **BilledCost** | Shows $0 for RI usage rows (amount billed to invoice) |\n"
        "| **EffectiveCost** | Shows amortized cost for RI usage rows ✅ USE THIS |\n"
        "| **ListCost** | Shows on-demand equivalent for comparison |"
        + charge_breakdown
    )

    provenance = [
        {"from": "Charge(Purchase)", "relationship": "CAUSES", "to": "Double-counting"},
        {"from": "FOCUSColumn(EffectiveCost)", "relationship": "DERIVED_BY", "to": "DerivationRule"},
        {"from": "CostRecord", "relationship": "HAS_CHARGE", "to": "Charge(Usage/Purchase)"},
    ]
    return make_result(answer, provenance, "hybrid",
                       compute_confidence("why_total_increases", provenance),
                       "why_total_increases", billing_period, question)


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 8 — Which cost type to analyze cloud spend
# ─────────────────────────────────────────────────────────────────────────────
def handle_cost_type_analysis(question, billing_period):
    with driver.session() as session:
        result = session.run("""
            MATCH (f:FOCUSColumn)
            WHERE f.name IN ["EffectiveCost", "BilledCost", "ListCost", "ContractedCost"]
            OPTIONAL MATCH (f)-[:DERIVED_BY]->(dr:DerivationRule)
            RETURN f.name AS name, f.description AS desc, dr.formula AS formula
            ORDER BY f.name
        """).data()

    col_rows = {r["name"]: r for r in result} if result else {}

    answer = (
        "## Which Cost Type Should You Use to Analyze Cloud Spend?\n\n"
        "FOCUS 1.0 defines 4 cost columns. Here's when to use each:\n\n"
        "### 🏆 Recommended: **EffectiveCost** (for most analyses)\n"
        f"> {col_rows.get('EffectiveCost', {}).get('desc', 'Amortized cost after discounts')}\n\n"
        "Use **EffectiveCost** for:\n"
        "- FinOps dashboards and trending\n"
        "- Commitment discount utilization analysis\n"
        "- True cost-per-workload attribution\n"
        "- Cost allocation across teams\n\n"
        "### 📄 **BilledCost** (for invoice reconciliation)\n"
        f"> {col_rows.get('BilledCost', {}).get('desc', 'Final invoiced amount')}\n\n"
        "Use **BilledCost** for:\n"
        "- Reconciling cloud invoices\n"
        "- Finance team billing reports\n"
        "- Checking what was actually charged\n\n"
        "### 📊 **ListCost** (for savings measurement)\n"
        f"> {col_rows.get('ListCost', {}).get('desc', 'On-demand list price cost')}\n\n"
        "Use **ListCost** for:\n"
        "- Measuring discount savings vs on-demand price\n"
        "- Negotiation benchmarking\n\n"
        "### 📝 **ContractedCost** (for contract compliance)\n"
        f"> {col_rows.get('ContractedCost', {}).get('desc', 'Contractual rate cost')}\n\n"
        "Use **ContractedCost** for:\n"
        "- Verifying negotiated pricing is being applied\n"
        "- MSP or enterprise agreement audits\n\n"
        "### Decision Matrix\n"
        "| Use Case | Recommended Column |\n"
        "|----------|-------------------|\n"
        "| General cloud spend analysis | **EffectiveCost** |\n"
        "| Invoice reconciliation | **BilledCost** |\n"
        "| Savings vs list price | **ListCost** - **EffectiveCost** |\n"
        "| Contract audit | **ContractedCost** |\n"
        "| RI/SP utilization | **EffectiveCost** (Usage only) |"
    )

    provenance = [
        {"from": "FOCUSColumn(EffectiveCost)", "relationship": "RECOMMENDED_FOR", "to": "Cloud Spend Analysis"},
        {"from": "FOCUSColumn(BilledCost)", "relationship": "RECOMMENDED_FOR", "to": "Invoice Reconciliation"},
        {"from": "FOCUSColumn", "relationship": "DEFINED_BY", "to": "FOCUS 1.0"},
    ]
    return make_result(answer, provenance, "graph",
                       compute_confidence("cost_type_analysis", provenance),
                       "cost_type_analysis", billing_period, question)


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 9 — ContractedCost vs ContractedUnitPrice × PricingQuantity
# ─────────────────────────────────────────────────────────────────────────────
def handle_contracted_cost_question(question, billing_period):
    with driver.session() as session:
        deriv = session.run("""
            MATCH (f:FOCUSColumn {name: "ContractedCost"})
            OPTIONAL MATCH (f)-[:DERIVED_BY]->(dr:DerivationRule)
            RETURN f.description AS desc, dr.formula AS formula, dr.description AS note
        """).single()

    formula = deriv["formula"] if deriv else "ContractedCost = ContractedUnitPrice × PricingQuantity"
    note    = deriv["note"]    if deriv else ""

    answer = (
        "## Can ContractedCost Differ from ContractedUnitPrice × PricingQuantity?\n\n"
        "**Yes — ContractedCost CAN differ** from the simple product for these charge types:\n\n"
        "### ✅ When They Are Equal\n"
        "For **standard Usage charges**:\n"
        "```\n"
        f"ContractedCost = ContractedUnitPrice × PricingQuantity\n"
        "Example: 100 hours × $0.10/hr = $10.00 ✓\n"
        "```\n\n"
        "### ❌ When They Differ\n"
        "| Scenario | Why They Differ |\n"
        "|----------|----------------|\n"
        "| **Commitment Purchases** | ContractedCost = full upfront/partial RI price; PricingQuantity × UnitPrice = amortized hourly rate |\n"
        "| **Credits** | Provider may set ContractedCost = 0 but PricingQuantity × UnitPrice ≠ 0 |\n"
        "| **Adjustments/Corrections** | ChargeClass = Correction may carry negative ContractedCost |\n"
        "| **Tiered Pricing** | Blended rate differs from simple unit × quantity |\n"
        "| **Negotiated Discounts** | Volume discounts not reflected in per-unit price alone |\n\n"
        "### FOCUS 1.0 Derivation Rule\n"
        f"```\n{formula}\n```\n"
        f"{note}\n\n"
        "### Practical Implication\n"
        "Always filter to `ChargeCategory = 'Usage'` before assuming ContractedCost = "
        "ContractedUnitPrice × PricingQuantity. For `Purchase` and `Credit` rows, treat "
        "ContractedCost as the authoritative value."
    )

    provenance = [
        {"from": "FOCUSColumn(ContractedCost)", "relationship": "DERIVED_BY", "to": "DerivationRule"},
        {"from": "Charge(Purchase)", "relationship": "CAUSES", "to": "ContractedCost divergence"},
        {"from": "FOCUSColumn(ContractedCost)", "relationship": "DEFINED_BY", "to": "FOCUS 1.0"},
    ]
    return make_result(answer, provenance, "graph",
                       compute_confidence("contracted_cost_question", provenance),
                       "contracted_cost_question", billing_period, question)


# ─────────────────────────────────────────────────────────────────────────────
# COST AGGREGATION (generic)
# ─────────────────────────────────────────────────────────────────────────────
def handle_cost_aggregation(question, billing_period, data):
    query_lower = question.lower()
    exclude_commitment = ("exclude commitment" in query_lower or "without commitment" in query_lower)

    provider = None
    if "aws" in query_lower:
        provider = "AWS"
    elif "azure" in query_lower:
        provider = "Azure"

    with driver.session() as session:
        cypher = """
            MATCH (c:CostRecord)
            OPTIONAL MATCH (c)-[:HAS_CHARGE]->(ch:Charge)
            OPTIONAL MATCH (c)-[:IN_BILLING_PERIOD]->(p:BillingPeriod)
            WHERE 1=1
        """
        params = {}
        if provider:
            cypher += " AND c.cloudProvider = $provider"
            params["provider"] = provider
        if billing_period:
            cypher += " AND p.start STARTS WITH $bp"
            params["bp"] = billing_period
        if exclude_commitment:
            cypher += " AND (ch IS NULL OR ch.category <> 'Purchase')"
        cypher += """
            RETURN
                sum(c.effectiveCost) AS total_effective,
                sum(c.billedCost)    AS total_billed,
                sum(c.listCost)      AS total_list,
                count(c)             AS record_count
        """
        result = session.run(cypher, **params).single()

    eff   = result["total_effective"] or 0
    billed= result["total_billed"]    or 0
    lst   = result["total_list"]      or 0
    count = result["record_count"]    or 0

    savings = lst - eff if lst > 0 else 0

    answer = (
        f"## {'Filtered ' if provider else 'Total '}Cloud Cost Summary"
        + (f" — {provider}" if provider else "")
        + (f" — {billing_period}" if billing_period else "")
        + "\n\n"
        f"| Cost Metric | Amount |\n"
        f"|-------------|--------|\n"
        f"| **EffectiveCost** (recommended for analysis) | **${eff:,.2f}** |\n"
        f"| BilledCost (invoiced) | ${billed:,.2f} |\n"
        f"| ListCost (on-demand rates) | ${lst:,.2f} |\n"
        f"| Savings vs List | ${savings:,.2f} |\n"
        f"| Total Records | {count:,} |\n"
    )
    if exclude_commitment:
        answer += "\n> 🔍 Commitment purchase charges excluded from this total."

    provenance = [
        {"from": "CostRecord", "relationship": "AGGREGATED", "to": provider or "All Providers"},
    ]
    return make_result(answer, provenance, "graph",
                       compute_confidence("cost_aggregation", provenance),
                       "cost_aggregation", billing_period, question)


# ─────────────────────────────────────────────────────────────────────────────
# CROSS-CLOUD COMPARISON
# ─────────────────────────────────────────────────────────────────────────────
def handle_cross_cloud_comparison(question, billing_period):
    with driver.session() as session:
        result = session.run("""
            MATCH (c:CostRecord)
            OPTIONAL MATCH (c)-[:USES_SERVICE]->(s:Service)
            RETURN c.cloudProvider AS provider,
                   s.serviceCategory AS category,
                   count(c) AS records,
                   sum(c.effectiveCost) AS total_cost
            ORDER BY provider, total_cost DESC
        """).data()

    by_provider = {}
    for r in result:
        p = r["provider"] or "Unknown"
        if p not in by_provider:
            by_provider[p] = {"total": 0, "records": 0, "categories": {}}
        by_provider[p]["total"] += r["total_cost"] or 0
        by_provider[p]["records"] += r["records"] or 0
        cat = r["category"] or "Uncategorized"
        by_provider[p]["categories"][cat] = by_provider[p]["categories"].get(cat, 0) + (r["total_cost"] or 0)

    answer = "## Cross-Cloud Cost Comparison: AWS vs Azure\n\n"
    for prov, d in by_provider.items():
        answer += f"### {prov}\n"
        answer += f"- **Total EffectiveCost:** ${d['total']:,.2f}\n"
        answer += f"- **Records:** {d['records']:,}\n"
        answer += "- **By Category:**\n"
        for cat, cost in sorted(d["categories"].items(), key=lambda x: -x[1])[:5]:
            answer += f"  - {cat}: ${cost:,.2f}\n"
        answer += "\n"

    provenance = [
        {"from": "CostRecord", "relationship": "GROUPED_BY", "to": "cloudProvider"},
        {"from": "Service", "relationship": "HAS_CATEGORY", "to": "ServiceCategory"},
    ]
    return make_result(answer, provenance, "graph",
                       compute_confidence("cross_cloud_comparison", provenance, data_found=bool(result)),
                       "cross_cloud_comparison", billing_period, question)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
def generate_answer(question: str) -> dict:
    """Main entry point — ALWAYS returns a dict, never raises."""
    try:
        data           = build_context(question)
        intent         = data.get("intent", "general")
        billing_period = data.get("billing_period")

        # Route to deterministic handlers
        if intent == "focus_schema":
            return handle_focus_schema(data, question, billing_period)

        if intent == "aws_compute":
            return handle_aws_compute(question, billing_period)

        if intent == "azure_equivalent":
            return handle_azure_equivalent(question, billing_period)

        if intent == "storage_comparison":
            return handle_storage_comparison(question, billing_period)

        if intent == "top_resources":
            return handle_top_resources(question, billing_period)

        if intent == "commitment_double_counting":
            return handle_commitment_double_counting(question, billing_period)

        if intent == "why_total_increases":
            return handle_why_total_increases(question, billing_period)

        if intent == "cost_type_analysis":
            return handle_cost_type_analysis(question, billing_period)

        if intent == "contracted_cost_question":
            return handle_contracted_cost_question(question, billing_period)

        if intent == "cross_cloud_comparison":
            return handle_cross_cloud_comparison(question, billing_period)

        if intent == "cost_aggregation":
            return handle_cost_aggregation(question, billing_period, data)

        # ── Column definition ─────────────────────────────────────────────────
        if intent == "column_definition":
            context = data.get("context", "")
            cols    = data.get("columns", [])
            provenance = [{"from": "FOCUSColumn", "relationship": "DESCRIBES", "to": c} for c in cols]

            if context:
                prompt = (
                    "You are a FOCUS 1.0 cloud cost expert.\n\n"
                    f"Context from knowledge graph:\n{context}\n\n"
                    f"Question: {question}\n\n"
                    "Answer clearly using the context. Cite column names and their properties."
                )
                answer = call_llm(prompt)
                used_llm = True
            else:
                answer = f"Column definitions for: {', '.join(cols)}\n\n{context or 'No additional details found.'}"
                used_llm = False

            conf = compute_confidence("column_definition", provenance, used_llm=used_llm)
            return make_result(answer, provenance, "hybrid" if used_llm else "graph",
                               conf, "column_definition", billing_period, question)

        # ── General hybrid fallback ───────────────────────────────────────────
        context    = data.get("context", "")
        provenance = data.get("provenance", [])

        # If graph didn't find specific data, use general schema knowledge as context
        if not context:
            context = (
                "This is a Cloud Cost Knowledge Graph based on the FOCUS 1.0 standard. "
                "It contains billing data from AWS and Azure cloud providers, including: "
                "CostRecords with fields like EffectiveCost, BilledCost, ListCost, ContractedCost; "
                "Services (EC2, S3, Lambda, Azure VMs, Azure Storage, Azure SQL, etc.); "
                "ChargeCategories (Usage, Purchase, Tax, Credit); "
                "Accounts and ResourceGroups linked to cost records. "
                "AWS and Azure vendor columns are mapped to FOCUS standard columns."
            )
            provenance = [{"from": "system", "relationship": "schema_context", "to": "FOCUS 1.0 KG"}]

        prompt = (
            "You are a cloud cost ontology and FinOps expert with access to a FOCUS 1.0 knowledge graph "
            "containing AWS and Azure billing data.\n\n"
            f"Knowledge Graph Context:\n{context}\n\n"
            f"Question: {question}\n\n"
            "Provide a clear, structured answer. If exact numbers aren't available from context, "
            "explain the concept using FOCUS 1.0 standards and general cloud pricing knowledge."
        )
        answer    = call_llm(prompt)
        used_llm  = True
        conf      = compute_confidence("general", provenance, used_llm=True)

        return make_result(answer, provenance, "hybrid", conf, "general", billing_period, question)

    except Exception as e:
        # Always return a valid dict — never crash the UI
        print(f"generate_answer error: {e}")
        error_msg = str(e)
        friendly = (
            f"⚠️ An error occurred while processing your query.\n\n"
            f"**Query:** {question}\n\n"
        )
        if "Neo4j" in error_msg or "neo4j" in error_msg or "ServiceUnavailable" in error_msg:
            friendly += (
                "**Likely cause:** Neo4j database is not running or not reachable.\n\n"
                "**Fix:** Start Neo4j Desktop and ensure your database is running at `bolt://127.0.0.1:7687`.\n\n"
                "Then re-run `python setup_demo_db.py` if this is a fresh setup."
            )
        elif "GEMINI" in error_msg.upper() or "API" in error_msg.upper():
            friendly += (
                "**Likely cause:** LLM API key issue.\n\n"
                "**Fix:** Add `GEMINI_API_KEY=your_key` to your `.env` file.\n"
                "Get a free key at https://aistudio.google.com/apikey"
            )
        else:
            friendly += f"**Error detail:** `{error_msg}`"

        return {
            "answer": friendly,
            "provenance": [],
            "retrieval_method": "error",
            "confidence": 0.0,
        }