# ☁️ Cloud Cost Knowledge Graph

### Ontology-Driven Cloud Billing Intelligence | FOCUS 1.0 | Neo4j | Hybrid RAG | Gemini LLM

---

## 📌 Executive Summary

A **production-grade Cloud Cost Intelligence system** aligned with the **FOCUS (FinOps Open Cost & Usage Specification) 1.0 standard**, built for AWS + Azure billing datasets.

**What makes this different from LLM-only systems:**
- All financial calculations are **deterministically executed inside the Neo4j graph** via Cypher
- LLM (Gemini 2.0) is used **only for natural language explanation and open-ended analysis** — never for cost math
- Every answer carries **provenance paths** showing exactly which graph nodes produced the result
- Full **ontology class hierarchy** with 14 classes, 30+ FOCUS columns, and 3 derivation rules
- **Any question** about cloud costs, billing concepts, or FinOps is answered — not just predefined ones

---

## 🏗 System Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    DATA INGESTION LAYER                          │
│  AWS XLS  ──► SQLite DB ◄──  Azure XLS                          │
└──────────────────────┬───────────────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────────────┐
│                   GRAPH CONSTRUCTION LAYER                       │
│                                                                  │
│  FOCUS Schema Loader ──► OntologyClass Hierarchy                 │
│  Metadata Loader     ──► Service / Account / Resource / Location │
│  Cost Record Loader  ──► CostRecord + all relationships          │
│  Allocation Loader   ──► CostAllocation → CostCentre            │
│                                                                  │
│              ┌──────────────────────────┐                        │
│              │     Neo4j Graph DB       │                        │
│              │  8,500+ Nodes            │                        │
│              │  43,900+ Relationships   │                        │
│              │  6 Vector Indexes        │                        │
│              │  Full-text Indexes       │                        │
│              └──────────────────────────┘                        │
└──────────────────────┬───────────────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────────────┐
│                   EMBEDDING LAYER (all-MiniLM-L6-v2)            │
│  FOCUSColumn · Service · AWSColumn · AzureColumn                 │
│  Charge · CostAllocation · Resource — all embedded               │
└──────────────────────┬───────────────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────────────┐
│                   HYBRID RETRIEVAL LAYER                         │
│  Vector Search (cosine similarity on embeddings)                 │
│    +                                                             │
│  Graph Traversal (multi-hop Cypher: CostRecord→Service→Category) │
│    +                                                             │
│  Intent Detection (11 structured + unlimited general queries)    │
└──────────────────────┬───────────────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────────────┐
│                   LLM GENERATION LAYER                           │
│  Gemini 2.0 Flash Lite → Gemini 2.5 Flash → Gemini 2.0 Flash    │
│  → OpenAI GPT-4o-mini → Ollama (fallback)                       │
│  Direct REST API calls (no SDK deadlocks)                        │
│  Deterministic graph answers bypass LLM entirely                 │
└──────────────────────┬───────────────────────────────────────────┘
                       │
             ┌─────────┴─────────┐
             │                   │
   ┌─────────▼──────┐  ┌────────▼────────┐
   │  Streamlit UI  │  │  FastAPI REST   │
   │  (app.py)      │  │  (api.py)       │
   └────────────────┘  └─────────────────┘
```

---

## 🧠 Ontology Design (Part A)

### Class Hierarchy (14 Classes)

```
CloudCostEntity (root)
├── CostRecord          — billing line item
├── Account             — billing + sub-account
├── Service             — cloud service/product
├── Resource            — individual cloud resource
├── Location            — geographic region
├── TimeFrame           — charge/billing periods
├── Charge              — charge category/type
├── Tag                 — key-value cost metadata
├── CostAllocation      — shared cost allocation rule
├── CostCentre          — allocation target entity
├── FOCUSStandard       — FOCUS spec reference
└── VendorSpecificAttributes
    ├── AWSVendorAttributes   (x_ServiceCode, x_UsageType)
    └── AzureVendorAttributes (x_SkuMeterCategory, x_SkuDescription)
```

### FOCUS 1.0 Columns Modeled (31 columns)

| Category | Columns |
|----------|---------|
| **Cost** | BilledCost, EffectiveCost, ListCost, ContractedCost, BillingCurrency |
| **Quantity** | ConsumedQuantity, ConsumedUnit, PricingQuantity, PricingUnit |
| **Charge** | ChargeCategory, ChargeFrequency, ChargeDescription, ChargeClass |
| **Resource** | ResourceId, ResourceName, ResourceType |
| **Service** | ServiceName, ServiceCategory |
| **Account** | BillingAccountId, BillingAccountName, SubAccountId, SubAccountName |
| **Location** | RegionId, RegionName |
| **Time** | ChargePeriodStart, ChargePeriodEnd, BillingPeriodStart, BillingPeriodEnd |
| **Tags** | Tags (TagsKV) |
| **Commitment** | CommitmentDiscountId, CommitmentDiscountType |

### Derivation Rules

| Target Column | Formula | FOCUS Ref |
|--------------|---------|-----------|
| **EffectiveCost** | `BilledCost - CommitmentDiscountSavings + AmortizedUpfrontFees` | §4.2 |
| **ListCost** | `ListUnitPrice × PricingQuantity` | §4.3 |
| **ContractedCost** | `ContractedUnitPrice × PricingQuantity` (Usage only) | §4.4 |

### Validation Rules

| Column | Rule |
|--------|------|
| BilledCost | `>= 0`, NOT NULL |
| EffectiveCost | `>= 0`, NOT NULL |
| ChargeCategory | `IN [Usage, Purchase, Tax, Credit, Adjustment]` |
| ChargeFrequency | `IN [One-Time, Recurring, Usage-Based]` |
| BillingCurrency | ISO 4217 |
| ChargePeriodEnd | `> ChargePeriodStart` |

---

## 🕸 Graph Schema Design (Part B)

### Node Types

| Label | Key Properties | Constraint |
|-------|----------------|------------|
| `CostRecord` | id, billedCost, effectiveCost, listCost, cloudProvider | UNIQUE id |
| `Service` | name, cloudProvider, serviceCategory | UNIQUE name |
| `Resource` | id, resourceName, resourceType, cloudProvider | UNIQUE id |
| `Account` | id, billingAccountId, billingAccountName, subAccountId | UNIQUE id |
| `Location` | regionId, regionName | UNIQUE regionId |
| `BillingPeriod` | start, end | UNIQUE start |
| `Charge` | category, description, frequency, chargeClass | — |
| `Tag` | key, value | — |
| `CostAllocation` | allocationRuleName, allocationMethod, allocationBasis | — |
| `CostCentre` | name | UNIQUE name |
| `FOCUSColumn` | name, description, dataType, nullable, validationRule | UNIQUE name |
| `AWSColumn` | name, embedding | UNIQUE name |
| `AzureColumn` | name, embedding | UNIQUE name |
| `OntologyClass` | name, description | — |
| `DerivationRule` | targetColumn, formula, description | — |
| `Standard` | name, description, url | — |

### Relationship Types (12 types)

| Relationship | Source → Target | Description |
|-------------|----------------|-------------|
| `BELONGS_TO_BILLING_ACCOUNT` | CostRecord → Account | Links charge to billing account |
| `BELONGS_TO_SUBACCOUNT` | CostRecord → Account | Links charge to sub-account |
| `IN_BILLING_PERIOD` | CostRecord → BillingPeriod | Temporal period of charge |
| `HAS_CHARGE` | CostRecord → Charge | Charge type classification |
| `INCURRED_BY` | CostRecord → Resource | Resource that generated cost |
| `USES_SERVICE` | CostRecord → Service | Service consumed |
| `HAS_TAG` | CostRecord → Tag | Cost allocation tags |
| `HAS_VENDOR_ATTRS` | CostRecord → VendorSpecificAttributes | x_* fields |
| `ALLOCATED_VIA` | CostRecord → CostAllocation | Shared cost allocation |
| `ALLOCATED_TO` | CostAllocation → CostCentre | Allocation target |
| `DEPLOYED_IN` | Resource → Location | Resource geography |
| `OWNED_BY` | Resource → Account | Resource account ownership |
| `BELONGS_TO` | Resource → Service | Resource to service link |
| `EQUIVALENT_TO` | Service → Service | Cross-cloud service equivalence |
| `MAPS_TO` | AWSColumn/AzureColumn → FOCUSColumn | Vendor→FOCUS normalization (with embedding) |
| `DERIVED_BY` | FOCUSColumn → DerivationRule | Cost derivation formula |
| `SUBCLASS_OF` | OntologyClass → OntologyClass | Ontology hierarchy |
| `DEFINED_BY` | FOCUSColumn → Standard | FOCUS spec reference |

---

## 🔎 RAG Pipeline Architecture (Part D)

### Query Processing Flow

```
User Query (any question — no restrictions)
    │
    ▼
Intent Detection ──► 11 structured intents + unlimited general queries
    │
    ├──► Deterministic Cypher handlers (cost math, aggregations)
    │         → Returns exact numbers, no LLM needed
    │
    └──► Hybrid Search:
              Vector Search (cosine similarity on 3+ indexes)
                    +
              Multi-hop Graph Traversal
              (CostRecord→Service→Category→Equivalents)
                    │
                    ▼
              Context Assembly (deduplicate + rank + provenance)
                    │
                    ▼
              LLM Generation (Gemini 2.0 → fallback chain)
              Works even with empty graph context —
              uses FOCUS 1.0 schema as base knowledge
                    │
                    ▼
              Confidence Scoring + Provenance Paths
```

### Intent Handlers

| Intent | Retrieval | Description |
|--------|-----------|-------------|
| `focus_schema` | Graph | FOCUS columns vs vendor columns |
| `aws_compute` | Graph | All AWS Compute services |
| `azure_equivalent` | Hybrid | Azure equivalent of AWS service |
| `storage_comparison` | Graph | Cross-cloud storage cost comparison |
| `top_resources` | Graph | Top N expensive tagged resources |
| `commitment_double_counting` | Graph | Commitment utilization filtering |
| `why_total_increases` | Hybrid | Commitment purchase effect on totals |
| `cost_type_analysis` | Graph | Which cost column to use |
| `contracted_cost_question` | Hybrid | ContractedCost derivation edge cases |
| `cross_cloud_comparison` | Graph | Provider cost breakdown |
| `cost_aggregation` | Graph | Total cost with optional filters |
| `column_definition` | Hybrid | FOCUS column definitions |
| `general` | Hybrid + Gemini | **Any other question** — answered via LLM |

---

## 🧬 Vector Store Design (Part C)

### Embedding Strategy

- **Model:** `all-MiniLM-L6-v2` (384 dimensions, cosine similarity)
- **Storage:** Persisted as `embedding` property on Neo4j nodes
- **Indexes:** 6 Neo4j vector indexes
- **Loading:** Lazy-loaded on first query (avoids startup deadlocks)

| Index | Node Type | Text Used for Embedding |
|-------|-----------|------------------------|
| `service_embedding_index` | Service | name + serviceCategory |
| `focus_embedding_index` | FOCUSColumn | name + description + category + standard |
| `aws_embedding_index` | AWSColumn | "AWS billing column {name}" |
| `azure_embedding_index` | AzureColumn | "Azure billing column {name}" |
| `charge_embedding_index` | Charge | category + description |
| `allocation_embedding_index` | CostAllocation | method + target + basis |
| `resource_embedding_index` | Resource | id + type |

**Relationship Embeddings:**
`MAPS_TO` relationships also carry an `embedding` property for semantic retrieval of vendor↔FOCUS mappings.

---

## 🚀 Setup & Installation

### Prerequisites
- Python 3.11+
- Neo4j Desktop or Neo4j Community Edition (running at `bolt://127.0.0.1:7687`)
- `data/` folder with `aws_test-focus-00001.snappy_transformed.xls` and `focusazure_anon_transformed.xls`

### 1. Create Virtual Environment

```bash
python -m venv venv
venv\Scripts\activate        # Windows
# or: source venv/bin/activate  # macOS/Linux
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment

Create `.env` (or copy `.env.example`):

```env
NEO4J_URI=neo4j://127.0.0.1:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_neo4j_password

# LLM Keys (Gemini is free — strongly recommended)
GEMINI_API_KEY=your_gemini_api_key
OPENAI_API_KEY=                     # Optional fallback
```

> **Get Gemini API key (free):** https://aistudio.google.com/apikey

### 4. Start Neo4j

Ensure Neo4j is running at `bolt://127.0.0.1:7687`

### 5. Run Full Setup (one command)

```bash
python setup_demo_db.py
```

This runs all 9 steps:
1. Load XLS data into SQLite
2. Create Neo4j constraints + indexes
3. Load FOCUS 1.0 ontology (31 columns, 14 classes, 3 derivation rules)
4. Load Services (with ServiceCategory), Accounts, Resources, Locations
5. Load 1000+ CostRecord nodes with all graph relationships
6. Load Cost Allocation nodes
7. Create AWS↔Azure service equivalence relationships
8. Embed all Service nodes
9. Embed all FOCUSColumn, Charge, Allocation, Resource nodes

### 6. Launch Streamlit UI

```bash
streamlit run app.py
```

Visit http://localhost:8501

### 7. Launch FastAPI (Optional)

```bash
uvicorn api:app --reload
```

Visit http://127.0.0.1:8000/docs for interactive API docs.

---

## 💬 Sample Questions You Can Ask

### From Your Data (Exact Graph Answers)
- *"Find all AWS compute services"*
- *"Show total AWS cost vs Azure cost breakdown by service category"*
- *"Compare storage costs between AWS and Azure"*
- *"Find the top 5 most expensive resources tagged as Production in Azure"*
- *"What is the Azure equivalent of AWS S3?"*

### Open-Ended Analysis (Gemini-Powered)
- *"Which cloud provider has better pricing for database services?"*
- *"How can I reduce my cloud spend based on this data?"*
- *"What are the main cost drivers in my AWS billing?"*
- *"Explain the difference between EffectiveCost and BilledCost"*
- *"What is a savings plan and how does it affect my costs?"*

---

## 🌐 REST API Reference (Part F)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | `{"status": "ok"}` |
| `/query` | POST | `{"question": "..."}` → `{"answer", "concepts", "paths", "confidence"}` |
| `/concept/{name}` | GET | FOCUSColumn / Service / OntologyClass details + similarity scores |
| `/stats` | GET | Total nodes, relationships, label breakdown, index status |

---

## 🧪 Testing (Part E)

All 11 assignment queries are handled by dedicated deterministic handlers. Run via the Streamlit UI sidebar presets or directly via Python:

```bash
python run_evaluations.py
```

Results are saved to `evaluation_log.json`.

| # | Query | Handler |
|---|-------|---------|
| 1 | Core FOCUS columns vs vendor-specific | `focus_schema` |
| 2 | Find all AWS compute services | `aws_compute` |
| 3 | Azure equivalent of AWS S3 | `azure_equivalent` |
| 4 | Compare storage costs AWS vs Azure | `storage_comparison` |
| 5 | Top 5 Production resources in Azure | `top_resources` |
| 6 | Commitment utilization exclusions | `commitment_double_counting` |
| 7 | Why total increases with commitment purchases | `why_total_increases` |
| 8 | Which cost type to analyze cloud spend | `cost_type_analysis` |
| 9 | ContractedCost vs ContractedUnitPrice × PricingQuantity | `contracted_cost_question` |
| 10 | EffectiveCost derivation formula and when it differs from BilledCost | `column_definition` |
| 11 | AWS vs Azure total cost breakdown by service category | `cross_cloud_comparison` |

---

## 🛠 Troubleshooting

| Issue | Solution |
|-------|----------|
| `NEO4J_PASSWORD not set` | Add password to `.env` file |
| `Failed to connect to Neo4j` | Ensure Neo4j is running at `bolt://127.0.0.1:7687` — the app now shows a soft warning instead of crashing |
| `No data found` | Re-run `python setup_demo_db.py` |
| `FileNotFoundError` for XLS | XLS files are committed in `data/`. Run `git pull` then `python setup_demo_db.py`. The loader auto-searches `data/`, `db/`, and project root |
| LLM unavailable / No LLM error | Add `GEMINI_API_KEY` to `.env` (free at aistudio.google.com) |
| Gemini 429 Rate Limited | Wait 1 minute — free tier resets per minute (15 RPM). The system auto-retries once before falling back |
| `pyvis` not found | `pip install pyvis` for graph visualization |
| CUDA error | Sentence transformers auto-falls back to CPU |
| Slow first query | Embedding model loads lazily on first use — subsequent queries are fast |

---

## 📁 Project Structure

```
cloud-cost-knowledge-graph/
├── app.py                      # Streamlit UI (premium dark-mode)
├── api.py                      # FastAPI REST API (Part F bonus)
├── setup_demo_db.py            # One-command full pipeline setup
├── run_evaluations.py          # Automated 11-query evaluation runner (all 11 queries)
├── requirements.txt            # All dependencies
├── .env                        # NEO4J + LLM credentials (not committed)
├── .env.example                # Template for .env
├── billing.db                  # SQLite (auto-generated, not committed)
├── evaluation_log.json         # Query evaluation log (auto-generated)
│
├── data/                       # Raw billing XLS files (committed to repo)
│   ├── aws_test-focus-00001.snappy_transformed.xls
│   └── focusazure_anon_transformed.xls
│
├── graph/                      # Graph construction layer
│   ├── neo4j_connection.py     # Driver setup (graceful failure if Neo4j offline)
│   ├── schema.py               # Constraints + full-text + vector indexes
│   ├── focus_schema_loader.py  # FOCUS 1.0 ontology (31 cols, 14 classes)
│   ├── metadata_loader.py      # Services + Accounts + Resources + Locations
│   ├── cost_record_loader.py   # CostRecord nodes + all relationships
│   ├── cost_allocation_loader.py # CostAllocation + CostCentre nodes
│   ├── service_mapping.py      # AWS ↔ Azure equivalence relationships
│   ├── embed_services.py       # Service node embeddings
│   └── embed_all_nodes.py      # All-node embedding pipeline (lazy model load)
│
├── rag/                        # RAG pipeline layer
│   ├── context_builder.py      # Intent detection + hybrid context assembly
│   └── llm_pipeline.py         # Gemini 2.0→OpenAI→Ollama chain + 11 handlers
│
├── retrieval/                  # Retrieval utilities
│   ├── hybrid_engine.py        # Cross-cloud vector + graph retrieval (lazy model)
│   └── semantic_search.py      # Pure vector search utility (lazy model)
│
└── db/                         # SQLite utilities
    └── init_sqlite.py          # XLS → SQLite loader (auto-discovers files in data/, db/, root)
```

---

## 📊 Evaluation Criteria Alignment

| Criterion | Weight | Implementation |
|-----------|--------|----------------|
| Ontology Design | 20% | 14-class hierarchy, 31 FOCUS columns, 3 derivation rules, validation rules, cardinality via constraints |
| Knowledge Graph | 30% | 18 relationship types, 16 node types, full-text + vector + standard indexes, 8,500+ nodes, 43,900+ relationships |
| Vector Embeddings | 15% | 7 vector indexes, all nodes embedded, relationship-level embeddings on MAPS_TO, lazy loading for performance |
| RAG Pipeline | 20% | 11 structured queries + unlimited general queries via Gemini, multi-hop traversal, provenance paths |
| Testing (11 queries) | 15% | All 11 assignment queries have dedicated handlers + automated evaluation via `run_evaluations.py` |
| Bonus (API + UI) | 20% | FastAPI 4 endpoints + premium Streamlit with pyvis graph visualization + query history |
