# graph/focus_schema_loader.py
# Full FOCUS 1.0 ontology — all 20+ columns, class hierarchy, derivation rules

import sqlite3
from graph.neo4j_connection import driver
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")


# ─────────────────────────────────────────────────────────────────────────────
# COMPLETE FOCUS 1.0 COLUMN DEFINITIONS
# ─────────────────────────────────────────────────────────────────────────────
FOCUS_COLUMNS = [
    # Cost Columns
    {
        "name": "BilledCost",
        "description": "The cost billed for the charge after applying all discounts. Represents the final invoiced amount in the billing currency. Must be >= 0.",
        "dataType": "Decimal",
        "nullable": False,
        "validationRule": ">= 0",
        "standard": "FOCUS 1.0",
        "ontologyClass": "CostRecord",
        "category": "Cost"
    },
    {
        "name": "EffectiveCost",
        "description": "The amortized cost of the charge after applying all discounts, including amortization of upfront commitment purchases. Derived from BilledCost adjusted for reserved instance/savings plan amortization.",
        "dataType": "Decimal",
        "nullable": False,
        "validationRule": ">= 0",
        "standard": "FOCUS 1.0",
        "ontologyClass": "CostRecord",
        "category": "Cost"
    },
    {
        "name": "ListCost",
        "description": "The cost calculated by multiplying the ListUnitPrice by the PricingQuantity, representing the on-demand list price before any discounts.",
        "dataType": "Decimal",
        "nullable": True,
        "validationRule": ">= 0",
        "standard": "FOCUS 1.0",
        "ontologyClass": "CostRecord",
        "category": "Cost"
    },
    {
        "name": "ContractedCost",
        "description": "The cost calculated at the negotiated or contracted rate for the charge. ContractedCost = ContractedUnitPrice * PricingQuantity for standard usage charges.",
        "dataType": "Decimal",
        "nullable": True,
        "validationRule": ">= 0",
        "standard": "FOCUS 1.0",
        "ontologyClass": "CostRecord",
        "category": "Cost"
    },
    # Quantity Columns
    {
        "name": "ConsumedQuantity",
        "description": "The volume of a given SKU associated with a resource or service used, based on the ConsumedUnit. Reflects actual consumption.",
        "dataType": "Decimal",
        "nullable": True,
        "validationRule": ">= 0",
        "standard": "FOCUS 1.0",
        "ontologyClass": "CostRecord",
        "category": "Quantity"
    },
    {
        "name": "ConsumedUnit",
        "description": "Provider-specified measurement unit indicating how the ConsumedQuantity is measured (e.g., GB-Hours, vCPU-Hours, Requests).",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "CostRecord",
        "category": "Quantity"
    },
    {
        "name": "PricingQuantity",
        "description": "The quantity used for pricing calculation. This may differ from ConsumedQuantity due to pricing tier or unit transformations.",
        "dataType": "Decimal",
        "nullable": True,
        "validationRule": ">= 0",
        "standard": "FOCUS 1.0",
        "ontologyClass": "CostRecord",
        "category": "Quantity"
    },
    {
        "name": "PricingUnit",
        "description": "Provider-specified measurement unit used for pricing calculations (e.g., GB, Hour). Base unit for ContractedUnitPrice and ListUnitPrice.",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "CostRecord",
        "category": "Quantity"
    },
    # Charge Columns
    {
        "name": "ChargeCategory",
        "description": "Highest-level classification of the nature of a charge. Allowed values: Usage, Purchase, Tax, Credit, Adjustment. Commitment purchases must be ChargeCategory=Purchase.",
        "dataType": "String",
        "nullable": False,
        "validationRule": "IN [Usage, Purchase, Tax, Credit, Adjustment]",
        "standard": "FOCUS 1.0",
        "ontologyClass": "Charge",
        "category": "Charge"
    },
    {
        "name": "ChargeFrequency",
        "description": "Indicates how often a charge occurs. Allowed values: One-Time, Recurring, Usage-Based.",
        "dataType": "String",
        "nullable": False,
        "validationRule": "IN [One-Time, Recurring, Usage-Based]",
        "standard": "FOCUS 1.0",
        "ontologyClass": "Charge",
        "category": "Charge"
    },
    {
        "name": "ChargeDescription",
        "description": "Human-readable description of the charge explaining what the charge is for, including service details and usage context.",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "Charge",
        "category": "Charge"
    },
    {
        "name": "ChargeClass",
        "description": "Indicates whether the row represents a correction to a prior period. Allowed values: Correction, or null for non-corrections.",
        "dataType": "String",
        "nullable": True,
        "validationRule": "IN [Correction, null]",
        "standard": "FOCUS 1.0",
        "ontologyClass": "Charge",
        "category": "Charge"
    },
    # Resource Columns
    {
        "name": "ResourceId",
        "description": "Unique identifier assigned to a resource by the provider (e.g., AWS ARN, Azure Resource ID). Used to trace costs to specific cloud resources.",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "Resource",
        "category": "Resource"
    },
    {
        "name": "ResourceName",
        "description": "Human-readable display name of the resource. May differ from ResourceId. Set by the user or provider.",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "Resource",
        "category": "Resource"
    },
    {
        "name": "ResourceType",
        "description": "The kind of resource the charge applies to (e.g., Virtual Machine, Storage Account, Database). Enables filtering by resource category.",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "Resource",
        "category": "Resource"
    },
    # Service Columns
    {
        "name": "ServiceName",
        "description": "The display name for the type of cloud service, product, or offering for which charges apply (e.g., Amazon EC2, Azure Blob Storage).",
        "dataType": "String",
        "nullable": False,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "Service",
        "category": "Service"
    },
    {
        "name": "ServiceCategory",
        "description": "Highest-level classification of a service based on its core function. Allowed values: AI and Machine Learning, Analytics, Business Applications, Compute, Databases, Developer Tools, Multicloud, Networking, Security, Storage, Web.",
        "dataType": "String",
        "nullable": False,
        "validationRule": "IN [Compute, Storage, Databases, Networking, AI and Machine Learning, Analytics, Security, Developer Tools, Web, Business Applications, Multicloud]",
        "standard": "FOCUS 1.0",
        "ontologyClass": "Service",
        "category": "Service"
    },
    # Account Columns
    {
        "name": "BillingAccountId",
        "description": "Identifier assigned to a billing account by the provider. Used to group subscriptions and link to invoices.",
        "dataType": "String",
        "nullable": False,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "Account",
        "category": "Account"
    },
    {
        "name": "BillingAccountName",
        "description": "Display name assigned to the billing account by the consumer or provider.",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "Account",
        "category": "Account"
    },
    {
        "name": "SubAccountId",
        "description": "ID of a sub-account within the billing account hierarchy (e.g., AWS Linked Account, Azure Subscription).",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "Account",
        "category": "Account"
    },
    {
        "name": "SubAccountName",
        "description": "Display name of the sub-account. Used to identify AWS linked accounts or Azure subscriptions.",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "Account",
        "category": "Account"
    },
    # Location Columns
    {
        "name": "RegionId",
        "description": "Provider-assigned identifier for the geographical region where the resource runs (e.g., us-east-1, eastus).",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "Location",
        "category": "Location"
    },
    {
        "name": "RegionName",
        "description": "Human-readable name of the geographic region where the resource is deployed (e.g., US East (N. Virginia), East US).",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "Location",
        "category": "Location"
    },
    # Time Columns
    {
        "name": "ChargePeriodStart",
        "description": "Start date/time of the charge period for a usage-based charge or the effective start date for a purchase.",
        "dataType": "DateTime",
        "nullable": False,
        "validationRule": "ISO 8601 format",
        "standard": "FOCUS 1.0",
        "ontologyClass": "TimeFrame",
        "category": "Time"
    },
    {
        "name": "ChargePeriodEnd",
        "description": "End date/time (exclusive) of the charge period. ChargePeriodEnd > ChargePeriodStart always.",
        "dataType": "DateTime",
        "nullable": False,
        "validationRule": "> ChargePeriodStart",
        "standard": "FOCUS 1.0",
        "ontologyClass": "TimeFrame",
        "category": "Time"
    },
    {
        "name": "BillingPeriodStart",
        "description": "Start date/time of the billing period in which this charge appears on the invoice.",
        "dataType": "DateTime",
        "nullable": False,
        "validationRule": "ISO 8601 format",
        "standard": "FOCUS 1.0",
        "ontologyClass": "TimeFrame",
        "category": "Time"
    },
    {
        "name": "BillingPeriodEnd",
        "description": "End date/time (exclusive) of the billing period.",
        "dataType": "DateTime",
        "nullable": False,
        "validationRule": "> BillingPeriodStart",
        "standard": "FOCUS 1.0",
        "ontologyClass": "TimeFrame",
        "category": "Time"
    },
    # Currency
    {
        "name": "BillingCurrency",
        "description": "Currency in which the charge is billed. Uses ISO 4217 currency codes (e.g., USD, EUR, GBP).",
        "dataType": "String",
        "nullable": False,
        "validationRule": "ISO 4217",
        "standard": "FOCUS 1.0",
        "ontologyClass": "CostRecord",
        "category": "Cost"
    },
    # Tags
    {
        "name": "Tags",
        "description": "Set of key-value pairs applied to the resource or charge for cost allocation, categorization and organizational purposes. Stored as TagsKV.",
        "dataType": "JSON",
        "nullable": True,
        "validationRule": "Key-value pairs",
        "standard": "FOCUS 1.0",
        "ontologyClass": "CostRecord",
        "category": "Tags"
    },
    # Commitment
    {
        "name": "CommitmentDiscountId",
        "description": "Unique identifier for the commitment discount (Reserved Instance, Savings Plan, Committed Use Discount) applied to the charge.",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "CostRecord",
        "category": "Commitment"
    },
    {
        "name": "CommitmentDiscountType",
        "description": "Type of commitment discount applied (e.g., Reserved Instance, Savings Plan). Determines how EffectiveCost is calculated.",
        "dataType": "String",
        "nullable": True,
        "validationRule": None,
        "standard": "FOCUS 1.0",
        "ontologyClass": "CostRecord",
        "category": "Commitment"
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# DERIVATION RULES
# ─────────────────────────────────────────────────────────────────────────────
DERIVATION_RULES = [
    {
        "targetColumn": "EffectiveCost",
        "formula": "EffectiveCost = BilledCost - CommitmentDiscountSavings + AmortizedUpfrontFees",
        "description": "For commitment-based charges, EffectiveCost amortizes the upfront purchase cost across the commitment period. For Usage charges, EffectiveCost equals BilledCost minus any on-demand discounts.",
        "inputs": ["BilledCost", "CommitmentDiscountId"],
        "focusRef": "FOCUS 1.0 Section 4.2"
    },
    {
        "targetColumn": "ListCost",
        "formula": "ListCost = ListUnitPrice * PricingQuantity",
        "description": "List cost represents the cost at the on-demand public list price, before any negotiated discounts or commitment discounts are applied.",
        "inputs": ["PricingQuantity"],
        "focusRef": "FOCUS 1.0 Section 4.3"
    },
    {
        "targetColumn": "ContractedCost",
        "formula": "ContractedCost = ContractedUnitPrice * PricingQuantity",
        "description": "For standard Usage charges, ContractedCost equals ContractedUnitPrice multiplied by PricingQuantity. For commitment purchases and adjustments, ContractedCost may not equal this product.",
        "inputs": ["PricingQuantity"],
        "focusRef": "FOCUS 1.0 Section 4.4"
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# ONTOLOGY CLASSES (Full Hierarchy)
# ─────────────────────────────────────────────────────────────────────────────
ONTOLOGY_CLASSES = [
    # Root
    {"name": "CloudCostEntity",      "parent": None,
     "description": "Root ontology class for all cloud cost entities per FOCUS 1.0"},
    # Core Classes
    {"name": "CostRecord",           "parent": "CloudCostEntity",
     "description": "Primary billing record representing a single charge line item"},
    {"name": "Account",              "parent": "CloudCostEntity",
     "description": "Billing or sub-account entity grouping resources and charges"},
    {"name": "Service",              "parent": "CloudCostEntity",
     "description": "Cloud service or product generating charges"},
    {"name": "Resource",             "parent": "CloudCostEntity",
     "description": "Individual cloud resource incurring charges (e.g., VM, Bucket)"},
    {"name": "Location",             "parent": "CloudCostEntity",
     "description": "Geographic region where a resource is deployed"},
    {"name": "TimeFrame",            "parent": "CloudCostEntity",
     "description": "Temporal period covering charge and billing periods"},
    {"name": "Charge",               "parent": "CloudCostEntity",
     "description": "Classification of the type and nature of a billing charge"},
    {"name": "Tag",                  "parent": "CloudCostEntity",
     "description": "Key-value metadata applied to resources for allocation and governance"},
    {"name": "CostAllocation",       "parent": "CloudCostEntity",
     "description": "Rule-based allocation of shared costs across applications or cost centres"},
    {"name": "CostCentre",           "parent": "CloudCostEntity",
     "description": "Organizational unit or application receiving allocated costs"},
    # Vendor Hierarchy
    {"name": "VendorSpecificAttributes", "parent": "CloudCostEntity",
     "description": "Vendor-specific extended fields beyond FOCUS standard (x_* prefix)"},
    {"name": "AWSVendorAttributes",  "parent": "VendorSpecificAttributes",
     "description": "AWS-specific fields: x_ServiceCode, x_UsageType, x_Operation"},
    {"name": "AzureVendorAttributes","parent": "VendorSpecificAttributes",
     "description": "Azure-specific fields: x_SkuMeterCategory, x_SkuDescription, x_CostCenter"},
    # Standards
    {"name": "FOCUSStandard",        "parent": "CloudCostEntity",
     "description": "FinOps Open Cost and Usage Specification standard governing column definitions"},
]


# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOADER
# ─────────────────────────────────────────────────────────────────────────────
def load_focus_schema():
    print("🔹 Connecting to SQLite...")
    try:
        conn = sqlite3.connect("billing.db")
        cursor = conn.cursor()
        aws_columns = cursor.execute("PRAGMA table_info(aws_billing)").fetchall()
        azure_columns = cursor.execute("PRAGMA table_info(azure_billing)").fetchall()
    except Exception as e:
        print(f"❌ SQLite error: {e}")
        aws_columns = []
        azure_columns = []

    print("🔹 Connecting to Neo4j...")
    with driver.session(database="neo4j") as session:

        # ── 1. Ontology Class Hierarchy ─────────────────────────────────────
        print("🔹 Creating ontology class hierarchy...")
        for cls in ONTOLOGY_CLASSES:
            session.run("""
                MERGE (c:OntologyClass {name: $name})
                SET c.description = $description
            """, name=cls["name"], description=cls["description"])

            if cls["parent"]:
                session.run("""
                    MATCH (child:OntologyClass {name: $child})
                    MATCH (parent:OntologyClass {name: $parent})
                    MERGE (child)-[:SUBCLASS_OF]->(parent)
                """, child=cls["name"], parent=cls["parent"])

        # ── 2. FOCUS Standard Node ──────────────────────────────────────────
        print("🔹 Creating FOCUS standard node...")
        session.run("""
            MERGE (std:Standard {name: "FOCUS 1.0"})
            SET std.description = "FinOps Open Cost and Usage Specification v1.0",
                std.url = "https://focus.finops.org/focus-specification/v10/",
                std.publisher = "FinOps Foundation"
        """)

        # ── 3. FOCUS Columns ────────────────────────────────────────────────
        print("🔹 Creating FOCUS columns with full metadata...")
        focus_names = [col["name"] for col in FOCUS_COLUMNS]

        for col in FOCUS_COLUMNS:
            text_for_embed = (
                f"{col['name']}: {col['description']} "
                f"Category: {col['category']}. "
                f"DataType: {col['dataType']}. Standard: {col['standard']}"
            )
            embedding = model.encode(text_for_embed).tolist()

            session.run("""
                MERGE (f:FOCUSColumn {name: $name})
                SET f.standard       = $standard,
                    f.description    = $description,
                    f.dataType       = $dataType,
                    f.nullable       = $nullable,
                    f.validationRule = $rule,
                    f.ontologyClass  = $ontologyClass,
                    f.category       = $category,
                    f.embedding      = $embedding
            """,
                name=col["name"],
                standard=col["standard"],
                description=col["description"],
                dataType=col["dataType"],
                nullable=col.get("nullable"),
                rule=col.get("validationRule"),
                ontologyClass=col.get("ontologyClass"),
                category=col.get("category"),
                embedding=embedding
            )

            # Link to Standard node
            session.run("""
                MATCH (f:FOCUSColumn {name: $name})
                MATCH (std:Standard {name: "FOCUS 1.0"})
                MERGE (f)-[:DEFINED_BY]->(std)
            """, name=col["name"])

            # Link to OntologyClass
            if col.get("ontologyClass"):
                session.run("""
                    MATCH (f:FOCUSColumn {name: $col})
                    MATCH (c:OntologyClass {name: $cls})
                    MERGE (f)-[:BELONGS_TO_CLASS]->(c)
                """, col=col["name"], cls=col["ontologyClass"])

        # ── 4. Derivation Rules ─────────────────────────────────────────────
        print("🔹 Creating derivation rules...")
        for rule in DERIVATION_RULES:
            session.run("""
                MERGE (r:DerivationRule {targetColumn: $target})
                SET r.formula      = $formula,
                    r.description  = $description,
                    r.inputs       = $inputs,
                    r.focusRef     = $focusRef
            """,
                target=rule["targetColumn"],
                formula=rule["formula"],
                description=rule["description"],
                inputs=rule["inputs"],
                focusRef=rule["focusRef"]
            )

            session.run("""
                MATCH (f:FOCUSColumn {name: $name})
                MATCH (r:DerivationRule {targetColumn: $name})
                MERGE (f)-[:DERIVED_BY]->(r)
            """, name=rule["targetColumn"])

        # ── 5. AWS Column Mapping ───────────────────────────────────────────
        print("🔹 Creating AWS column mappings...")
        focus_names_lower = {n.lower(): n for n in focus_names}

        for col in aws_columns:
            col_name = col[1]

            text = f"AWS billing column {col_name}"
            embedding = model.encode(text).tolist()

            session.run("""
                MERGE (a:AWSColumn {name: $name})
                SET  a.embedding = $embedding
                WITH a
                MATCH (cls:OntologyClass {name: "AWSVendorAttributes"})
                MERGE (a)-[:INSTANCE_OF]->(cls)
            """, name=col_name, embedding=embedding)

            # Map to FOCUS column if matching
            focus_match = focus_names_lower.get(col_name.lower())
            if focus_match:
                rel_text = (
                    f"AWS column {col_name} maps directly to "
                    f"FOCUS standard column {focus_match}"
                )
                rel_embedding = model.encode(rel_text).tolist()

                session.run("""
                    MATCH (a:AWSColumn {name: $aws})
                    MATCH (f:FOCUSColumn {name: $focus})
                    MERGE (a)-[r:MAPS_TO]->(f)
                    SET r.transformationType = "Direct",
                        r.embedding          = $embedding
                """,
                    aws=col_name,
                    focus=focus_match,
                    embedding=rel_embedding
                )

        # ── 6. Azure Column Mapping ─────────────────────────────────────────
        print("🔹 Creating Azure column mappings...")
        for col in azure_columns:
            col_name = col[1]

            text = f"Azure billing column {col_name}"
            embedding = model.encode(text).tolist()

            session.run("""
                MERGE (z:AzureColumn {name: $name})
                SET  z.embedding = $embedding
                WITH z
                MATCH (cls:OntologyClass {name: "AzureVendorAttributes"})
                MERGE (z)-[:INSTANCE_OF]->(cls)
            """, name=col_name, embedding=embedding)

            focus_match = focus_names_lower.get(col_name.lower())
            if focus_match:
                rel_text = (
                    f"Azure column {col_name} normalized to "
                    f"FOCUS column {focus_match}"
                )
                rel_embedding = model.encode(rel_text).tolist()

                session.run("""
                    MATCH (z:AzureColumn {name: $azure})
                    MATCH (f:FOCUSColumn {name: $focus})
                    MERGE (z)-[r:MAPS_TO]->(f)
                    SET r.transformationType = "Normalized",
                        r.embedding          = $embedding
                """,
                    azure=col_name,
                    focus=focus_match,
                    embedding=rel_embedding
                )

    if aws_columns or azure_columns:
        conn.close()
    print("✅ Full FOCUS 1.0 ontology schema created successfully")


if __name__ == "__main__":
    load_focus_schema()