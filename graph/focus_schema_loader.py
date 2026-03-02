# graph/focus_schema_loader.py

import sqlite3
from graph.neo4j_connection import driver
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")


def load_focus_schema():

    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    aws_columns = cursor.execute("PRAGMA table_info(aws_billing)").fetchall()
    azure_columns = cursor.execute("PRAGMA table_info(azure_billing)").fetchall()

    # -------------------------------------------------
    # Full FOCUS Core Column Set
    # -------------------------------------------------
    focus_core_columns = [
        "BilledCost",
        "EffectiveCost",
        "ContractedCost",
        "ConsumedQuantity",
        "PricingQuantity",
        "ChargeCategory",
        "ChargeFrequency",
        "ChargeDescription",
        "ResourceId",
        "ServiceName",
        "RegionName",
        "BillingAccountId"
    ]

    # -------------------------------------------------
    # FOCUS Metadata (Authoritative Layer)
    # -------------------------------------------------
    focus_metadata = {
        "BilledCost": {
            "description": "Final invoiced amount for the billing period.",
            "dataType": "Decimal",
            "nullable": False,
            "validationRule": ">= 0"
        },
        "EffectiveCost": {
            "description": "Actual cost after discounts and adjustments.",
            "dataType": "Decimal",
            "nullable": False,
            "validationRule": ">= 0"
        },
        "ContractedCost": {
            "description": "Contractual agreed pricing amount.",
            "dataType": "Decimal",
            "nullable": True,
            "validationRule": ">= 0"
        },
        "ConsumedQuantity": {
            "description": "Quantity of usage consumed.",
            "dataType": "Float",
            "nullable": False,
            "validationRule": ">= 0"
        },
        "PricingQuantity": {
            "description": "Quantity used for pricing calculation.",
            "dataType": "Float",
            "nullable": True,
            "validationRule": ">= 0"
        },
        "ChargeCategory": {
            "description": "Category of charge (Usage, Commitment, Tax, etc.)",
            "dataType": "String",
            "nullable": False
        }
    }

    with driver.session() as session:

        print("Creating FOCUS Columns...")

        # -------------------------------------------------
        # Create ALL FOCUS Core Columns
        # -------------------------------------------------
        for col_name in focus_core_columns:

            meta = focus_metadata.get(col_name, {})

            session.run("""
                MERGE (f:FOCUSColumn {name:$name})
                SET f.standard = "FOCUS 1.0",
                    f.description = $desc,
                    f.dataType = $dtype,
                    f.nullable = $nullable,
                    f.validationRule = $rule
            """,
                name=col_name,
                desc=meta.get("description"),
                dtype=meta.get("dataType"),
                nullable=meta.get("nullable"),
                rule=meta.get("validationRule")
            )

        # -------------------------------------------------
        # Derivation Rule Modeling
        # -------------------------------------------------
        session.run("""
            MERGE (r:DerivationRule {
                formula:"EffectiveCost = BilledCost + AmortizedCost"
            })
            SET r.description = "Effective cost derived per FOCUS 1.0 specification"
        """)

        session.run("""
            MATCH (f:FOCUSColumn {name:"EffectiveCost"})
            MATCH (r:DerivationRule {
                formula:"EffectiveCost = BilledCost + AmortizedCost"
            })
            MERGE (f)-[:DERIVED_BY]->(r)
        """)

        # -------------------------------------------------
        # AWS Column Mapping + Relationship Embeddings
        # -------------------------------------------------
        print("Creating AWS column mappings...")

        for col in aws_columns:
            col_name = col[1]

            session.run("""
                MERGE (a:AWSColumn {name:$name})
            """, name=col_name)

            if col_name in focus_core_columns:

                relationship_text = (
                    f"AWS column {col_name} maps directly to "
                    f"FOCUS standard column {col_name}"
                )
                embedding = model.encode(relationship_text).tolist()

                session.run("""
                    MATCH (a:AWSColumn {name:$name})
                    MATCH (f:FOCUSColumn {name:$name})
                    MERGE (a)-[r:MAPS_TO]->(f)
                    SET r.transformationType = "Direct",
                        r.embedding = $embedding
                """,
                    name=col_name,
                    embedding=embedding
                )

        # -------------------------------------------------
        # Azure Column Mapping (Case-Insensitive)
        # -------------------------------------------------
        print("Creating Azure column mappings...")

        for col in azure_columns:
            col_name = col[1]

            session.run("""
                MERGE (z:AzureColumn {name:$name})
            """, name=col_name)

            for focus_col in focus_core_columns:

                if col_name.lower() == focus_col.lower():

                    relationship_text = (
                        f"Azure column {col_name} normalized to "
                        f"FOCUS column {focus_col}"
                    )
                    embedding = model.encode(relationship_text).tolist()

                    session.run("""
                        MATCH (z:AzureColumn {name:$azure})
                        MATCH (f:FOCUSColumn {name:$focus})
                        MERGE (z)-[r:MAPS_TO]->(f)
                        SET r.transformationType = "Normalized",
                            r.embedding = $embedding
                    """,
                        azure=col_name,
                        focus=focus_col,
                        embedding=embedding
                    )

    conn.close()
    print("✅ FOCUS schema mapping created successfully")


if __name__ == "__main__":
    load_focus_schema()