# graph/embed_all_nodes.py
# Embeds all node types with rich text representations

from graph.neo4j_connection import driver
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")


def embed_nodes(label, text_fields):
    """
    Embed all nodes of a given label using concatenated text fields.
    label: Neo4j node label string
    text_fields: list of property names to concatenate
    """
    print(f"Embedding {label} nodes...")
    count = 0

    with driver.session() as session:
        result = session.run(f"""
            MATCH (n:{label})
            RETURN elementId(n) AS eid, n
        """)

        for record in result:
            node = record["n"]
            eid  = record["eid"]

            text_parts = []
            for field in text_fields:
                val = node.get(field)
                if val is not None and str(val).strip():
                    text_parts.append(str(val))

            if not text_parts:
                continue

            text      = " | ".join(text_parts)
            embedding = model.encode(text).tolist()

            session.run("""
                MATCH (n)
                WHERE elementId(n) = $eid
                SET n.embedding = $embedding
            """, eid=eid, embedding=embedding)
            count += 1

    print(f"  ✅ {label}: {count} nodes embedded\n")
    return count


def run_embedding_pipeline():
    """Embed all non-Service node types (Services are embedded separately)."""

    # FOCUS Columns — rich text with description + category + standard
    embed_nodes("FOCUSColumn", ["name", "description", "category", "standard", "dataType"])

    # AWS/Azure columns — vendor column names
    embed_nodes("AWSColumn",   ["name"])
    embed_nodes("AzureColumn", ["name"])

    # Charge nodes — category + description text
    embed_nodes("Charge", ["category", "description", "frequency"])

    # Cost Allocation — method + target + basis
    embed_nodes("CostAllocation", [
        "allocationRuleName",
        "allocationMethod",
        "allocationTargetType",
        "allocationBasis",
    ])

    # Resource nodes — id + type
    embed_nodes("Resource", ["id", "resourceName", "resourceType"])

    # OntologyClass nodes
    embed_nodes("OntologyClass", ["name", "description"])

    print("✅ Full embedding pipeline complete")


if __name__ == "__main__":
    run_embedding_pipeline()