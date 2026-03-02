# graph/embed_all_nodes.py

from graph.neo4j_connection import driver
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")


def embed_nodes(label, text_fields):
    """
    label: Neo4j node label
    text_fields: list of properties to concatenate for embedding
    """

    print(f"Embedding {label} nodes...")

    with driver.session() as session:
        result = session.run(f"""
            MATCH (n:{label})
            RETURN elementId(n) AS eid, n
        """)

        for record in result:
            node = record["n"]
            eid = record["eid"]

            text_parts = []
            for field in text_fields:
                if field in node and node[field]:
                    text_parts.append(str(node[field]))

            if not text_parts:
                continue

            text = " | ".join(text_parts)

            embedding = model.encode(text).tolist()

            session.run("""
                MATCH (n)
                WHERE elementId(n) = $eid
                SET n.embedding = $embedding
            """, eid=eid, embedding=embedding)

    print(f"✅ {label} embeddings created\n")


def run_embedding_pipeline():

    embed_nodes("FOCUSColumn", ["name", "standard"])
    embed_nodes("AWSColumn", ["name"])
    embed_nodes("AzureColumn", ["name"])
    embed_nodes("Charge", ["category", "description"])
    embed_nodes("CostAllocation", [
        "allocationRuleName",
        "allocationMethod",
        "allocationTargetType",
        "allocationBasis"
    ])


if __name__ == "__main__":
    run_embedding_pipeline()