# graph/embed_services.py

from graph.neo4j_connection import driver

_model = None

def get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model


def embed_services():
    skipped = 0
    embedded = 0

    with driver.session() as session:
        result = session.run("MATCH (s:Service) RETURN s.serviceId AS serviceId, s.name AS name")

        for record in result:
            service_id = record["serviceId"]
            name = record["name"]

            # Skip None / empty names — prevents TypeError: NoneType is not subscriptable
            if not name or not str(name).strip():
                skipped += 1
                continue

            embedding = get_model().encode(str(name)).tolist()

            session.run("""
                MATCH (s:Service {serviceId: $serviceId})
                SET s.embedding = $embedding
            """, serviceId=service_id, embedding=embedding)
            embedded += 1

    if skipped:
        print(f"  ⚠️  Skipped {skipped} Service nodes with null/empty name")
    print(f"✅ Service embeddings created ({embedded} nodes embedded)")


if __name__ == "__main__":
    embed_services()
