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
        result = session.run("MATCH (s:Service) RETURN s.name AS name")

        for record in result:
            name = record["name"]

            # Skip None / empty names — prevents TypeError: NoneType is not subscriptable
            if not name or not str(name).strip():
                skipped += 1
                continue

            embedding = get_model().encode(str(name)).tolist()

            session.run("""
                MATCH (s:Service {name: $name})
                SET s.embedding = $embedding
            """, name=name, embedding=embedding)
            embedded += 1

    if skipped:
        print(f"  ⚠️  Skipped {skipped} Service nodes with null/empty name")
    print(f"✅ Service embeddings created ({embedded} nodes embedded)")


if __name__ == "__main__":
    embed_services()
