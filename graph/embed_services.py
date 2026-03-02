#graph/embed_services.py

from graph.neo4j_connection import driver
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")

def embed_services():

    with driver.session() as session:
        result = session.run("MATCH (s:Service) RETURN s.name")

        for record in result:
            name = record["s.name"]

            embedding = model.encode(name).tolist()

            session.run("""
                MATCH (s:Service {name:$name})
                SET s.embedding = $embedding
            """, name=name, embedding=embedding)

    print("✅ Service embeddings created")


if __name__ == "__main__":
    embed_services()