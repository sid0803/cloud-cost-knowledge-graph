#retrieval/semantic_search.py

from graph.neo4j_connection import driver
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")


def hybrid_search(query, top_k=3):

    query_embedding = model.encode(query).tolist()

    with driver.session() as session:
        result = session.run("""
            CALL db.index.vector.queryNodes(
                'service_embedding_index',
                $top_k,
                $embedding
            )
            YIELD node, score

            OPTIONAL MATCH (node)-[:EQUIVALENT_TO]-(equivalent)

            RETURN 
                node.name AS service,
                score,
                collect(DISTINCT equivalent.name) AS equivalents
            ORDER BY score DESC
        """, embedding=query_embedding, top_k=top_k)

        return result.data()


if __name__ == "__main__":
    user_query = input("Enter search query: ")
    results = hybrid_search(user_query)

    print("\n🔎 Top Intelligent Matches:")
    for r in results:
        print(f"\nService: {r['service']}")
        print(f"Score: {round(r['score'],4)}")
        print(f"Equivalent Services: {r['equivalents']}")