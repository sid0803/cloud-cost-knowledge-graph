# graph/neo4j_connection.py

from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI", "neo4j://127.0.0.1:7687")
USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD")

if not PASSWORD:
    raise ValueError("NEO4J_PASSWORD not set in .env file")

try:
    driver = GraphDatabase.driver(
        URI,
        auth=(USERNAME, PASSWORD)
    )

    # Immediate connection test
    with driver.session(database="neo4j") as session:
        session.run("RETURN 1")

except Exception as e:
    raise RuntimeError(f"Failed to connect to Neo4j: {e}")


def get_session():
    return driver.session(database="neo4j")