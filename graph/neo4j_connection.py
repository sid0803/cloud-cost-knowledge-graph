# graph/neo4j_connection.py

from neo4j import GraphDatabase
import os
import warnings
from dotenv import load_dotenv

load_dotenv()

URI      = os.getenv("NEO4J_URI", "neo4j://127.0.0.1:7687")
USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD")

_connection_error = None

if not PASSWORD:
    _connection_error = "NEO4J_PASSWORD not set in .env file"
    warnings.warn(f"⚠️  Neo4j: {_connection_error}")
    driver = None
else:
    try:
        driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
        # Quick connection test
        with driver.session(database="neo4j") as session:
            session.run("RETURN 1")
    except Exception as e:
        _connection_error = str(e)
        warnings.warn(f"⚠️  Neo4j connection failed: {e} — graph features disabled.")
        driver = None


def is_connected() -> bool:
    """Returns True if Neo4j driver is alive."""
    return driver is not None


def get_session():
    """Return a Neo4j session, or raise a clear error if not connected."""
    if driver is None:
        raise RuntimeError(
            f"Neo4j is not connected. Reason: {_connection_error}\n"
            "Ensure Neo4j is running and NEO4J_PASSWORD is set in .env"
        )
    return driver.session(database="neo4j")