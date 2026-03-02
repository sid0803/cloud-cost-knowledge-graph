#graph/neo4j_connection.py

from neo4j import GraphDatabase
import os

URI = "neo4j://127.0.0.1:7687"
USERNAME = "neo4j"
PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j")

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))