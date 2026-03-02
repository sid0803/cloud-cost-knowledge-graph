#graph/metadata_loader.py

import sqlite3
from graph.neo4j_connection import driver


# -----------------------------
# LOAD SERVICES
# -----------------------------
def load_services():

    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    aws_services = cursor.execute("""
        SELECT DISTINCT ServiceName FROM aws_billing
    """).fetchall()

    azure_services = cursor.execute("""
        SELECT DISTINCT ServiceName FROM azure_billing
    """).fetchall()

    with driver.session() as session:

        for service in aws_services:
            if service[0]:
                session.run("""
                    MERGE (s:Service {name: $name})
                    SET s.cloudProvider = "AWS"
                """, name=service[0])

        for service in azure_services:
            if service[0]:
                session.run("""
                    MERGE (s:Service {name: $name})
                    SET s.cloudProvider = "Azure"
                """, name=service[0])

    conn.close()
    print("✅ Services inserted successfully")


# -----------------------------
# LOAD ACCOUNTS
# -----------------------------
def load_accounts():

    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    aws_accounts = cursor.execute("""
        SELECT DISTINCT BillingAccountId
        FROM aws_billing
    """).fetchall()

    azure_accounts = cursor.execute("""
        SELECT DISTINCT BillingAccountId
        FROM azure_billing
    """).fetchall()

    with driver.session() as session:

        for acc in aws_accounts:
            if acc[0]:
                session.run("""
                    MERGE (a:Account {id: $id})
                    SET a.cloudProvider = "AWS"
                """, id=acc[0])

        for acc in azure_accounts:
            if acc[0]:
                session.run("""
                    MERGE (a:Account {id: $id})
                    SET a.cloudProvider = "Azure"
                """, id=acc[0])

    conn.close()
    print("✅ Accounts inserted successfully")


# -----------------------------
# LOAD RESOURCES
# -----------------------------
def load_resources():

    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    aws_resources = cursor.execute("""
        SELECT DISTINCT ResourceId, ServiceName
        FROM aws_billing
    """).fetchall()

    azure_resources = cursor.execute("""
        SELECT DISTINCT ResourceId, ServiceName
        FROM azure_billing
    """).fetchall()

    with driver.session() as session:

        for res in aws_resources:
            if res[0]:
                session.run("""
                    MERGE (r:Resource {id: $id})
                    SET r.cloudProvider = "AWS"
                """, id=res[0])

                if res[1]:
                    session.run("""
                        MATCH (r:Resource {id: $id})
                        MATCH (s:Service {name: $service})
                        MERGE (r)-[:BELONGS_TO]->(s)
                    """, id=res[0], service=res[1])

        for res in azure_resources:
            if res[0]:
                session.run("""
                    MERGE (r:Resource {id: $id})
                    SET r.cloudProvider = "Azure"
                """, id=res[0])

                if res[1]:
                    session.run("""
                        MATCH (r:Resource {id: $id})
                        MATCH (s:Service {name: $service})
                        MERGE (r)-[:BELONGS_TO]->(s)
                    """, id=res[0], service=res[1])

    conn.close()
    print("✅ Resources inserted successfully")


# -----------------------------
# LINK RESOURCES TO ACCOUNTS
# -----------------------------
def link_resources_to_accounts():

    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    aws_data = cursor.execute("""
        SELECT DISTINCT ResourceId, BillingAccountId
        FROM aws_billing
    """).fetchall()

    azure_data = cursor.execute("""
        SELECT DISTINCT ResourceId, BillingAccountId
        FROM azure_billing
    """).fetchall()

    with driver.session() as session:

        for row in aws_data:
            if row[0] and row[1]:
                session.run("""
                    MATCH (r:Resource {id:$rid})
                    MATCH (a:Account {id:$aid})
                    MERGE (r)-[:OWNED_BY]->(a)
                """, rid=row[0], aid=row[1])

        for row in azure_data:
            if row[0] and row[1]:
                session.run("""
                    MATCH (r:Resource {id:$rid})
                    MATCH (a:Account {id:$aid})
                    MERGE (r)-[:OWNED_BY]->(a)
                """, rid=row[0], aid=row[1])

    conn.close()
    print("✅ Resources linked to accounts successfully")

def load_locations():

    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    aws_locations = cursor.execute("""
        SELECT DISTINCT ResourceId, RegionName
        FROM aws_billing
    """).fetchall()

    azure_locations = cursor.execute("""
        SELECT DISTINCT ResourceId, RegionName
        FROM azure_billing
    """).fetchall()

    with driver.session() as session:

        for row in aws_locations:
            if row[0] and row[1]:
                session.run("""
                    MERGE (l:Location {name:$region})
                """, region=row[1])

                session.run("""
                    MATCH (r:Resource {id:$rid})
                    MATCH (l:Location {name:$region})
                    MERGE (r)-[:DEPLOYED_IN]->(l)
                """, rid=row[0], region=row[1])

        for row in azure_locations:
            if row[0] and row[1]:
                session.run("""
                    MERGE (l:Location {name:$region})
                """, region=row[1])

                session.run("""
                    MATCH (r:Resource {id:$rid})
                    MATCH (l:Location {name:$region})
                    MERGE (r)-[:DEPLOYED_IN]->(l)
                """, rid=row[0], region=row[1])

    conn.close()
    print("✅ Locations inserted successfully")

# -----------------------------
# MAIN EXECUTION
# -----------------------------
if __name__ == "__main__":
    load_services()
    load_accounts()
    load_resources()
    link_resources_to_accounts()
    load_locations()