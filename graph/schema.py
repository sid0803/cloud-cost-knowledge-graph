from graph.neo4j_connection import driver


def create_schema():
    with driver.session() as session:
        # Drop legacy uniqueness constraint on Service.name if it exists.
        legacy_constraints = session.run(
            """
            SHOW CONSTRAINTS
            YIELD name, labelsOrTypes, properties, type
            WHERE type = 'UNIQUENESS'
              AND labelsOrTypes = ['Service']
              AND properties = ['name']
            RETURN name
            """
        ).data()
        for row in legacy_constraints:
            session.run(f"DROP CONSTRAINT {row['name']} IF EXISTS")

        constraints = [
            "FOR (s:Service)          REQUIRE s.serviceId IS UNIQUE",
            "FOR (r:Resource)         REQUIRE r.id IS UNIQUE",
            "FOR (a:Account)          REQUIRE a.id IS UNIQUE",
            "FOR (c:CostRecord)       REQUIRE c.id IS UNIQUE",
            "FOR (f:FOCUSColumn)      REQUIRE f.name IS UNIQUE",
            "FOR (aws:AWSColumn)      REQUIRE aws.name IS UNIQUE",
            "FOR (az:AzureColumn)     REQUIRE az.name IS UNIQUE",
            "FOR (l:Location)         REQUIRE l.regionId IS UNIQUE",
            "FOR (bp:BillingPeriod)   REQUIRE bp.start IS UNIQUE",
            "FOR (cc:CostCentre)      REQUIRE cc.name IS UNIQUE",
        ]
        for constraint in constraints:
            session.run(f"CREATE CONSTRAINT IF NOT EXISTS {constraint}")

        session.run("CREATE INDEX IF NOT EXISTS FOR (c:CostRecord) ON (c.billedCost)")
        session.run("CREATE INDEX IF NOT EXISTS FOR (c:CostRecord) ON (c.effectiveCost)")
        session.run("CREATE INDEX IF NOT EXISTS FOR (c:CostRecord) ON (c.cloudProvider)")
        session.run("CREATE INDEX IF NOT EXISTS FOR (s:Service) ON (s.name)")
        session.run("CREATE INDEX IF NOT EXISTS FOR (s:Service) ON (s.serviceCategory)")
        session.run("CREATE INDEX IF NOT EXISTS FOR (s:Service) ON (s.cloudProvider)")
        session.run("CREATE INDEX IF NOT EXISTS FOR (bp:BillingPeriod) ON (bp.start)")
        session.run("CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.billingAccountId)")

        for query in [
            "CREATE FULLTEXT INDEX service_fulltext IF NOT EXISTS FOR (s:Service) ON EACH [s.name, s.serviceCategory]",
            "CREATE FULLTEXT INDEX focus_fulltext IF NOT EXISTS FOR (f:FOCUSColumn) ON EACH [f.name, f.description]",
            "CREATE FULLTEXT INDEX charge_fulltext IF NOT EXISTS FOR (ch:Charge) ON EACH [ch.category, ch.description]",
        ]:
            try:
                session.run(query)
            except Exception:
                pass

        vector_indexes = [
            ("service_embedding_index", "Service"),
            ("focus_embedding_index", "FOCUSColumn"),
            ("aws_embedding_index", "AWSColumn"),
            ("azure_embedding_index", "AzureColumn"),
            ("charge_embedding_index", "Charge"),
            ("allocation_embedding_index", "CostAllocation"),
            ("resource_embedding_index", "Resource"),
        ]
        for index_name, label in vector_indexes:
            session.run(
                f"""
                CREATE VECTOR INDEX {index_name} IF NOT EXISTS
                FOR (n:{label}) ON (n.embedding)
                OPTIONS {{
                    indexConfig: {{
                        `vector.dimensions`: 384,
                        `vector.similarity_function`: 'cosine'
                    }}
                }}
                """
            )

    print("Schema, constraints, and indexes created successfully")


if __name__ == "__main__":
    create_schema()
