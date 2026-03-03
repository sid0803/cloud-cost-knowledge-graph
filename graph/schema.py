# graph/schema.py

from graph.neo4j_connection import driver


def create_schema():
    with driver.session() as session:

        # ─────────────────────────────────────────
        # UNIQUENESS CONSTRAINTS
        # ─────────────────────────────────────────
        constraints = [
            "FOR (s:Service)          REQUIRE s.name IS UNIQUE",
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

        for c in constraints:
            session.run(f"CREATE CONSTRAINT IF NOT EXISTS {c}")

        # ─────────────────────────────────────────
        # STANDARD INDEXES
        # ─────────────────────────────────────────
        session.run("""
            CREATE INDEX IF NOT EXISTS
            FOR (c:CostRecord) ON (c.billedCost)
        """)
        session.run("""
            CREATE INDEX IF NOT EXISTS
            FOR (c:CostRecord) ON (c.effectiveCost)
        """)
        session.run("""
            CREATE INDEX IF NOT EXISTS
            FOR (c:CostRecord) ON (c.cloudProvider)
        """)
        session.run("""
            CREATE INDEX IF NOT EXISTS
            FOR (s:Service) ON (s.name)
        """)
        session.run("""
            CREATE INDEX IF NOT EXISTS
            FOR (s:Service) ON (s.serviceCategory)
        """)
        session.run("""
            CREATE INDEX IF NOT EXISTS
            FOR (bp:BillingPeriod) ON (bp.start)
        """)
        session.run("""
            CREATE INDEX IF NOT EXISTS
            FOR (a:Account) ON (a.billingAccountId)
        """)

        # ─────────────────────────────────────────
        # FULL-TEXT SEARCH INDEXES
        # ─────────────────────────────────────────
        try:
            session.run("""
                CREATE FULLTEXT INDEX service_fulltext IF NOT EXISTS
                FOR (s:Service) ON EACH [s.name, s.serviceCategory]
            """)
        except Exception:
            pass

        try:
            session.run("""
                CREATE FULLTEXT INDEX focus_fulltext IF NOT EXISTS
                FOR (f:FOCUSColumn) ON EACH [f.name, f.description]
            """)
        except Exception:
            pass

        try:
            session.run("""
                CREATE FULLTEXT INDEX charge_fulltext IF NOT EXISTS
                FOR (ch:Charge) ON EACH [ch.category, ch.description]
            """)
        except Exception:
            pass

        # ─────────────────────────────────────────
        # VECTOR INDEXES
        # ─────────────────────────────────────────
        vector_indexes = [
            ("service_embedding_index",    "Service",        "s"),
            ("focus_embedding_index",      "FOCUSColumn",    "f"),
            ("aws_embedding_index",        "AWSColumn",      "a"),
            ("azure_embedding_index",      "AzureColumn",    "z"),
            ("charge_embedding_index",     "Charge",         "ch"),
            ("allocation_embedding_index", "CostAllocation", "ca"),
            ("resource_embedding_index",   "Resource",       "r"),
        ]

        for idx_name, label, _ in vector_indexes:
            session.run(f"""
                CREATE VECTOR INDEX {idx_name} IF NOT EXISTS
                FOR (n:{label}) ON (n.embedding)
                OPTIONS {{
                    indexConfig: {{
                        `vector.dimensions`: 384,
                        `vector.similarity_function`: 'cosine'
                    }}
                }}
            """)

    print("✅ Schema + Constraints + Full-text + Vector Indexes created successfully")


if __name__ == "__main__":
    create_schema()