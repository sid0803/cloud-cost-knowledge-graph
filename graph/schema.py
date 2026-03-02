# graph/schema.py

from graph.neo4j_connection import driver


def create_schema():
    with driver.session() as session:

        # -----------------------------
        # UNIQUENESS CONSTRAINTS
        # -----------------------------
        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (s:Service) REQUIRE s.name IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (r:Resource) REQUIRE r.id IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (a:Account) REQUIRE a.id IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (c:CostRecord) REQUIRE c.id IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (f:FOCUSColumn) REQUIRE f.name IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (aws:AWSColumn) REQUIRE aws.name IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (az:AzureColumn) REQUIRE az.name IS UNIQUE
        """)

        # -----------------------------
        # STANDARD INDEXES
        # -----------------------------
        session.run("""
        CREATE INDEX IF NOT EXISTS
        FOR (c:CostRecord) ON (c.billedCost)
        """)

        session.run("""
        CREATE INDEX IF NOT EXISTS
        FOR (c:CostRecord) ON (c.effectiveCost)
        """)

        # -----------------------------
        # VECTOR INDEXES
        # -----------------------------
        session.run("""
        CREATE VECTOR INDEX service_embedding_index IF NOT EXISTS
        FOR (s:Service) ON (s.embedding)
        OPTIONS {
            indexConfig: {
                `vector.dimensions`: 384,
                `vector.similarity_function`: 'cosine'
            }
        }
        """)

        session.run("""
        CREATE VECTOR INDEX focus_embedding_index IF NOT EXISTS
        FOR (f:FOCUSColumn) ON (f.embedding)
        OPTIONS {
            indexConfig: {
                `vector.dimensions`: 384,
                `vector.similarity_function`: 'cosine'
            }
        }
        """)

        session.run("""
        CREATE VECTOR INDEX aws_embedding_index IF NOT EXISTS
        FOR (a:AWSColumn) ON (a.embedding)
        OPTIONS {
            indexConfig: {
                `vector.dimensions`: 384,
                `vector.similarity_function`: 'cosine'
            }
        }
        """)

        session.run("""
        CREATE VECTOR INDEX azure_embedding_index IF NOT EXISTS
        FOR (z:AzureColumn) ON (z.embedding)
        OPTIONS {
            indexConfig: {
                `vector.dimensions`: 384,
                `vector.similarity_function`: 'cosine'
            }
        }
        """)

        session.run("""
        CREATE VECTOR INDEX charge_embedding_index IF NOT EXISTS
        FOR (ch:Charge) ON (ch.embedding)
        OPTIONS {
            indexConfig: {
                `vector.dimensions`: 384,
                `vector.similarity_function`: 'cosine'
            }
        }
        """)

        session.run("""
        CREATE VECTOR INDEX allocation_embedding_index IF NOT EXISTS
        FOR (c:CostAllocation) ON (c.embedding)
        OPTIONS {
            indexConfig: {
                `vector.dimensions`: 384,
                `vector.similarity_function`: 'cosine'
            }
        }
        """)

    print("✅ Schema + Vector Indexes created successfully")


if __name__ == "__main__":
    create_schema()