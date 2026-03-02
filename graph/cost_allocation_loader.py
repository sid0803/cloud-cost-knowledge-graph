# graph/cost_allocation_loader.py

import sqlite3
from graph.neo4j_connection import driver


def load_cost_allocations():

    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    print("Processing Azure Cost Centers...")

    azure_cost_centers = cursor.execute("""
        SELECT DISTINCT x_costcenter
        FROM azure_billing
        WHERE x_costcenter IS NOT NULL
    """).fetchall()

    with driver.session() as session:

        # -------------------------------------------------
        # Create Allocation Rule ONCE
        # -------------------------------------------------
        session.run("""
            MERGE (alloc:CostAllocation {
                allocationRuleName:"AzureCostCenterRule"
            })
            SET alloc.allocationMethod = "Proportional",
                alloc.allocationTargetType = "CostCentre",
                alloc.allocationBasis = "Tag",
                alloc.isSharedCost = false
        """)

        for row in azure_cost_centers:

            cost_center = row[0]

            if not cost_center:
                continue

            # -------------------------------------------------
            # Create CostCentre node
            # -------------------------------------------------
            session.run("""
                MERGE (cc:CostCentre {name:$name})
            """, name=cost_center)

            # -------------------------------------------------
            # Link Allocation → CostCentre
            # -------------------------------------------------
            session.run("""
                MATCH (alloc:CostAllocation {allocationRuleName:"AzureCostCenterRule"})
                MATCH (cc:CostCentre {name:$name})
                MERGE (alloc)-[:ALLOCATED_TO]->(cc)
            """, name=cost_center)

            # -------------------------------------------------
            # Find ALL CostRecords with matching Tag
            # -------------------------------------------------
            records = session.run("""
                MATCH (c:CostRecord)-[:HAS_TAG]->(t:Tag)
                WHERE toLower(t.key) = "costcentre"
                  AND t.value = $cost_center
                RETURN c.id AS id,
                       c.effectiveCost AS effectiveCost
            """, cost_center=cost_center).data()

            # -------------------------------------------------
            # Link each CostRecord to Allocation
            # -------------------------------------------------
            for record in records:

                cost_id = record["id"]
                effective_cost = record["effectiveCost"] or 0

                session.run("""
                    MATCH (c:CostRecord {id:$cid})
                    MATCH (alloc:CostAllocation {allocationRuleName:"AzureCostCenterRule"})
                    MERGE (c)-[r:ALLOCATED_VIA]->(alloc)
                    SET r.allocatedCost = $allocatedCost
                """,
                    cid=cost_id,
                    allocatedCost=effective_cost
                )

    conn.close()
    print("✅ Cost Allocation nodes created successfully")


if __name__ == "__main__":
    load_cost_allocations()