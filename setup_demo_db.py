# setup_demo_db.py
# One-command full pipeline setup: SQLite → Schema → FOCUS → Metadata → Records → Allocations → Mappings → Embeddings

import sys
import time

def step(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


def run():
    # 1. Load raw data into SQLite
    step("Step 1/9 — Loading XLS data into SQLite")
    from db.init_sqlite import load_data
    load_data()

    # 2. Create Neo4j schema + indexes
    step("Step 2/9 — Creating Neo4j schema, constraints & vector indexes")
    from graph.schema import create_schema
    create_schema()
    time.sleep(1)   # allow indexes to initialize

    # 3. Load FOCUS ontology
    step("Step 3/9 — Loading FOCUS 1.0 ontology + column schema")
    from graph.focus_schema_loader import load_focus_schema
    load_focus_schema()

    # 4. Load metadata (Services, Accounts, Resources, Locations)
    step("Step 4/9 — Loading metadata (Services, Accounts, Resources, Locations)")
    from graph.metadata_loader import (
        load_services, load_accounts,
        load_resources, link_resources_to_accounts, load_locations
    )
    load_services()
    load_accounts()
    load_resources()
    link_resources_to_accounts()
    load_locations()

    # 5. Load CostRecord nodes + all relationships
    step("Step 5/9 — Loading CostRecord nodes & graph relationships")
    from graph.cost_record_loader import load_cost_records
    load_cost_records()

    # 6. Load Cost Allocations
    step("Step 6/9 — Loading Cost Allocation nodes")
    from graph.cost_allocation_loader import load_cost_allocations
    load_cost_allocations()

    # 7. Service equivalence mappings
    step("Step 7/9 — Creating AWS ↔ Azure service equivalence relationships")
    from graph.service_mapping import create_equivalence_relationships
    create_equivalence_relationships()

    # 8. Embed service nodes
    step("Step 8/9 — Embedding Service nodes")
    from graph.embed_services import embed_services
    embed_services()

    # 9. Embed all other node types
    step("Step 9/9 — Embedding all remaining nodes (FOCUSColumn, Charge, Allocation, Resource)")
    from graph.embed_all_nodes import run_embedding_pipeline
    run_embedding_pipeline()

    print("\n" + "="*60)
    print("  ✅ ALL DONE — Knowledge Graph is fully loaded!")
    print("="*60)
    print("\nNext steps:")
    print("  1. Run the Streamlit UI:  streamlit run app.py")
    print("  2. Run the FastAPI server: uvicorn api:app --reload")
    print("     Then visit: http://127.0.0.1:8000/docs")
    print()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\n⚠️  Setup interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        print("Check Neo4j is running at bolt://127.0.0.1:7687")
        raise
