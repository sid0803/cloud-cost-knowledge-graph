# graph/cost_record_loader.py

import sqlite3
from graph.neo4j_connection import driver
import uuid


# =================================================
# Helper: Parse Tags
# =================================================
def parse_tags(tag_string):
    """
    Expected format examples:
    key1=value1;key2=value2
    key1:value1,key2:value2
    """
    tag_dict = {}

    if not tag_string:
        return tag_dict

    # Normalize separators
    tag_string = tag_string.replace(";", ",")
    pairs = tag_string.split(",")

    for pair in pairs:
        if "=" in pair:
            key, value = pair.split("=", 1)
        elif ":" in pair:
            key, value = pair.split(":", 1)
        else:
            continue

        tag_dict[key.strip()] = value.strip()

    return tag_dict


# =================================================
# Load Records
# =================================================
def load_cost_records():

    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    print("Loading AWS records...")
    aws_rows = cursor.execute("""
        SELECT 
            ResourceId,
            ServiceName,
            BillingAccountId,
            ChargeCategory,
            ChargeFrequency,
            ChargeDescription,
            ChargeClass,
            ChargePeriodStart,
            ChargePeriodEnd,
            BilledCost,
            EffectiveCost,
            ContractedCost,
            ConsumedQuantity,
            ConsumedUnit,
            x_ServiceCode,
            x_UsageType,
            Tags
        FROM aws_billing
    """).fetchall()

    print("Loading Azure records...")
    azure_rows = cursor.execute("""
        SELECT 
            resourceid,
            servicename,
            billingaccountid,
            chargecategory,
            chargefrequency,
            chargedescription,
            chargeclass,
            chargeperiodstart,
            chargeperiodend,
            billedcost,
            effectivecost,
            contractedcost,
            consumedquantity,
            consumedunit,
            x_skumetercategory,
            x_skudescription,
            tags
        FROM azure_billing
    """).fetchall()

    with driver.session() as session:
        for row in aws_rows:
            create_cost_record(session, row, "AWS")

        for row in azure_rows:
            create_cost_record(session, row, "Azure")

    conn.close()
    print("✅ CostRecords inserted successfully")


# =================================================
# Create CostRecord
# =================================================
def create_cost_record(session, row, provider):

    (
        resource_id,
        service_name,
        account_id,
        charge_category,
        charge_frequency,
        charge_description,
        charge_class,
        start,
        end,
        billed_cost,
        effective_cost,
        contracted_cost,
        consumed_qty,
        consumed_unit,
        vendor_field_1,
        vendor_field_2,
        tags
    ) = row

    cost_id = str(uuid.uuid4())

    # Basic validation enforcement
    billed_cost = max(billed_cost or 0, 0)
    effective_cost = max(effective_cost or 0, 0)
    contracted_cost = max(contracted_cost or 0, 0)
    consumed_qty = max(consumed_qty or 0, 0)

    # -------------------------------------------------
    # Create CostRecord
    # -------------------------------------------------
    session.run("""
        MERGE (c:CostRecord {id:$id})
        SET c.billedCost = $billed,
            c.effectiveCost = $effective,
            c.contractedCost = $contracted,
            c.consumedQuantity = $qty,
            c.consumedUnit = $unit,
            c.cloudProvider = $provider
    """,
        id=cost_id,
        billed=billed_cost,
        effective=effective_cost,
        contracted=contracted_cost,
        qty=consumed_qty,
        unit=consumed_unit,
        provider=provider
    )

    # -------------------------------------------------
    # Parse and Attach Tags
    # -------------------------------------------------
    tag_pairs = parse_tags(tags)

    for key, value in tag_pairs.items():
        session.run("""
            MERGE (t:Tag {key:$key, value:$value})
            WITH t
            MATCH (c:CostRecord {id:$cid})
            MERGE (c)-[:HAS_TAG]->(t)
        """, key=key, value=value, cid=cost_id)

    # -------------------------------------------------
    # Link to Resource
    # -------------------------------------------------
    if resource_id:
        session.run("""
            MATCH (c:CostRecord {id:$cid})
            MATCH (r:Resource {id:$rid})
            MERGE (c)-[:INCURRED_BY]->(r)
        """, cid=cost_id, rid=resource_id)

    # -------------------------------------------------
    # Link to Service
    # -------------------------------------------------
    if service_name:
        session.run("""
            MATCH (c:CostRecord {id:$cid})
            MATCH (s:Service {name:$service})
            MERGE (c)-[:USES_SERVICE]->(s)
        """, cid=cost_id, service=service_name)

    # -------------------------------------------------
    # Link to Account
    # -------------------------------------------------
    if account_id:
        session.run("""
            MATCH (c:CostRecord {id:$cid})
            MATCH (a:Account {id:$aid})
            MERGE (c)-[:BELONGS_TO_ACCOUNT]->(a)
        """, cid=cost_id, aid=account_id)

    # -------------------------------------------------
    # Billing Period
    # -------------------------------------------------
    if start and end:
        session.run("""
            MERGE (p:BillingPeriod {start:$start, end:$end})
        """, start=start, end=end)

        session.run("""
            MATCH (c:CostRecord {id:$cid})
            MATCH (p:BillingPeriod {start:$start, end:$end})
            MERGE (c)-[:IN_PERIOD]->(p)
        """, cid=cost_id, start=start, end=end)

    # -------------------------------------------------
    # Charge Node
    # -------------------------------------------------
    if charge_category:
        session.run("""
            MERGE (ch:Charge {
                category:$category,
                description:$description
            })
            SET ch.frequency = $frequency,
                ch.chargeClass = $charge_class
        """,
            category=charge_category,
            description=charge_description,
            frequency=charge_frequency,
            charge_class=charge_class
        )

        session.run("""
            MATCH (c:CostRecord {id:$cid})
            MATCH (ch:Charge {category:$category, description:$description})
            MERGE (c)-[:HAS_CHARGE]->(ch)
        """,
            cid=cost_id,
            category=charge_category,
            description=charge_description
        )

    # -------------------------------------------------
    # Vendor Specific Attributes (MERGE instead of CREATE)
    # -------------------------------------------------
    if vendor_field_1 or vendor_field_2:
        session.run("""
            MERGE (v:VendorSpecificAttributes {
                field1:$f1,
                field2:$f2,
                provider:$provider
            })
        """,
            f1=vendor_field_1,
            f2=vendor_field_2,
            provider=provider
        )

        session.run("""
            MATCH (c:CostRecord {id:$cid})
            MATCH (v:VendorSpecificAttributes {
                field1:$f1,
                field2:$f2,
                provider:$provider
            })
            MERGE (c)-[:HAS_VENDOR_ATTRS]->(v)
        """,
            cid=cost_id,
            f1=vendor_field_1,
            f2=vendor_field_2,
            provider=provider
        )


if __name__ == "__main__":
    load_cost_records()