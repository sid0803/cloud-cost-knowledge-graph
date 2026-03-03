# graph/cost_record_loader.py
# Loads CostRecord nodes with full FOCUS fields and all graph relationships

import sqlite3
from graph.neo4j_connection import driver
import uuid


def parse_tags(tag_string):
    tag_dict = {}
    if not tag_string:
        return tag_dict
    tag_string = str(tag_string).replace(";", ",")
    pairs = tag_string.split(",")
    for pair in pairs:
        if "=" in pair:
            key, value = pair.split("=", 1)
        elif ":" in pair:
            key, value = pair.split(":", 1)
        else:
            continue
        tag_dict[key.strip().lower()] = value.strip()
    return tag_dict


def safe_float(val):
    try:
        return max(float(val), 0.0)
    except (TypeError, ValueError):
        return 0.0


def create_cost_record(session, row, provider):
    (
        resource_id, resource_name, resource_type,
        service_name, account_id, sub_account_id,
        charge_category, charge_frequency, charge_description, charge_class,
        start, end,
        billed_cost, effective_cost, contracted_cost, list_cost,
        consumed_qty, consumed_unit,
        currency,
        vendor_field_1, vendor_field_2,
        tags
    ) = row

    cost_id = str(uuid.uuid4())

    billed_cost    = safe_float(billed_cost)
    effective_cost = safe_float(effective_cost)
    contracted_cost= safe_float(contracted_cost)
    list_cost      = safe_float(list_cost)
    consumed_qty   = safe_float(consumed_qty)

    # ── CostRecord Node ──────────────────────────────────────────────────────
    session.run("""
        MERGE (c:CostRecord {id: $id})
        SET c.billedCost      = $billed,
            c.effectiveCost   = $effective,
            c.contractedCost  = $contracted,
            c.listCost        = $list_cost,
            c.consumedQuantity= $qty,
            c.consumedUnit    = $unit,
            c.currency        = $currency,
            c.cloudProvider   = $provider
    """,
        id=cost_id, billed=billed_cost, effective=effective_cost,
        contracted=contracted_cost, list_cost=list_cost,
        qty=consumed_qty, unit=consumed_unit, currency=currency,
        provider=provider
    )

    # ── Resource (create inline so INCURRED_BY never fails) ──────────────────
    if resource_id:
        session.run("""
            MERGE (r:Resource {id: $rid})
            ON CREATE SET r.cloudProvider = $provider,
                          r.resourceName  = $name,
                          r.resourceType  = $type
        """, rid=str(resource_id), provider=provider,
             name=resource_name, type=resource_type)

        session.run("""
            MATCH (c:CostRecord {id: $cid})
            MATCH (r:Resource   {id: $rid})
            MERGE (c)-[:INCURRED_BY]->(r)
        """, cid=cost_id, rid=str(resource_id))

    # ── Service ──────────────────────────────────────────────────────────────
    if service_name:
        session.run("""
            MATCH (c:CostRecord {id: $cid})
            MATCH (s:Service    {name: $service})
            MERGE (c)-[:USES_SERVICE]->(s)
        """, cid=cost_id, service=service_name)

    # ── Account ──────────────────────────────────────────────────────────────
    if account_id:
        session.run("""
            MATCH (c:CostRecord {id: $cid})
            MATCH (a:Account    {id: $aid})
            MERGE (c)-[:BELONGS_TO_BILLING_ACCOUNT]->(a)
        """, cid=cost_id, aid=str(account_id))

    # ── Sub-Account ──────────────────────────────────────────────────────────
    if sub_account_id:
        session.run("""
            MERGE (sa:Account {id: $said})
            ON CREATE SET sa.subAccountId = $said, sa.cloudProvider = $provider
            WITH sa
            MATCH (c:CostRecord {id: $cid})
            MERGE (c)-[:BELONGS_TO_SUBACCOUNT]->(sa)
        """, said=str(sub_account_id), cid=cost_id, provider=provider)

    # ── BillingPeriod ────────────────────────────────────────────────────────
    if start and end:
        session.run("""
            MERGE (p:BillingPeriod {start: $start})
            ON CREATE SET p.end = $end
        """, start=str(start), end=str(end))
        session.run("""
            MATCH (c:CostRecord    {id: $cid})
            MATCH (p:BillingPeriod {start: $start})
            MERGE (c)-[:IN_BILLING_PERIOD]->(p)
        """, cid=cost_id, start=str(start))

    # ── Charge ───────────────────────────────────────────────────────────────
    if charge_category:
        session.run("""
            MERGE (ch:Charge {category: $category, description: $description})
            ON CREATE SET ch.frequency   = $frequency,
                          ch.chargeClass = $charge_class
        """,
            category=charge_category or "Unknown",
            description=charge_description or "",
            frequency=charge_frequency,
            charge_class=charge_class
        )
        session.run("""
            MATCH (c:CostRecord {id: $cid})
            MATCH (ch:Charge {category: $category, description: $description})
            MERGE (c)-[:HAS_CHARGE]->(ch)
        """,
            cid=cost_id,
            category=charge_category or "Unknown",
            description=charge_description or ""
        )

    # ── Tags ─────────────────────────────────────────────────────────────────
    tag_pairs = parse_tags(tags)
    for key, value in tag_pairs.items():
        session.run("""
            MERGE (t:Tag {key: $key, value: $value})
            WITH t
            MATCH (c:CostRecord {id: $cid})
            MERGE (c)-[:HAS_TAG]->(t)
        """, key=key, value=value, cid=cost_id)

    # ── VendorSpecificAttributes ──────────────────────────────────────────────
    if vendor_field_1 or vendor_field_2:
        session.run("""
            MERGE (v:VendorSpecificAttributes {
                field1: $f1, field2: $f2, provider: $provider
            })
        """, f1=str(vendor_field_1) if vendor_field_1 else "",
             f2=str(vendor_field_2) if vendor_field_2 else "",
             provider=provider)
        session.run("""
            MATCH (c:CostRecord {id: $cid})
            MATCH (v:VendorSpecificAttributes {
                field1: $f1, field2: $f2, provider: $provider
            })
            MERGE (c)-[:HAS_VENDOR_ATTRS]->(v)
        """, cid=cost_id,
             f1=str(vendor_field_1) if vendor_field_1 else "",
             f2=str(vendor_field_2) if vendor_field_2 else "",
             provider=provider)


def _get_col(cursor, table, col_name):
    """Return col_name if it exists in table, else 'NULL'."""
    cols = [row[1] for row in cursor.execute(f"PRAGMA table_info({table})").fetchall()]
    return col_name if col_name in cols else "NULL"


def load_cost_records(limit=None):
    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    lim = f"LIMIT {limit}" if limit else ""

    # ── AWS ──────────────────────────────────────────────────────────────────
    print("Loading AWS records...")
    rn  = _get_col(cursor, "aws_billing", "ResourceName")
    rt  = _get_col(cursor, "aws_billing", "ResourceType")
    sub = _get_col(cursor, "aws_billing", "SubAccountId")
    lc  = _get_col(cursor, "aws_billing", "ListCost")
    cur = _get_col(cursor, "aws_billing", "BillingCurrency")

    aws_rows = cursor.execute(f"""
        SELECT ResourceId, {rn}, {rt},
               ServiceName, BillingAccountId, {sub},
               ChargeCategory, ChargeFrequency, ChargeDescription, ChargeClass,
               ChargePeriodStart, ChargePeriodEnd,
               BilledCost, EffectiveCost, ContractedCost, {lc},
               ConsumedQuantity, ConsumedUnit,
               {cur},
               x_ServiceCode, x_UsageType, Tags
        FROM aws_billing {lim}
    """).fetchall()

    # ── Azure ─────────────────────────────────────────────────────────────────
    print("Loading Azure records...")
    rn2  = _get_col(cursor, "azure_billing", "ResourceName")
    rt2  = _get_col(cursor, "azure_billing", "ResourceType")
    sub2 = _get_col(cursor, "azure_billing", "SubAccountId")
    lc2  = _get_col(cursor, "azure_billing", "ListCost")
    cur2 = _get_col(cursor, "azure_billing", "BillingCurrency")

    azure_rows = cursor.execute(f"""
        SELECT resourceid, {rn2}, {rt2},
               servicename, billingaccountid, {sub2},
               chargecategory, chargefrequency, chargedescription, chargeclass,
               chargeperiodstart, chargeperiodend,
               billedcost, effectivecost, contractedcost, {lc2},
               consumedquantity, consumedunit,
               {cur2},
               x_skumetercategory, x_skudescription, tags
        FROM azure_billing {lim}
    """).fetchall()

    conn.close()

    with driver.session() as session:
        print(f"  → Inserting {len(aws_rows)} AWS + {len(azure_rows)} Azure records...")
        for row in aws_rows:
            create_cost_record(session, row, "AWS")
        for row in azure_rows:
            create_cost_record(session, row, "Azure")

    print(f"✅ CostRecords inserted: {len(aws_rows)} AWS + {len(azure_rows)} Azure")


if __name__ == "__main__":
    load_cost_records()