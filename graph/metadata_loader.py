# graph/metadata_loader.py
# Loads Services (with ServiceCategory), Accounts (with names), Resources (with type/name), Locations

import sqlite3
from graph.neo4j_connection import driver

# ─────────────────────────────────────────────────────────────────────────────
# SERVICE CATEGORY MAPPING (FOCUS 1.0 ServiceCategory values)
# ─────────────────────────────────────────────────────────────────────────────
SERVICE_CATEGORY_MAP = {
    # Compute
    "ec2": "Compute", "compute": "Compute", "virtual machine": "Compute",
    "lambda": "Compute", "fargate": "Compute", "ecs": "Compute",
    "app service": "Compute", "functions": "Compute", "batch": "Compute",
    "lightsail": "Compute", "elastic beanstalk": "Compute",
    "container": "Compute", "kubernetes": "Compute", "eks": "Compute",
    "aks": "Compute",
    # Storage
    "s3": "Storage", "blob": "Storage", "storage": "Storage",
    "ebs": "Storage", "efs": "Storage", "glacier": "Storage",
    "file": "Storage", "disk": "Storage", "backup": "Storage",
    "archive": "Storage",
    # Databases
    "rds": "Databases", "dynamodb": "Databases", "cosmos": "Databases",
    "database": "Databases", "aurora": "Databases", "postgresql": "Databases",
    "mysql": "Databases", "redis": "Databases", "cassandra": "Databases",
    "sql": "Databases", "elasticache": "Databases",
    # Networking
    "vpc": "Networking", "cloudfront": "Networking", "route 53": "Networking",
    "load balancer": "Networking", "dns": "Networking", "cdn": "Networking",
    "network": "Networking", "firewall": "Networking", "nat": "Networking",
    "transit": "Networking", "express route": "Networking",
    "api gateway": "Networking", "api management": "Networking",
    # AI & ML
    "sagemaker": "AI and Machine Learning", "cognitive": "AI and Machine Learning",
    "machine learning": "AI and Machine Learning", "ai": "AI and Machine Learning",
    "rekognition": "AI and Machine Learning", "translate": "AI and Machine Learning",
    # Analytics
    "redshift": "Analytics", "athena": "Analytics", "glue": "Analytics",
    "kinesis": "Analytics", "analytics": "Analytics", "synapse": "Analytics",
    "databricks": "Analytics", "emr": "Analytics",
    # Security
    "shield": "Security", "waf": "Security", "kms": "Security",
    "secrets": "Security", "security": "Security", "guardduty": "Security",
    "sentinel": "Security", "key vault": "Security",
    # Developer Tools
    "codecommit": "Developer Tools", "codepipeline": "Developer Tools",
    "devops": "Developer Tools", "monitor": "Developer Tools",
    "cloudwatch": "Developer Tools", "xray": "Developer Tools",
    # Multicloud
    "arc": "Multicloud",
}


def infer_service_category(service_name: str) -> str:
    name_lower = service_name.lower()
    for keyword, category in SERVICE_CATEGORY_MAP.items():
        if keyword in name_lower:
            return category
    return "Other"


def make_service_id(provider: str, service_name: str) -> str:
    normalized = " ".join(str(service_name).strip().lower().split())
    return f"{provider}:{normalized}"


# ─────────────────────────────────────────────────────────────────────────────
# LOAD SERVICES
# ─────────────────────────────────────────────────────────────────────────────
def load_services():
    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    aws_services = cursor.execute(
        "SELECT DISTINCT ServiceName, ServiceCategory FROM aws_billing WHERE ServiceName IS NOT NULL"
    ).fetchall()

    azure_services = cursor.execute(
        "SELECT DISTINCT ServiceName, ServiceCategory FROM azure_billing WHERE ServiceName IS NOT NULL"
    ).fetchall()

    conn.close()

    with driver.session() as session:
        for (name, category) in aws_services:
            if not name:
                continue
            cat = category if category else infer_service_category(name)
            service_id = make_service_id("AWS", name)
            session.run("""
                MERGE (s:Service {serviceId: $serviceId})
                SET s.name            = $name,
                    s.cloudProvider   = "AWS",
                    s.serviceCategory = $category
            """, serviceId=service_id, name=name, category=cat)

        for (name, category) in azure_services:
            if not name:
                continue
            cat = category if category else infer_service_category(name)
            service_id = make_service_id("Azure", name)
            session.run("""
                MERGE (s:Service {serviceId: $serviceId})
                SET s.name            = $name,
                    s.cloudProvider   = "Azure",
                    s.serviceCategory = $category
            """, serviceId=service_id, name=name, category=cat)

    print("✅ Services loaded with ServiceCategory")


# ─────────────────────────────────────────────────────────────────────────────
# LOAD ACCOUNTS
# ─────────────────────────────────────────────────────────────────────────────
def load_accounts():
    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    # AWS — try to get BillingAccountName and SubAccount fields
    aws_cols = [row[1] for row in cursor.execute("PRAGMA table_info(aws_billing)").fetchall()]
    aws_name_col   = "BillingAccountName"   if "BillingAccountName"   in aws_cols else None
    aws_sub_id_col = "SubAccountId"         if "SubAccountId"         in aws_cols else None
    aws_sub_nm_col = "SubAccountName"       if "SubAccountName"       in aws_cols else None

    aws_select = f"""
        SELECT DISTINCT BillingAccountId,
               {aws_name_col   or 'NULL'},
               {aws_sub_id_col or 'NULL'},
               {aws_sub_nm_col or 'NULL'}
        FROM aws_billing WHERE BillingAccountId IS NOT NULL
    """
    aws_accounts = cursor.execute(aws_select).fetchall()

    az_cols = [row[1] for row in cursor.execute("PRAGMA table_info(azure_billing)").fetchall()]
    az_name_col   = "BillingAccountName"   if "BillingAccountName"   in az_cols else None
    az_sub_id_col = "SubAccountId"         if "SubAccountId"         in az_cols else None
    az_sub_nm_col = "SubAccountName"       if "SubAccountName"       in az_cols else None

    az_select = f"""
        SELECT DISTINCT BillingAccountId,
               {az_name_col   or 'NULL'},
               {az_sub_id_col or 'NULL'},
               {az_sub_nm_col or 'NULL'}
        FROM azure_billing WHERE BillingAccountId IS NOT NULL
    """
    azure_accounts = cursor.execute(az_select).fetchall()
    conn.close()

    with driver.session() as session:
        for (acc_id, acc_name, sub_id, sub_name) in aws_accounts:
            if not acc_id:
                continue
            session.run("""
                MERGE (a:Account {id: $id})
                SET a.cloudProvider      = "AWS",
                    a.billingAccountId   = $id,
                    a.billingAccountName = $name,
                    a.subAccountId       = $subId,
                    a.subAccountName     = $subName
            """, id=str(acc_id), name=acc_name, subId=sub_id, subName=sub_name)

        for (acc_id, acc_name, sub_id, sub_name) in azure_accounts:
            if not acc_id:
                continue
            session.run("""
                MERGE (a:Account {id: $id})
                SET a.cloudProvider      = "Azure",
                    a.billingAccountId   = $id,
                    a.billingAccountName = $name,
                    a.subAccountId       = $subId,
                    a.subAccountName     = $subName
            """, id=str(acc_id), name=acc_name, subId=sub_id, subName=sub_name)

    print("✅ Accounts loaded with full metadata")


# ─────────────────────────────────────────────────────────────────────────────
# LOAD RESOURCES
# ─────────────────────────────────────────────────────────────────────────────
def load_resources():
    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    # Check available columns
    aws_cols   = [row[1] for row in cursor.execute("PRAGMA table_info(aws_billing)").fetchall()]
    azure_cols = [row[1] for row in cursor.execute("PRAGMA table_info(azure_billing)").fetchall()]

    aws_name_col = "ResourceName" if "ResourceName" in aws_cols else None
    aws_type_col = "ResourceType" if "ResourceType" in aws_cols else None
    az_name_col  = "ResourceName" if "ResourceName" in azure_cols else None
    az_type_col  = "ResourceType" if "ResourceType" in azure_cols else None

    aws_rows = cursor.execute(f"""
        SELECT DISTINCT ResourceId, ServiceName,
               {aws_name_col or 'NULL'}, {aws_type_col or 'NULL'}
        FROM aws_billing WHERE ResourceId IS NOT NULL
    """).fetchall()

    azure_rows = cursor.execute(f"""
        SELECT DISTINCT ResourceId, ServiceName,
               {az_name_col or 'NULL'}, {az_type_col or 'NULL'}
        FROM azure_billing WHERE ResourceId IS NOT NULL
    """).fetchall()

    conn.close()

    with driver.session() as session:
        for (res_id, service_name, res_name, res_type) in aws_rows:
            if not res_id:
                continue
            session.run("""
                MERGE (r:Resource {id: $id})
                SET r.cloudProvider = "AWS",
                    r.resourceName  = $name,
                    r.resourceType  = $type
            """, id=str(res_id), name=res_name, type=res_type)

            if service_name:
                service_id = make_service_id("AWS", service_name)
                session.run("""
                    MATCH (r:Resource {id: $id})
                    MATCH (s:Service  {serviceId: $serviceId})
                    MERGE (r)-[:BELONGS_TO]->(s)
                """, id=str(res_id), serviceId=service_id)

        for (res_id, service_name, res_name, res_type) in azure_rows:
            if not res_id:
                continue
            session.run("""
                MERGE (r:Resource {id: $id})
                SET r.cloudProvider = "Azure",
                    r.resourceName  = $name,
                    r.resourceType  = $type
            """, id=str(res_id), name=res_name, type=res_type)

            if service_name:
                service_id = make_service_id("Azure", service_name)
                session.run("""
                    MATCH (r:Resource {id: $id})
                    MATCH (s:Service  {serviceId: $serviceId})
                    MERGE (r)-[:BELONGS_TO]->(s)
                """, id=str(res_id), serviceId=service_id)

    print("✅ Resources loaded with ResourceName + ResourceType")


# ─────────────────────────────────────────────────────────────────────────────
# LINK RESOURCES TO ACCOUNTS
# ─────────────────────────────────────────────────────────────────────────────
def link_resources_to_accounts():
    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    aws_data = cursor.execute("""
        SELECT DISTINCT ResourceId, BillingAccountId FROM aws_billing
        WHERE ResourceId IS NOT NULL AND BillingAccountId IS NOT NULL
    """).fetchall()

    azure_data = cursor.execute("""
        SELECT DISTINCT ResourceId, BillingAccountId FROM azure_billing
        WHERE ResourceId IS NOT NULL AND BillingAccountId IS NOT NULL
    """).fetchall()

    conn.close()

    with driver.session() as session:
        for (rid, aid) in aws_data + azure_data:
            session.run("""
                MATCH (r:Resource {id: $rid})
                MATCH (a:Account  {id: $aid})
                MERGE (r)-[:OWNED_BY]->(a)
            """, rid=str(rid), aid=str(aid))

    print("✅ Resources linked to accounts")


# ─────────────────────────────────────────────────────────────────────────────
# LOAD LOCATIONS
# ─────────────────────────────────────────────────────────────────────────────
def load_locations():
    conn = sqlite3.connect("billing.db")
    cursor = conn.cursor()

    aws_cols   = [row[1] for row in cursor.execute("PRAGMA table_info(aws_billing)").fetchall()]
    azure_cols = [row[1] for row in cursor.execute("PRAGMA table_info(azure_billing)").fetchall()]

    aws_region_id = "RegionId"   if "RegionId"   in aws_cols   else None
    az_region_id  = "RegionId"   if "RegionId"   in azure_cols else None

    aws_locs = cursor.execute(f"""
        SELECT DISTINCT {aws_region_id or 'NULL'}, RegionName, ResourceId
        FROM aws_billing WHERE RegionName IS NOT NULL
    """).fetchall()

    azure_locs = cursor.execute(f"""
        SELECT DISTINCT {az_region_id or 'NULL'}, RegionName, ResourceId
        FROM azure_billing WHERE RegionName IS NOT NULL
    """).fetchall()

    conn.close()

    with driver.session() as session:
        seen_regions = set()
        for (region_id, region_name, res_id) in aws_locs + azure_locs:
            if region_name and region_name not in seen_regions:
                seen_regions.add(region_name)
                rid_val = region_id if region_id else region_name.replace(" ", "-").lower()
                session.run("""
                    MERGE (l:Location {regionId: $rid})
                    SET l.regionName = $name
                """, rid=str(rid_val), name=region_name)

            if res_id and region_name:
                rid_val = region_id if region_id else region_name.replace(" ", "-").lower()
                session.run("""
                    MATCH (r:Resource  {id: $rid})
                    MATCH (l:Location  {regionId: $loc})
                    MERGE (r)-[:DEPLOYED_IN]->(l)
                """, rid=str(res_id), loc=str(rid_val))

    print("✅ Locations loaded with RegionId + RegionName")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    load_services()
    load_accounts()
    load_resources()
    link_resources_to_accounts()
    load_locations()
