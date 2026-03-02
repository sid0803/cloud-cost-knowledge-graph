#graph/service_mapping.py

from graph.neo4j_connection import driver

SERVICE_EQUIVALENCE = [

    # Compute
    ("Amazon Elastic Compute Cloud", "Azure App Service"),

    # Containers
    ("Amazon EC2 Container Registry (ECR)", "Azure Container Registry"),

    # Networking
    ("Amazon Virtual Private Cloud", "Azure DNS"),

    # Serverless
    ("AWS Lambda", "Azure Automation"),

    # Database example (if RDS exists in your data)
    ("Amazon Relational Database Service", "Azure DB for PostgreSQL"),

    # API
    ("Amazon API Gateway", "API Management")
]


def create_equivalence_relationships():

    with driver.session() as session:

        for aws_service, azure_service in SERVICE_EQUIVALENCE:
            session.run("""
                MATCH (s1:Service {name:$aws})
                MATCH (s2:Service {name:$azure})
                MERGE (s1)-[:EQUIVALENT_TO]->(s2)
                MERGE (s2)-[:EQUIVALENT_TO]->(s1)
            """, aws=aws_service, azure=azure_service)

    print("✅ Service equivalence relationships created")


if __name__ == "__main__":
    create_equivalence_relationships()