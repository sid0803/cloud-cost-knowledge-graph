import pandas as pd
import sqlite3
import os

def load_data():
    aws_path = os.path.join("data", "aws_test-focus-00001.snappy_transformed.xls")
    azure_path = os.path.join("data", "focusazure_anon_transformed.xls")

    print("Loading AWS file...")
    aws_df = pd.read_excel(aws_path)

    print("Loading Azure file...")
    azure_df = pd.read_excel(azure_path)

    print("Connecting to SQLite...")
    conn = sqlite3.connect("billing.db")

    print("Storing AWS table...")
    aws_df.to_sql("aws_billing", conn, if_exists="replace", index=False)

    print("Storing Azure table...")
    azure_df.to_sql("azure_billing", conn, if_exists="replace", index=False)

    conn.close()
    print("✅ Data successfully stored in billing.db")

if __name__ == "__main__":
    load_data()