# db/init_sqlite.py

import pandas as pd
import sqlite3
import os

# Project root = two levels up from this file (db/init_sqlite.py → project root)
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

AWS_FILENAME   = "aws_test-focus-00001.snappy_transformed.xls"
AZURE_FILENAME = "focusazure_anon_transformed.xls"

# Search order: data/ → db/ → project root → current working dir
_SEARCH_DIRS = [
    os.path.join(_ROOT, "data"),
    os.path.join(_ROOT, "db"),
    _ROOT,
    os.getcwd(),
    os.path.join(os.getcwd(), "data"),
    os.path.join(os.getcwd(), "db"),
]


def _find_file(filename: str) -> str:
    """Search for a billing XLS file across all known candidate directories."""
    for directory in _SEARCH_DIRS:
        candidate = os.path.join(directory, filename)
        if os.path.isfile(candidate):
            return candidate
    searched = "\n  ".join(_SEARCH_DIRS)
    raise FileNotFoundError(
        f"Cannot find '{filename}'.\n"
        f"Searched in:\n  {searched}\n\n"
        f"Place the file in the 'data/' folder of the project root and retry."
    )


def load_data():
    aws_path   = _find_file(AWS_FILENAME)
    azure_path = _find_file(AZURE_FILENAME)

    print(f"  AWS   file: {aws_path}")
    print(f"  Azure file: {azure_path}")

    print("Loading AWS file...")
    aws_df = pd.read_excel(aws_path)

    print("Loading Azure file...")
    azure_df = pd.read_excel(azure_path)

    # Always write billing.db to the project root
    db_path = os.path.join(_ROOT, "billing.db")
    print(f"Connecting to SQLite ({db_path})...")
    conn = sqlite3.connect(db_path)

    print("Storing AWS table...")
    aws_df.to_sql("aws_billing", conn, if_exists="replace", index=False)

    print("Storing Azure table...")
    azure_df.to_sql("azure_billing", conn, if_exists="replace", index=False)

    conn.close()
    print(f"✅ Data successfully stored in {db_path}")


if __name__ == "__main__":
    load_data()
