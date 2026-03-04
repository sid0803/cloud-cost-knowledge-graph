"""
check_setup.py — Pre-flight environment validator
Run this BEFORE setup_demo_db.py to catch all issues upfront.

Usage:
    python check_setup.py
"""

import sys
import os

PASS  = "✅"
FAIL  = "❌"
WARN  = "⚠️ "
SEP   = "─" * 60

results = []

def check(label, ok, fix_msg=""):
    status = PASS if ok else FAIL
    results.append((ok, label, fix_msg))
    print(f"  {status}  {label}")
    if not ok and fix_msg:
        for line in fix_msg.strip().splitlines():
            print(f"       {line}")

print()
print("=" * 60)
print("  Cloud Cost Knowledge Graph — Setup Pre-flight Check")
print("=" * 60)

# ── 1. Python Version ─────────────────────────────────────────
print(f"\n{SEP}")
print("  [1] Python Version")
print(SEP)
ver = sys.version_info

if ver >= (3, 13):
    # 3.13+ doesn't have stable torch/sentence-transformers wheels yet
    check(
        f"Python {ver.major}.{ver.minor}.{ver.micro} — TOO NEW ⚠️",
        False,
        f"Python 3.11.x or 3.12.x is required.\n"
        f"You are running Python {ver.major}.{ver.minor}.{ver.micro} which does NOT have\n"
        f"stable wheels for torch and sentence-transformers yet.\n\n"
        f"Fix: Activate your Python 3.11 virtual environment FIRST:\n"
        f"  Windows: venv\\Scripts\\activate\n"
        f"  macOS/Linux: source venv/bin/activate\n\n"
        f"Then re-run: python check_setup.py\n\n"
        f"If you don't have Python 3.11 installed:\n"
        f"  Download: https://www.python.org/downloads/release/python-3119/"
    )
elif ver >= (3, 11):
    check(f"Python {ver.major}.{ver.minor}.{ver.micro} — Perfect ✅", True)
else:
    check(
        f"Python {ver.major}.{ver.minor}.{ver.micro} — too old",
        False,
        "Python 3.11+ required.\nDownload: https://www.python.org/downloads/release/python-3119/"
    )

# ── 2. Required packages ──────────────────────────────────────
print(f"\n{SEP}")
print("  [2] Required Python Packages")
print(SEP)

PACKAGES = [
    ("neo4j",               "neo4j"),
    ("pandas",              "pandas"),
    ("dotenv",              "python-dotenv"),
    ("streamlit",           "streamlit"),
    ("fastapi",             "fastapi"),
    ("sentence_transformers","sentence-transformers"),
    ("sklearn",             "scikit-learn"),
    ("numpy",               "numpy"),
    ("requests",            "requests"),
    ("openpyxl",            "openpyxl"),
    ("xlrd",                "xlrd"),
]

missing_pkgs = []
for import_name, pip_name in PACKAGES:
    try:
        __import__(import_name)
        check(f"'{pip_name}' installed", True)
    except ImportError:
        check(
            f"'{pip_name}' NOT installed",
            False,
            f"Fix: pip install {pip_name}"
        )
        missing_pkgs.append(pip_name)

if missing_pkgs:
    print(f"\n  👉 Quick fix: pip install {' '.join(missing_pkgs)}")
    print( "  👉 Or install all at once: pip install -r requirements.txt")

# ── 3. .env file + required variables ────────────────────────
print(f"\n{SEP}")
print("  [3] Environment Variables (.env file)")
print(SEP)

env_path = os.path.join(os.path.dirname(__file__), ".env")
env_exists = os.path.isfile(env_path)
check(
    ".env file exists",
    env_exists,
    "Copy .env.example to .env and fill in your values:\n"
    "  copy .env.example .env    # Windows\n"
    "  cp .env.example .env      # macOS/Linux"
)

if env_exists:
    try:
        from dotenv import load_dotenv
        load_dotenv(env_path)
    except ImportError:
        pass

neo4j_pass = os.getenv("NEO4J_PASSWORD", "")
gemini_key = os.getenv("GEMINI_API_KEY", "")
neo4j_uri  = os.getenv("NEO4J_URI", "neo4j://127.0.0.1:7687")

check(
    f"NEO4J_PASSWORD set",
    bool(neo4j_pass),
    "Add to .env:  NEO4J_PASSWORD=your_neo4j_password"
)
check(
    "GEMINI_API_KEY set",
    bool(gemini_key),
    "Add to .env:  GEMINI_API_KEY=your_key\n"
    "Get free key: https://aistudio.google.com/apikey"
)
check(
    f"NEO4J_URI = {neo4j_uri}",
    True  # informational only
)

# ── 4. Data Files ─────────────────────────────────────────────
print(f"\n{SEP}")
print("  [4] Billing Data Files (XLS)")
print(SEP)

ROOT = os.path.dirname(os.path.abspath(__file__))
AWS_FILE   = "aws_test-focus-00001.snappy_transformed.xls"
AZURE_FILE = "focusazure_anon_transformed.xls"
SEARCH_DIRS = [
    os.path.join(ROOT, "data"),
    os.path.join(ROOT, "db"),
    ROOT,
]

def find_file(fname):
    for d in SEARCH_DIRS:
        p = os.path.join(d, fname)
        if os.path.isfile(p):
            return p
    return None

aws_path   = find_file(AWS_FILE)
azure_path = find_file(AZURE_FILE)

check(
    f"AWS XLS found: {aws_path or 'NOT FOUND'}",
    aws_path is not None,
    f"Run: git pull origin main\nFile should be in data/{AWS_FILE}"
)
check(
    f"Azure XLS found: {azure_path or 'NOT FOUND'}",
    azure_path is not None,
    f"Run: git pull origin main\nFile should be in data/{AZURE_FILE}"
)

# ── 5. Neo4j Connection ───────────────────────────────────────
print(f"\n{SEP}")
print("  [5] Neo4j Database Connection")
print(SEP)

neo4j_ok = False
if neo4j_pass:
    try:
        from neo4j import GraphDatabase
        drv = GraphDatabase.driver(neo4j_uri, auth=(
            os.getenv("NEO4J_USERNAME", "neo4j"), neo4j_pass
        ))
        with drv.session(database="neo4j") as s:
            count = s.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        drv.close()
        neo4j_ok = True
        check(f"Neo4j connected — {count:,} nodes in graph", True)
    except Exception as e:
        check(
            f"Neo4j connection failed: {e}",
            False,
            "Fix:\n"
            "1. Open Neo4j Desktop\n"
            "2. Start your database (wait for green 🟢)\n"
            "3. Verify password in .env matches Neo4j Desktop\n"
            "4. Default URI: neo4j://127.0.0.1:7687"
        )
else:
    check("Neo4j — skipped (password not set)", False,
          "Set NEO4J_PASSWORD in .env first")

# ── 6. Gemini API ─────────────────────────────────────────────
print(f"\n{SEP}")
print("  [6] Gemini API Key")
print(SEP)

if gemini_key:
    try:
        import requests
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.0-flash-lite:generateContent?key={gemini_key}",
            json={"contents": [{"parts": [{"text": "Say OK"}]}]},
            timeout=10
        )
        if resp.status_code == 200:
            check("Gemini API key valid and working", True)
        elif resp.status_code == 400:
            check("Gemini API reachable (key may have restrictions)", True)
        elif resp.status_code == 403:
            check("Gemini API key invalid or expired", False,
                  "Get a new free key: https://aistudio.google.com/apikey")
        else:
            check(f"Gemini API returned status {resp.status_code}", False,
                  "Check your key at https://aistudio.google.com/apikey")
    except Exception as e:
        check(f"Gemini API check failed: {e}", False,
              "Check internet connection and retry")
else:
    check("Gemini API — skipped (key not set)", False,
          "Add GEMINI_API_KEY to .env\nGet free key: https://aistudio.google.com/apikey")

# ── Summary ───────────────────────────────────────────────────
print()
print("=" * 60)
passed = sum(1 for ok, _, _ in results if ok)
failed = sum(1 for ok, _, _ in results if not ok)

if failed == 0:
    print(f"  🎉 ALL CHECKS PASSED ({passed}/{len(results)})")
    print()
    print("  You're ready! Run:")
    print("    python setup_demo_db.py")
    print("    streamlit run app.py")
else:
    print(f"  {passed} passed  |  {failed} need attention")
    print()
    print("  Fix the ❌ items above, then re-run:")
    print("    python check_setup.py")
    print()
    print("  Once all green, run:")
    print("    python setup_demo_db.py")
    print("    streamlit run app.py")

print("=" * 60)
print()
