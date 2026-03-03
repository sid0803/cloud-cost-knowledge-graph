import json
from rag.llm_pipeline import generate_answer

def test_11_queries():
    test_queries = [
       "Which are the core FOCUS columns and how do they differ from vendor specific columns?",
       "Find all AWS compute services",
       "What is the Azure equivalent of AWS S3?",
       "Compare storage costs between AWS and Azure",
       "Find the top 5 most expensive resources tagged as 'Production' in Azure",
       "When calculating commitment utilization using Commitment Discount Quantity, which charge categories must be excluded to avoid double counting?",
       "Why does my total increase when I include commitment purchases and usage together?",
       "which cost type should be used to analyze cloud spend?",
       "Can ContractedCost differ from ContractedUnitPrice × PricingQuantity for a normal Usage charge? If so, when"
    ]

    results = []
    print("Beginning execution of validation prompts...")
    for idx, q in enumerate(test_queries, 1):
        print(f"\\n[{idx}/11] Running Query: {q}")
        res = generate_answer(q)
        results.append({
            "query": q,
            "method": res.get("retrieval_method", "hybrid"),
            "answer": res.get("answer", ""),
            "paths": res.get("provenance", [])
        })

    with open("evaluation_log.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print("\\n✅ COMPLETED ALL QUERIES. SAVED TO evaluation_log.json")

if __name__ == "__main__":
    test_11_queries()
