from rag.context_builder import detect_intent, extract_billing_period


def test_detect_storage_intent():
    assert detect_intent("Compare storage costs between AWS and Azure") == "storage_comparison"


def test_detect_cost_aggregation_intent():
    assert detect_intent("Show total aws cost for 2025-01") == "cost_aggregation"


def test_extract_billing_period_iso():
    assert extract_billing_period("show costs for 2025-02") == "2025-02"


def test_extract_billing_period_month_name():
    assert extract_billing_period("show costs for March 2024") == "2024-03"
