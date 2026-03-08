from graph.metadata_loader import make_service_id
from graph.cost_record_loader import make_cost_record_id


def test_make_service_id_is_provider_aware():
    aws = make_service_id("AWS", "Amazon S3")
    azure = make_service_id("Azure", "Amazon S3")
    assert aws != azure


def test_make_service_id_normalizes_whitespace_and_case():
    a = make_service_id("AWS", "  Amazon   S3 ")
    b = make_service_id("AWS", "amazon s3")
    assert a == b


def test_make_cost_record_id_is_deterministic():
    first = make_cost_record_id("AWS", 101, "2025-01-01", "2025-01-31", "res-1", "Amazon S3")
    second = make_cost_record_id("AWS", 101, "2025-01-01", "2025-01-31", "res-1", "Amazon S3")
    assert first == second


def test_make_cost_record_id_changes_with_row_id():
    first = make_cost_record_id("AWS", 101, "2025-01-01", "2025-01-31", "res-1", "Amazon S3")
    second = make_cost_record_id("AWS", 102, "2025-01-01", "2025-01-31", "res-1", "Amazon S3")
    assert first != second
