from transform_jobs.csv_to_curated_transform_job import sanitize_column


def test_sanitize_column():
    """
    Validate helper converts source headers into normalized names.

    Args:
        None.

    Returns:
        None.
    """
    assert sanitize_column("Order Amount($)") == "order_amount"
    assert sanitize_column(" customer-id ") == "customer_id"
