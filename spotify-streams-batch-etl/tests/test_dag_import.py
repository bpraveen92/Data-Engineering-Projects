import pytest
from airflow.models import DagBag


def test_dag_loaded():
    """
    Validate Airflow can import the project DAG without errors.

    Args:
        None.

    Returns:
        None.
    """
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    for error in dag_bag.import_errors.values():
        if "No module named 'airflow.providers.amazon'" in error:
            pytest.skip("amazon provider package is not installed in this local test environment")
        if "No module named 'great_expectations'" in error:
            pytest.skip("great_expectations package is not installed in this local test environment")
    assert "s3_to_redshift_pipeline" in dag_bag.dags
    assert dag_bag.import_errors == {}
