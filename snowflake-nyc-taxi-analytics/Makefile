.PHONY: install clean download load dbt-deps dbt-seed dbt-run dbt-test \
        dbt-snapshot dbt-docs backfill pytest ci \
        airflow-install airflow-init airflow-start airflow-stop airflow-reset

AIRFLOW_HOME := $(shell pwd)/airflow

install:
	uv pip install -e ".[dev]"

clean:
	rm -rf target/ dbt_packages/ __pycache__ .pytest_cache

download:
	python scripts/download_data.py --months $(MONTHS)

load:
	python scripts/load_to_snowflake.py --month $(MONTH)

dbt-deps:
	dbt deps

dbt-seed:
	dbt seed

dbt-run:
	dbt run --target dev

dbt-test:
	dbt test --target dev

dbt-snapshot:
	dbt snapshot --target dev

dbt-docs:
	dbt docs generate && dbt docs serve

backfill:
	dbt run --vars '{"start_date": "$(START)", "end_date": "$(END)"}' --full-refresh

pytest:
	pytest tests_pytest/ -v

ci: dbt-seed dbt-run dbt-test pytest

airflow-install:
	uv pip install -e ".[orchestration]"

airflow-init:
	AIRFLOW_HOME=$(AIRFLOW_HOME) \
	AIRFLOW__CORE__DAGS_FOLDER=$(shell pwd)/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	airflow db migrate

airflow-start:
	AIRFLOW_HOME=$(AIRFLOW_HOME) \
	AIRFLOW__CORE__DAGS_FOLDER=$(shell pwd)/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER=True \
	airflow standalone

airflow-stop:
	pkill -f "airflow standalone" || true

airflow-reset:
	rm -rf $(AIRFLOW_HOME)/airflow.db $(AIRFLOW_HOME)/logs \
	       $(AIRFLOW_HOME)/airflow.cfg $(AIRFLOW_HOME)/webserver_config.py \
	       $(AIRFLOW_HOME)/standalone_admin_password.txt
