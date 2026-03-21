.PHONY: install clean lint fmt download load dbt-deps dbt-seed dbt-run dbt-test \
        dbt-snapshot dbt-docs backfill pytest ci \
        airflow-install airflow-init airflow-start airflow-stop airflow-reset

UV           := uv
DBT          := dbt
AIRFLOW_HOME := $(shell pwd)/airflow

install:
	$(UV) pip install -e ".[dev]"

clean:
	rm -rf target/ dbt_packages/ __pycache__ .pytest_cache

lint:
	ruff check .

fmt:
	ruff format .

download:
	python scripts/download_data.py --months $(MONTHS)

load:
	python scripts/load_to_snowflake.py --month $(MONTH)

dbt-deps:
	$(DBT) deps

dbt-seed:
	$(DBT) seed

dbt-run:
	$(DBT) run --target dev

dbt-test:
	$(DBT) test --target dev

dbt-snapshot:
	$(DBT) snapshot --target dev

dbt-docs:
	$(DBT) docs generate && $(DBT) docs serve

backfill:
	$(DBT) run --vars '{"start_date": "$(START)", "end_date": "$(END)"}' --full-refresh

pytest:
	pytest tests_pytest/ -v

ci: lint dbt-seed dbt-run dbt-test pytest

# ---------------------------------------------------------------------------
# Airflow orchestration
# ---------------------------------------------------------------------------

airflow-install:
	$(UV) pip install -e ".[orchestration]"

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
