.PHONY: install clean lint fmt download load dbt-deps dbt-seed dbt-run dbt-test \
        dbt-snapshot dbt-docs backfill pytest ci

UV  := uv
DBT := dbt

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
