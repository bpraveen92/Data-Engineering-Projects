.PHONY: install clean validate deploy-dev deploy-prod run-dev test

UV         := uv
DATABRICKS := databricks

install:
	$(UV) pip install -e ".[dev]"

clean:
	rm -rf __pycache__ .pytest_cache

validate:
	$(DATABRICKS) bundle validate --target dev

deploy-dev:
	$(DATABRICKS) bundle deploy --target dev

deploy-prod:
	$(DATABRICKS) bundle deploy --target prod

run-dev:
	$(DATABRICKS) bundle run ecommerce_dlt_pipeline --target dev

test:
	$(UV) run pytest tests/ -v
