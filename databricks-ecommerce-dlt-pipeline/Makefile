.PHONY: install build lint fmt clean validate deploy-dev deploy-prod run-dev

UV         := uv
DATABRICKS := databricks

install:
	$(UV) pip install -e ".[dev]"

build:
	$(UV) build --wheel

lint:
	$(UV) run ruff check utils/ pipeline/

fmt:
	$(UV) run ruff format utils/ pipeline/

clean:
	rm -rf dist/ *.egg-info/ __pycache__ .pytest_cache

validate:
	$(DATABRICKS) bundle validate --target dev

deploy-dev: build
	$(DATABRICKS) bundle deploy --target dev

deploy-prod: build
	$(DATABRICKS) bundle deploy --target prod

run-dev:
	$(DATABRICKS) bundle run ecommerce_dlt_pipeline --target dev
