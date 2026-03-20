.PHONY: install build validate deploy-dev deploy-prod run-dev fetch-2024 fetch-2025-round clean

PYTHON   := python3
UV       := uv
DATABRICKS := databricks

install:
	$(UV) pip install -e ".[dev]"

build:
	$(UV) build --wheel

clean:
	rm -rf dist/ *.egg-info/ __pycache__ .pytest_cache data/

validate:
	$(DATABRICKS) bundle validate --target dev

deploy-dev: build
	$(DATABRICKS) bundle deploy --target dev

deploy-prod: build
	$(DATABRICKS) bundle deploy --target prod

run-dev:
	$(DATABRICKS) bundle run f1_intelligence_job --target dev

fetch-2024:
	$(PYTHON) scripts/fetch_and_upload.py --season 2024 --mode full

fetch-2025-round:
	@if [ -z "$(ROUND)" ]; then \
		echo "Usage: make fetch-2025-round ROUND=1"; exit 1; \
	fi
	$(PYTHON) scripts/fetch_and_upload.py --season 2025 --round $(ROUND) --mode incremental
