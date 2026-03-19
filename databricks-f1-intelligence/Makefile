.PHONY: install build validate deploy-dev deploy-prod run-dev fetch-2024 fetch-2025-round lint fmt clean

PYTHON   := python3
UV       := uv
DATABRICKS := databricks

# ── Local dev ──────────────────────────────────────────────────────────────────

install:
	$(UV) pip install -e ".[dev]"

build:
	$(UV) build --wheel

lint:
	$(UV) run ruff check utils/ scripts/ streamlit_app/

fmt:
	$(UV) run ruff format utils/ scripts/ streamlit_app/

clean:
	rm -rf dist/ *.egg-info/ __pycache__ .pytest_cache data/

# ── Databricks Asset Bundle ────────────────────────────────────────────────────

validate:
	$(DATABRICKS) bundle validate --target dev

deploy-dev: build
	$(DATABRICKS) bundle deploy --target dev

deploy-prod: build
	$(DATABRICKS) bundle deploy --target prod

run-dev:
	$(DATABRICKS) bundle run f1_intelligence_job --target dev

# ── Data fetch ─────────────────────────────────────────────────────────────────

fetch-2024:
	$(PYTHON) scripts/fetch_and_upload.py --season 2024 --mode full

fetch-2025-round:
	@if [ -z "$(ROUND)" ]; then \
		echo "Usage: make fetch-2025-round ROUND=1"; exit 1; \
	fi
	$(PYTHON) scripts/fetch_and_upload.py --season 2025 --round $(ROUND) --mode incremental
