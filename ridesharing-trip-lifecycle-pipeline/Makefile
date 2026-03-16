.PHONY: help install test clean setup-local local-producer local-runner local-all up down logs docker-setup-local docker-producer docker-runner docker-all

help:
	@echo "ETL-Project-3 commands"
	@echo "  make up               - start docker local stack (app + localstack)"
	@echo "  make down             - stop docker stack and remove volumes"
	@echo "  make logs             - stream docker compose logs"
	@echo "  make docker-setup-local - create localstack resources from app container"
	@echo "  make docker-producer  - publish trip CSVs to kinesis from app container"
	@echo "  make docker-runner    - run lambda simulation + aggregation from app container"
	@echo "  make docker-all       - docker-setup-local + docker-producer + docker-runner"
	@echo "  make install          - install python dependencies"
	@echo "  make setup-local      - create localstack resources (streams, ddb, s3, glue db)"
	@echo "  make local-producer   - publish trip_start and trip_end CSV data to kinesis"
	@echo "  make local-runner     - run local lambda simulation + local aggregation"
	@echo "  make local-all        - setup-local + local-producer + local-runner"
	@echo "  make test             - run unit tests"
	@echo "  make clean            - remove caches and local outputs"

install:
	python3 -m pip install -r requirements.txt

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f

docker-setup-local:
	docker exec etl-project-3-app python3 scripts/setup_local.py --endpoint-url http://etl-project-3-localstack:4566

docker-producer:
	docker exec etl-project-3-app python3 scripts/kinesis_trip_producer.py --endpoint-url http://etl-project-3-localstack:4566

docker-runner:
	docker exec -e AGGREGATION_BUCKET -e COMPLETED_TRIP_STAGING_PREFIX -e AGGREGATION_CHECKPOINT_URI -e AGGREGATION_OUTPUT_PATH -e AGGREGATION_PREFIX -e TOP_ROUTES_OUTPUT_PATH -e TOP_ROUTES_LIMIT etl-project-3-app python3 scripts/local_pipeline_runner.py --endpoint-url http://etl-project-3-localstack:4566 --output-path /workspace/output/aggregations

docker-all: docker-setup-local docker-producer docker-runner

setup-local:
	python3 scripts/setup_local.py

local-producer:
	python3 scripts/kinesis_trip_producer.py

local-runner:
	python3 scripts/local_pipeline_runner.py

local-all: setup-local local-producer local-runner

test:
	python3 -m pytest tests -v

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	rm -rf output/
