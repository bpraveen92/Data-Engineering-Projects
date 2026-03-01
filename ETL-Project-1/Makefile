.PHONY: up down logs test lint fmt inflate-sample aws-bootstrap upload-sample

up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f airflow-scheduler airflow-webserver

test:
	pytest -q

lint:
	ruff check dags src transform_jobs tests

fmt:
	ruff format dags src transform_jobs tests

inflate-sample:
	python3 scripts/inflate_sample_data.py

aws-bootstrap:
	./scripts/aws_bootstrap.sh

upload-sample:
	python3 scripts/upload_sample_data_to_s3.py

