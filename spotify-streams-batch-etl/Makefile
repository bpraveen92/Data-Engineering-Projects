.PHONY: up down logs test inflate-sample aws-bootstrap upload-sample

up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f airflow-scheduler airflow-webserver

test:
	pytest -q

inflate-sample:
	python3 scripts/inflate_sample_data.py

aws-bootstrap:
	./scripts/aws_bootstrap.sh

upload-sample:
	python3 scripts/upload_sample_data_to_s3.py

