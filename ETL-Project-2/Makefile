.PHONY: help up down logs producer consumer aws-producer glue-start glue-status test clean

up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f

producer:
	docker exec -d etl-project-2-spark sh -c 'python3 /opt/spark/scripts/kinesis_stream_producer.py \
		--stream-name music-streams \
		--batch-size 20 \
		--interval-seconds 5.0 \
		--local > /tmp/producer.log 2>&1'
	@echo "Producer started in background. Logs: docker exec etl-project-2-spark tail -f /tmp/producer.log"

aws-producer:
	python3 scripts/kinesis_stream_producer.py \
		--stream-name music-streams \
		--region ap-south-2 \
		--batch-size 20 \
		--interval-seconds 5.0

consumer:
	docker exec -d etl-project-2-spark sh -c '/opt/spark/bin/spark-submit \
		--jars /opt/spark/jars/spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.565.jar \
		/opt/spark/scripts/spark_aggregator.py \
		--kinesis-stream music-streams \
		--songs-path /opt/spark/sample_data_initial_load/songs.csv \
		--users-path /opt/spark/sample_data_initial_load/users.csv \
		--output-path s3a://etl-project-2-data/aggregations \
		--checkpoint-path s3a://etl-project-2-data/checkpoints \
		--window-minutes 5 \
		--watermark-minutes 1 \
		--local > /tmp/consumer.log 2>&1'
	@echo "Consumer started in background. Logs: docker exec etl-project-2-spark tail -f /tmp/consumer.log"

glue-start:
	aws glue start-job-run \
		--job-name etl-project-2-streaming \
		--region ap-south-2 \
		--no-cli-pager
	@echo "Glue job started. Monitor: https://ap-south-2.console.aws.amazon.com/glue/home?region=ap-south-2#/monitoring"

glue-status:
	aws glue get-job-runs \
		--job-name etl-project-2-streaming \
		--region ap-south-2 \
		--no-cli-pager \
		--query 'JobRuns[0].{State:JobRunState,Started:StartedOn,Duration:ExecutionTime,Error:ErrorMessage}'

test:
	pytest tests/ -v

clean:
	docker compose down -v --rmi local
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
