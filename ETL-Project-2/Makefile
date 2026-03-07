.PHONY: help up down logs producer consumer aws-producer emr-upload emr-start emr-status emr-list-apps glue-start glue-status test clean

help:
	@echo ""
	@echo "Local development:"
	@echo "  make up              Start all 3 Docker containers (Spark, LocalStack, MinIO)"
	@echo "  make down            Stop containers and remove volumes"
	@echo "  make logs            Tail docker-compose logs"
	@echo "  make producer        Run Kinesis producer inside Spark container (LocalStack)"
	@echo "  make consumer        Run Spark aggregator inside Spark container (LocalStack + MinIO)"
	@echo ""
	@echo "AWS production — producer:"
	@echo "  make aws-producer    Run Kinesis producer against real AWS Kinesis"
	@echo ""
	@echo "AWS production — EMR Serverless (primary deployment):"
	@echo "  make emr-upload      Upload jars, script and reference data to S3"
	@echo "  make emr-list-apps   List EMR Serverless applications"
	@echo "  make emr-start       Submit a job run (requires EMR_APP_ID and EMR_EXECUTION_ROLE_ARN env vars)"
	@echo "  make emr-status      Check latest job run status (requires EMR_APP_ID)"
	@echo ""
	@echo "AWS production — Glue Streaming (alternative deployment):"
	@echo "  make glue-start      Start the Glue streaming job"
	@echo "  make glue-status     Check latest Glue job run status"
	@echo ""
	@echo "Testing:"
	@echo "  make test            Run unit tests"
	@echo "  make clean           Remove containers, volumes, and cache"
	@echo ""

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
		--region ap-south-1 \
		--batch-size 20 \
		--interval-seconds 5.0 \
		> /tmp/aws-producer.log 2>&1 &
	@echo "Producer running in background. Logs: tail -f /tmp/aws-producer.log"
	@echo "Stop with: pkill -f kinesis_stream_producer.py"

consumer:
	@echo "NOTE: The awslabs Kinesis connector is bundled only on the EMR 7.1.0+ runtime image."
	@echo "      It is not published to Maven Central and cannot be downloaded for local Docker use."
	@echo "      For local development, point the consumer at a LocalStack Kinesis endpoint and"
	@echo "      ensure the connector jar is available inside the Spark container."
	docker exec -d etl-project-2-spark sh -c '/opt/spark/bin/spark-submit \
		--jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.565.jar \
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

emr-upload:
	@echo "Uploading jars to S3 (S3A support jars only — Kinesis connector is bundled in EMR 7.1.0+)..."
	aws s3 cp jars/hadoop-aws-3.3.4.jar s3://pravbala-de-etl-project-emr/Project-2/jars/ --region ap-south-1
	aws s3 cp jars/aws-java-sdk-bundle-1.12.565.jar s3://pravbala-de-etl-project-emr/Project-2/jars/ --region ap-south-1
	@echo "Uploading script to S3..."
	aws s3 cp scripts/spark_aggregator.py s3://pravbala-de-etl-project-emr/Project-2/scripts/ --region ap-south-1
	@echo "Uploading reference data to S3..."
	aws s3 cp sample_data_initial_load/songs.csv s3://pravbala-de-etl-project-emr/Project-2/sample_data_initial_load/ --region ap-south-1
	aws s3 cp sample_data_initial_load/users.csv s3://pravbala-de-etl-project-emr/Project-2/sample_data_initial_load/ --region ap-south-1
	@echo "Upload complete."

emr-start:
	aws emr-serverless start-job-run \
		--application-id $${EMR_APP_ID} \
		--execution-role-arn $${EMR_EXECUTION_ROLE_ARN} \
		--region ap-south-1 \
		--no-cli-pager \
		--job-driver '{"sparkSubmit":{"entryPoint":"s3://pravbala-de-etl-project-emr/Project-2/scripts/spark_aggregator.py","entryPointArguments":["--kinesis-stream","music-streams","--songs-path","s3://pravbala-de-etl-project-emr/Project-2/sample_data_initial_load/songs.csv","--users-path","s3://pravbala-de-etl-project-emr/Project-2/sample_data_initial_load/users.csv","--output-path","s3://pravbala-de-etl-project-emr/Project-2/aggregations","--checkpoint-path","s3://pravbala-de-etl-project-emr/Project-2/checkpoints","--region","ap-south-1","--window-minutes","5","--watermark-minutes","1","--trigger-mode","available_now"],"sparkSubmitParameters":"--jars /usr/share/aws/kinesis/spark-sql-kinesis/lib/spark-streaming-sql-kinesis-connector.jar,s3://pravbala-de-etl-project-emr/Project-2/jars/hadoop-aws-3.3.4.jar,s3://pravbala-de-etl-project-emr/Project-2/jars/aws-java-sdk-bundle-1.12.565.jar --conf spark.sql.shuffle.partitions=4"}}'
	@echo "EMR Serverless job submitted. Check status: make emr-status"

emr-status:
	aws emr-serverless list-job-runs \
		--application-id $${EMR_APP_ID} \
		--region ap-south-1 \
		--no-cli-pager \
		--query 'jobRuns[0].{State:state,Created:createdAt,Updated:updatedAt,Id:jobRunId}'

emr-list-apps:
	aws emr-serverless list-applications \
		--region ap-south-1 \
		--no-cli-pager \
		--query 'applications[*].{Id:id,Name:name,State:state,Type:type}'

glue-start:
	aws glue start-job-run \
		--job-name etl-project-2-streaming \
		--region ap-south-1 \
		--no-cli-pager
	@echo "Glue job started. Monitor: https://ap-south-1.console.aws.amazon.com/glue/home?region=ap-south-1#/monitoring"

glue-status:
	aws glue get-job-runs \
		--job-name etl-project-2-streaming \
		--region ap-south-1 \
		--no-cli-pager \
		--query 'JobRuns[0].{State:JobRunState,Started:StartedOn,Duration:ExecutionTime,Error:ErrorMessage}'

test:
	pytest tests/ -v

clean:
	docker compose down -v --rmi local
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
