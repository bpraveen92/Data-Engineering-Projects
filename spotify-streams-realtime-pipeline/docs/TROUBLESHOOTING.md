# Troubleshooting Guide

Issues encountered deploying this pipeline — first in Docker (LocalStack + MinIO), then on AWS Glue and EMR Serverless.

| | Local Docker | AWS |
|---|---|---|
| Kinesis | `etl-project-2-localstack:4566` | `music-streams`, `ap-south-2` |
| S3 | `etl-project-2-minio:9000` | `pravbala-data-engineering-projects` |
| Credentials | `test/test`, `minioadmin/minioadmin` | IAM role |
| Logs | Container stdout / `/tmp/consumer.log` | CloudWatch |

---

## Part 1 — Local Docker

---

### L1 — JVM connector can't see container credentials

**Error**
```
com.amazonaws.services.kinesis.model.AmazonKinesisException:
The security token included in the request is invalid.
```

**Cause** — Container env vars are visible to Python/boto3 but not to the JVM credential provider chain. The SDK falls through to the EC2 metadata endpoint (`169.254.169.254`), which is unreachable in a container.

**Fix** — Inject credentials as JVM system properties:
```python
jvm_creds = (
    f"-Daws.accessKeyId={os.getenv('AWS_ACCESS_KEY_ID', 'test')} "
    f"-Daws.secretAccessKey={os.getenv('AWS_SECRET_ACCESS_KEY', 'test')}"
)
builder.config("spark.driver.extraJavaOptions",   jvm_creds)
builder.config("spark.executor.extraJavaOptions", jvm_creds)
```

---

### L2 — Connector can't derive region from LocalStack URL (v1.4.2)

**Error**
```
StreamingQueryException: Invalid endpoint url received. Cannot parse region:
http://etl-project-2-localstack:4566
```

**Cause** — v1.4.2 introduced a new `kinesis.kinesisRegion` field. Its fallback calls `getRegionNameByEndpoint()`, which parses the region by splitting on dots. The LocalStack hostname has only one dot, so the lookup returns -1 and throws.

**Fix** — Set `kinesis.kinesisRegion` explicitly in the LocalStack code path:
```python
if use_localstack:
    options["kinesis.endpointUrl"]   = localstack_endpoint
    options["kinesis.kinesisRegion"] = region  # prevent URL-parsing fallback
```

---

### L3 — `NoSuchBucket` on startup

**Error**
```
com.amazonaws.services.s3.model.AmazonS3Exception: NoSuchBucket (404)
```

**Cause** — MinIO doesn't auto-create buckets. `make down` wipes the `minio-storage` volume.

**Fix** — After every `make up`, open `http://localhost:9001` and create `etl-project-2-data`. Upload `songs.csv` and `users.csv`.

---

### L4 — Access Denied (403) from MinIO

**Error**
```
com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (403)
```

**Cause** — S3A defaults to virtual-hosted-style addressing (`http://bucket.host:port`). MinIO requires path-style.

**Fix**
```python
builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
```

---

### L5 — Producer fails with `localhost:4566` inside container

**Error**
```
botocore.exceptions.EndpointConnectionError: Could not connect to "http://localhost:4566/"
```

**Cause** — Inside a Docker container, `localhost` resolves to the container itself, not the Mac host.

**Fix** — Keep the two values separate:
```yaml
# docker-compose.yml (inside container)
- KINESIS_ENDPOINT=http://etl-project-2-localstack:4566
```
```properties
# .env (Mac host)
KINESIS_ENDPOINT=http://localhost:4566
```

---

### Part 1 Summary

| # | Error | Cause | Fix |
|---|-------|-------|-----|
| L1 | `UnrecognizedClientException` | JVM doesn't inherit container env vars | `-Daws.accessKeyId` JVM system properties |
| L2 | `Cannot parse region` | v1.4.2 `kinesisRegion` field calls URL-parser as fallback | Set `kinesis.kinesisRegion` explicitly |
| L3 | `NoSuchBucket` | MinIO doesn't auto-create; `make down` wipes volume | Create `etl-project-2-data` before each run |
| L4 | `Access Denied` from MinIO | S3A virtual-hosted-style; MinIO needs path-style | `spark.hadoop.fs.s3a.path.style.access = true` |
| L5 | `EndpointConnectionError` to `localhost:4566` | `localhost` inside container is the container | Container hostname in `docker-compose.yml` |

---

## Part 2 — AWS Glue

---

### G1 — `SystemExit: 2` at startup

**Error**
```
Error from Python: SystemExit: 2
```

**Cause** — Glue injects its own args (`--JOB_NAME`, `--JOB_RUN_ID`, `--TempDir`, etc.) into `sys.argv`. `parse_args()` raises on any unrecognised argument.

**Fix**
```python
args, unknown = parser.parse_known_args()
```

---

### G2 — `kinesis.endpointUrl is not specified`

**Error**
```
java.lang.IllegalArgumentException: kinesis.endpointUrl is not specified
```

**Cause** — The connector has no automatic fallback to the regional AWS endpoint.

**Fix**
```python
options["kinesis.endpointUrl"] = f"https://kinesis.{region}.amazonaws.com"
# LocalStack overrides:
if use_localstack:
    options["kinesis.endpointUrl"] = localstack_endpoint
```

---

### G3 — `ModuleNotFoundError: dotenv`

**Cause** — `python-dotenv` is in `requirements-dev.txt` for local use but is not in the Glue runtime.

**Fix**
```python
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
```

---

### Part 2 Summary

| # | Error | Cause | Fix |
|---|-------|-------|-----|
| G1 | `SystemExit: 2` | Glue-injected args crash `parse_args()` | `parse_known_args()` |
| G2 | `kinesis.endpointUrl not specified` | No default endpoint fallback | Set explicitly |
| G3 | `ModuleNotFoundError: dotenv` | `python-dotenv` absent from Glue runtime | `try/except ImportError` |

---

## Part 3 — EMR Serverless

---

### E1 — `ClassNotFoundException: aws-kinesis.DefaultSource`

**Error**
```
java.lang.ClassNotFoundException: Failed to find data source: aws-kinesis
```

**Cause** — The connector JAR is bundled in the EMR 7.1.0+ runtime image but the `"aws-kinesis"` ServiceLoader entry is only registered when the JAR is explicitly on the Spark classpath via `--jars`.

**Fix** — Pass the local image path:
```
--jars /usr/share/aws/kinesis/spark-sql-kinesis/lib/spark-streaming-sql-kinesis-connector.jar
```
Already set in `make emr-start`. Not needed on Glue 4.0 — connector is built in.

---

### E2 — Job SUCCESS but zero output (`Trigger.AvailableNow` unsupported)

**Symptom** — EMR job exits `SUCCESS` but `aggregations/` in S3 is empty.

**Cause** — `Trigger.AvailableNow` requires `reportLatestOffset()` to give Spark a read fence. The connector implements `latestOffset()` but not `reportLatestOffset()`. Without the fence, Spark exits immediately — no partition readers run. The `shard-source/` checkpoint layer is never written, so the next job falls back to `TRIM_HORIZON`, replays everything from the start of the shard, and the watermark drops all records as late.

**Confirmed (job `00g3v9llj3al201v`, 7 March 2026):**
```json
{"shardId-000000000000": {"iteratorType": "TRIM_HORIZON", "iteratorPosition": ""}}
```

**Fix** — Use AWS Glue Streaming (`--trigger-mode continuous`) as the primary deployment. [Issue #79](https://github.com/awslabs/spark-sql-kinesis-connector/issues/79) tracks the upstream fix.

---

### Part 3 Summary

| # | Error | Cause | Fix |
|---|-------|-------|-----|
| E1 | `ClassNotFoundException: aws-kinesis` | ServiceLoader not triggered without `--jars` local path | `--jars /usr/share/aws/kinesis/...` |
| E2 | Job SUCCESS, zero output | `availableNow` unsupported (Issue #79) | Use Glue Streaming `continuous` trigger |
