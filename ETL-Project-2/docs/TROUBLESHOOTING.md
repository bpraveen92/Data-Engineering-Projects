# Deployment Troubleshooting Guide: Local Docker & AWS EMR Serverless

Issues encountered deploying this PySpark Structured Streaming pipeline — first in Docker (LocalStack + MinIO), then on AWS EMR Serverless. The same `spark_aggregator.py` script runs in both environments via the `--local` flag.

| Component | Local Docker | AWS EMR Serverless |
|-----------|-------------|----------|
| Runtime | Docker Desktop (Mac) | EMR 7.12.0 · Spark 3.5 · Python 3 |
| Kinesis | LocalStack `etl-project-2-localstack:4566` | Real stream `music-streams`, `ap-south-1` |
| S3 | MinIO `etl-project-2-minio:9000` | `<your-s3-bucket>` (recreate — original bucket deleted) |
| Credentials | Fake (`test/test`, `minioadmin/minioadmin`) | IAM role `etl-project-2-emr-execution` |
| Logs | Container stdout / `/tmp/consumer.log` | CloudWatch Logs |

---

## Part 1 — Local Docker Issues

---

### L1 — JVM connector can't see container credentials

**Error**
```
com.amazonaws.services.kinesis.model.AmazonKinesisException:
The security token included in the request is invalid.
```

**Cause** — The Kinesis connector is a JVM library. Docker container env vars (`AWS_ACCESS_KEY_ID`, etc.) are visible to Python/boto3 but are not forwarded into the JVM credential provider chain. The SDK falls through to the EC2 metadata endpoint (`169.254.169.254`), which is unreachable inside a container.

**Fix** — Inject credentials as JVM system properties so `SystemPropertiesCredentialsProvider` picks them up:
```python
# create_spark_session(), use_localstack=True only
jvm_creds = (
    f"-Daws.accessKeyId={os.getenv('AWS_ACCESS_KEY_ID', 'test')} "
    f"-Daws.secretAccessKey={os.getenv('AWS_SECRET_ACCESS_KEY', 'test')}"
)
builder.config("spark.driver.extraJavaOptions", jvm_creds)
builder.config("spark.executor.extraJavaOptions", jvm_creds)
```

**References**
- [AWS SDK for Java — credential provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html)
- [Spark `extraJavaOptions` config](https://spark.apache.org/docs/latest/configuration.html#runtime-environment)

---

### L2 — Connector can't derive region from LocalStack URL

**Error (v1.4.2)**
```
pyspark.errors.exceptions.captured.StreamingQueryException: [STREAM_FAILED]
Query terminated with exception: Invalid endpoint url received.
Cannot parse region: http://etl-project-2-localstack:4566
```

**Cause** — The connector's `getRegionNameByEndpoint()` utility parses the region from the URL by
splitting on dots and extracting the second segment (expecting `<service>.<region>.<tld>`). It is
called in two places in `KinesisOptions.apply()`:

1. As the fallback for `kinesis.region` — bypassed when you set `kinesis.region` explicitly.
2. As the fallback for `kinesis.kinesisRegion` (added in v1.4.2) — **this is what crashes**,
   because `kinesis.kinesisRegion` is a new separate field and was not set.

The URL `http://etl-project-2-localstack:4566` has only one dot (between `localstack` and `4566`),
so the second-dot lookup returns -1 and the function throws. This bug only appears with connector
v1.4.2+ — earlier versions had no `kinesisRegion` field.

**Fix** — Set both `kinesis.region` **and** `kinesis.kinesisRegion` explicitly in the LocalStack
code path:
```python
if use_localstack:
    options["kinesis.endpointUrl"]  = localstack_endpoint
    options["kinesis.kinesisRegion"] = region  # bypass URL-parsing fallback in v1.4.2
```

`kinesis.region` and `kinesis.kinesisRegion` accept the same value (e.g. `"ap-south-1"`). Setting
both means `KinesisOptions.apply()` never falls back to `getRegionNameByEndpoint()` for either field.

**References**
- [Kinesis connector config reference](https://github.com/awslabs/spark-sql-kinesis-connector#kinesis-source-configuration)
- [`KinesisOptions.scala` — `kinesisRegion` field](https://github.com/awslabs/spark-sql-kinesis-connector/blob/main/src/main/scala/org/apache/spark/sql/connector/kinesis/KinesisOptions.scala)
- [LocalStack Kinesis docs](https://docs.localstack.cloud/user-guide/aws/kinesis/)

---

### L3 — `NoSuchBucket` on startup

**Error**
```
com.amazonaws.services.s3.model.AmazonS3Exception: NoSuchBucket (404)
```

**Cause** — MinIO doesn't auto-create buckets. `make down` runs `docker compose down -v`, wiping the `minio-storage` named volume — every fresh `make up` starts with no buckets.

**Fix** — Before each run after `make up`:
1. Open `http://localhost:9001` -> create `etl-data`, upload `songs.csv` and `users.csv`
2. Create `aggregations` (leave empty — Spark writes here)

**References**
- [MinIO bucket management](https://min.io/docs/minio/linux/administration/object-management.html)

---

### L4 — `Access Denied` (403) from MinIO

**Error**
```
com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (403)
```

**Cause** — S3A defaults to virtual-hosted-style addressing (`http://bucket.host:port/key`). MinIO requires path-style (`http://host:port/bucket/key`). The subdomain DNS lookup fails and surfaces as a 403.

**Fix**
```python
builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
```

**References**
- [Hadoop S3A — path style access](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Changing_the_Request_Signing_Algorithm)
- [MinIO S3 compatibility](https://min.io/docs/minio/linux/developers/python/API.html)

---

### L5 — Producer fails with `localhost:4566` inside container

**Error**
```
botocore.exceptions.EndpointConnectionError:
    Could not connect to the endpoint URL: "http://localhost:4566/"
```

**Cause** — Inside a Docker container `localhost` resolves to the container itself, not the Mac host or any sibling container.

**Fix** — Keep the two values separate:
```yaml
# docker-compose.yml — used inside the container
- KINESIS_ENDPOINT=http://etl-project-2-localstack:4566
```
```properties
# .env — used when running scripts directly on the Mac host
KINESIS_ENDPOINT=http://localhost:4566
```

**References**
- [Docker bridge networking and container DNS](https://docs.docker.com/network/drivers/bridge/)

---

### Part 1 Summary

| # | Error | Cause | Fix |
|---|-------|-------|-----|
| L1 | `UnrecognizedClientException` | JVM doesn't inherit container env var credentials | Inject as `-Daws.accessKeyId` JVM system properties |
| L2 | `Invalid endpoint url received. Cannot parse region` | v1.4.2 `kinesisRegion` field also calls `getRegionNameByEndpoint()` as fallback | Set `kinesis.kinesisRegion` explicitly alongside `kinesis.region` in LocalStack path |
| L3 | `NoSuchBucket` (404) | MinIO doesn't auto-create; `make down` wipes the volume | Manually create `etl-project-2-data` bucket before each run |
| L4 | `Access Denied` (403) from MinIO | S3A virtual-hosted-style addressing; MinIO needs path-style | `spark.hadoop.fs.s3a.path.style.access = true` |
| L5 | `EndpointConnectionError` to `localhost:4566` | `localhost` inside a container is the container itself | Container hostname in `docker-compose.yml`; `localhost` only in `.env` |

---

## Part 2 — AWS EMR Serverless Issues

Each EMR Serverless run takes ~30–60 seconds to provision workers before any Spark code runs. Below are the issues encountered in the order they occurred.

---

### G1 — `SystemExit: 2` at startup (argparse crash)

**Error**
```
Error from Python: SystemExit: 2
```

**Cause** — When running on managed runtimes (Glue, EMR Serverless), additional arguments may be injected into `sys.argv`. `parser.parse_args()` raises an error on any unrecognised argument and calls `sys.exit(2)` before any Spark code runs.

**Fix**
```python
# parse_known_args() silently ignores any injected arguments
args, unknown = parser.parse_known_args()
if unknown:
    logger.warning(f"Ignoring unrecognized arguments (likely runtime internals): {unknown}")
```

**References**
- [AWS Glue — passing arguments to jobs](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html)
- [Python `argparse.parse_known_args`](https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.parse_known_args)

---

### G2 — `kinesis.endpointUrl is not specified`

**Error**
```
java.lang.IllegalArgumentException: kinesis.endpointUrl is not specified
```

**Cause** — The awslabs connector requires `kinesis.endpointUrl` explicitly — there is no automatic fallback to the public AWS endpoint, even when running on EMR with a VPC Interface Endpoint.

**Fix**
```python
options = {
    "kinesis.endpointUrl": f"https://kinesis.{region}.amazonaws.com",
    ...
}
# LocalStack overrides this:
if use_localstack:
    options["kinesis.endpointUrl"] = localstack_endpoint
```

On EMR Serverless with a Kinesis VPC Interface Endpoint configured and private DNS enabled, the hostname `kinesis.{region}.amazonaws.com` resolves to the VPC endpoint's private IP automatically — no special URL needed.

**References**
- [spark-sql-kinesis-connector — source config options](https://github.com/awslabs/spark-sql-kinesis-connector#kinesis-source-configuration)
- [AWS PrivateLink for Kinesis](https://docs.aws.amazon.com/streams/latest/dev/vpc.html)

---

### G3 — `ClassNotFoundException: aws-kinesis.DefaultSource`

**Error**
```
java.lang.ClassNotFoundException: Failed to find data source: aws-kinesis
```

**Cause** — The awslabs connector jar is bundled on the EMR 7.1.0+ runtime image, but the `"aws-kinesis"` ServiceLoader entry is only registered when the jar is explicitly placed on the classpath via `--jars`. Referencing it from an S3 path alone does not trigger ServiceLoader registration for PySpark.

**Fix** — Reference the connector's local path on the EMR image via `--jars`:
```
--jars /usr/share/aws/kinesis/spark-sql-kinesis/lib/spark-streaming-sql-kinesis-connector.jar
```

This is the `--jars` entry in `make emr-start` and is already set correctly.

**References**
- [EMR Serverless — Kinesis connector](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-structured-streaming.html)

---

### G4 — `dotenv` import crash (pre-emptive)

**Cause** — `python-dotenv` is in `requirements-dev.txt` for local use but is not available in EMR Serverless or Glue Python runtimes.

**Fix**
```python
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Not available in EMR runtime — env vars come from job parameters
```

---

### G5 — `Trigger.AvailableNow` unsupported — job succeeds but writes zero output

**Symptom** — EMR Serverless job exits with state `SUCCESS` but the aggregations S3 prefix is empty (or unchanged from the previous run).

**Cause** — The awslabs connector does not implement `reportLatestOffset()`, which is required by Spark's `AvailableNow` executor to establish a read fence. Without the fence, Spark decides there is nothing to process and exits immediately — no executor partition readers ever run.

Because partition readers never run, the connector's second checkpoint layer (`shard-source/`) is never written. On the next job, `getBatchShardsInfo()` finds no `shard-source/` entries and falls back to the Spark `offsets/` log, which stores the initial `TRIM_HORIZON` position with an empty `iteratorPosition`. The job replays from the beginning of the shard, and all records are dropped as late by the watermark advanced in prior runs.

**Confirmed evidence (job `00g3v9llj3al201v`, 7 March 2026)**

Offset checkpoint at `s3://.../checkpoints/hourly_streams/offsets/1`:
```json
{"metadata":{"streamName":"music-streams","batchId":"0"},
 "shardId-000000000000":{
   "subSequenceNumber":"-1","isLast":"true",
   "iteratorType":"TRIM_HORIZON","iteratorPosition":""
 }}
```
Watermark from job 7 (last successful run): `batchWatermarkMs: 1772885321287` = 2026-03-07T12:09 UTC.
Any record with `event_timestamp` before that is silently dropped.

**Root cause (GitHub Issue #79, open as of March 2026)**

`Trigger.AvailableNow` requires `SupportsAdmissionControl.reportLatestOffset()` to be implemented.
`KinesisV2MicrobatchStream` implements `latestOffset(start, readLimit)` but **not** `reportLatestOffset()`.
This is an upstream connector limitation, not a configuration problem.

**Fix** — Switch to `--trigger-mode continuous` on **AWS Glue Streaming** (primary deployment).
Glue keeps the job alive indefinitely, so `availableNow` is never invoked. Checkpoint resume works
correctly because state is held in memory across micro-batches without serialising the shard iterator
between separate job processes.

EMR Serverless remains viable if Issue #79 is resolved upstream. See `docs/AWS_PRODUCTION_DEPLOYMENT.md`
for the full root cause analysis and what to verify before re-enabling the scheduled architecture.

**References**
- [awslabs/spark-sql-kinesis-connector Issue #79](https://github.com/awslabs/spark-sql-kinesis-connector/issues/79)
- [awslabs/spark-sql-kinesis-connector Issue #34](https://github.com/awslabs/spark-sql-kinesis-connector/issues/34) (related: assertion crash on restart when no new records exist)
- `docs/AWS_PRODUCTION_DEPLOYMENT.md` — full root cause analysis and verification checklist

---

### Part 2 Summary

| # | Error | Cause | Fix |
|---|-------|-------|-----|
| G1 | `SystemExit: 2` | `parse_args()` rejects runtime-injected flags | Switch to `parse_known_args()` |
| G2 | `kinesis.endpointUrl is not specified` | Connector has no default endpoint fallback | Set `kinesis.endpointUrl` explicitly, even for real AWS |
| G3 | `ClassNotFoundException: aws-kinesis.DefaultSource` | ServiceLoader not triggered without `--jars` local path (EMR only) | Pass local jar path via `--jars /usr/share/aws/kinesis/...`; not needed on Glue (built-in) |
| G4 | `ModuleNotFoundError: dotenv` | `python-dotenv` absent from EMR/Glue runtime | Wrap import in `try/except ImportError` |
| G5 | Job SUCCESS but zero output | `availableNow` unsupported (Issue #79); checkpoint stores TRIM_HORIZON | Use Glue Streaming `--trigger-mode continuous` as primary deployment |

---

## Key Lessons

- **JVM != Python process.** In Docker, credentials set as env vars are not visible to the Kinesis connector's JVM. Use `-D` JVM system properties via `spark.driver.extraJavaOptions`.
- **`kinesis.region` is always required.** The connector has no automatic region detection — set it explicitly for both LocalStack and real AWS.
- **`kinesis.kinesisRegion` must also be set for LocalStack (v1.4.2+).** v1.4.2 introduced a second region field. If not explicitly set, it falls back to `getRegionNameByEndpoint()` which crashes on bare LocalStack hostnames. Set both `kinesis.region` and `kinesis.kinesisRegion` to the same value in the LocalStack code path.
- **MinIO requires path-style access.** `spark.hadoop.fs.s3a.path.style.access = true` is non-negotiable for S3A → MinIO.
- **`localhost` means different things inside vs outside Docker.** Keep `.env` (host-side) and `docker-compose.yml` env values (container-side) separate.
- **Use `parse_known_args()` on managed runtimes.** EMR/Glue may inject their own `sys.argv` entries; `parse_args()` will crash on them.
- **`kinesis.endpointUrl` has no default.** Always set it explicitly — even for real AWS.
- **ServiceLoader requires `--jars` with a local path on EMR.** The `"aws-kinesis"` format string resolves only when the connector jar is on the Spark classpath via `--jars` with the local image path. Not required on Glue — the connector is built into the Glue 4.0 runtime.
- **All checkpoint state must be in S3.** Never use `/tmp/` for Spark checkpoint or connector metadata — EMR workers are ephemeral and `/tmp/` is wiped on each run.
- **`Trigger.AvailableNow` is unsupported** by the awslabs connector (Issue #79, open March 2026). `reportLatestOffset()` is not implemented, so the drain-and-exit architecture is broken. Use Glue Streaming (`continuous` trigger) as the primary deployment target until this is resolved upstream.

---

## Current Working Configuration (Glue Streaming — primary)

```python
options = {
    "kinesis.streamName":       kinesis_stream,
    "kinesis.startingPosition": "TRIM_HORIZON",
    "kinesis.region":           region,
    "kinesis.endpointUrl":      f"https://kinesis.{region}.amazonaws.com",
    "kinesis.consumerType":     "GetRecords",
}

if use_localstack:
    options["kinesis.endpointUrl"]   = localstack_endpoint
    # v1.4.2: kinesisRegion is a second region field that also calls
    # getRegionNameByEndpoint() as its fallback — set it explicitly to
    # prevent it from trying to parse the region from the LocalStack URL.
    options["kinesis.kinesisRegion"] = region
```

Deployment: **AWS Glue Streaming** with `--trigger-mode continuous` (default). The job runs
indefinitely, polling Kinesis every few seconds. No `availableNow`, no checkpoint resume across
job boundaries, no connector bugs to work around.

A single `readStream` fans out to all 3 `writeStream` queries — the awslabs connector tracks
per-query shard offsets inside each query's own checkpoint directory, so no per-query Kinesis read
isolation is needed:
```python
raw      = read_kinesis(spark, kinesis_stream, region, use_localstack)
enriched = enrich_events(parse_events(raw), songs, users)

# All 3 writeStream calls share the same `enriched` DataFrame:
hourly     = compute_hourly_streams(enriched, ...)
top_tracks = compute_top_tracks(enriched, ...)
country    = compute_country_metrics(enriched, ...)
```
