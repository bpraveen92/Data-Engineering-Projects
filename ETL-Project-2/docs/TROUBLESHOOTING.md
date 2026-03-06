# Deployment Troubleshooting Guide: Local Docker & AWS Glue

Issues encountered deploying this PySpark Structured Streaming pipeline — first in Docker (LocalStack + MinIO), then on AWS Glue 4.0. The same `spark_aggregator.py` script runs in both environments via the `--local` flag.

| Component | Local Docker | AWS Glue |
|-----------|-------------|----------|
| Runtime | Docker Desktop (Mac) | Glue 4.0 · Spark 3.3.0-amzn-1 · Python 3.7 |
| Kinesis | LocalStack `etl-project-2-localstack:4566` | Real stream `music-streams`, `ap-south-2` |
| S3 | MinIO `etl-project-2-minio:9000` | `pravbala-data-engineering-projects` bucket |
| Credentials | Fake (`test/test`, `minioadmin/minioadmin`) | IAM role `etl-project-2` |
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

**Error**
```
com.amazonaws.SdkClientException: Invalid region name:
    http://etl-project-2-localstack:4566
```

**Cause** — The connector has two separate region code paths: `kinesis.region` (Spark DataSource layer) and `kinesis.kinesisRegion` (AWS SDK layer). Without `kinesis.kinesisRegion`, the SDK calls `getRegionNameByEndpoint()` to parse the region from the URL — works for AWS URLs, fails for a LocalStack hostname.

**Fix** — Set both keys explicitly:
```python
options = {
    "kinesis.region": region,        # Spark DataSource layer
    "kinesis.kinesisRegion": region, # AWS SDK layer inside the connector
    ...
}
```

**References**
- [Kinesis connector config reference](https://github.com/awslabs/spark-sql-kinesis-connector#kinesis-source-configuration)
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
| L2 | `SdkClientException` — invalid region name | Missing `kinesis.kinesisRegion`; SDK tries to parse region from LocalStack URL | Set both `kinesis.region` and `kinesis.kinesisRegion` |
| L3 | `NoSuchBucket` (404) | MinIO doesn't auto-create; `make down` wipes the volume | Manually create `etl-data` and `aggregations` before each run |
| L4 | `Access Denied` (403) from MinIO | S3A virtual-hosted-style addressing; MinIO needs path-style | `spark.hadoop.fs.s3a.path.style.access = true` |
| L5 | `EndpointConnectionError` to `localhost:4566` | `localhost` inside a container is the container itself | Container hostname in `docker-compose.yml`; `localhost` only in `.env` |

---

## Part 2 — AWS Glue Issues

Each Glue run takes ~90 seconds to provision a worker before any Python code runs. Below are all 6 failures in the order they occurred.

---

### G1 — `SystemExit: 2` at startup (argparse crash)

**Error**
```
Error from Python: SystemExit: 2
```

**Cause** — Glue injects its own arguments into `sys.argv` at runtime (`--JOB_NAME`, `--TempDir`, `--job-bookmark-option`, etc.). `parser.parse_args()` raises an error on any unrecognised argument and calls `sys.exit(2)` before any Spark code runs.

**Fix**
```python
# parse_known_args() silently ignores Glue-injected arguments
args, unknown = parser.parse_known_args()
if unknown:
    logger.warning(f"Ignoring unrecognized arguments (likely Glue internals): {unknown}")
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

**Cause** — Connector v1.0.0 has no default fallback to the public Kinesis endpoint. `kinesis.endpointUrl` is required even when targeting real AWS.

**Fix**
```python
options = {
    "kinesis.endpointUrl": f"https://kinesis.{region}.amazonaws.com",
    ...
}
# LocalStack overrides this:
if use_localstack:
    options.update({"kinesis.endpointUrl": localstack_endpoint, ...})
```

**References**
- [spark-sql-kinesis-connector — source config options](https://github.com/awslabs/spark-sql-kinesis-connector#kinesis-source-configuration)

---

### G3 — `No AbstractFileSystem configured for scheme: s3`

**Error**
```
org.apache.hadoop.fs.UnsupportedFileSystemException:
fs.AbstractFileSystem.s3.impl=null: No AbstractFileSystem configured for scheme: s3
    at ...kinesis.metadata.HDFSMetadataCommitter.<init>
```

**Cause** — When `kinesis.metadataPath` isn't set, `HDFSMetadataCommitter` derives its own path using the bare `s3://` scheme. Glue 4.0 has `s3a://` fully configured but the legacy `s3://` AbstractFileSystem is not, so the lookup fails immediately.

**Fix**
```python
options = {
    "kinesis.metadataPath": f"s3a://your-bucket/checkpoints/kinesis-metadata-{suffix}",
    ...
}
```

**In this project** — `read_kinesis()` derives `metadataPath` automatically:
- **Local** (`--local` flag): `{checkpoint_path}/kinesis-metadata-{suffix}` → e.g. `s3a://etl-project-2-data/checkpoints/kinesis-metadata-hourly`
- **Production (Glue)**: hardcoded to `s3a://pravbala-data-engineering-projects/Project-2/checkpoints/kinesis-metadata-{suffix}`

`checkpoint_path` is the value passed via `--checkpoint-path` (see `make consumer` / Glue job arguments).

**References**
- [Hadoop S3A vs legacy S3 filesystem](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Features)
- [AWS Glue — working with Amazon S3](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-s3-home.html)

---

### G4 — `Unable to fetch committed metadata from previous batch id 0` (concurrent queries)

**Error**
```
java.lang.IllegalStateException: Unable to fetch committed metadata
from previous batch id 0. Some data may have been missed.
```

**Cause** — Three streaming queries were started from the same shared `raw` DataFrame. Each `writeStream.start()` creates its own `KinesisV2MicrobatchStream` instance, all sharing the same `kinesis.metadataPath`. Each query's batch 0 write overwrites the others', so the next micro-batch can't find committed offsets.

**Fix** — Give each query its own independent Kinesis read with a unique `metadataPath`:
```python
def make_enriched(suffix):
    raw = read_kinesis(spark, kinesis_stream, region,
                       use_localstack, metadata_suffix=suffix,
                       checkpoint_path=checkpoint_path)
    return enrich_events(parse_events(raw), songs, users)

hourly     = compute_hourly_streams(make_enriched("hourly"), ...)
top_tracks = compute_top_tracks(make_enriched("top_tracks"), ...)
country    = compute_country_metrics(make_enriched("country"), ...)
```

`checkpoint_path` is passed into `read_kinesis()` so the derived `metadataPath` stays co-located with the Spark checkpoints under the same root (local: `s3a://etl-project-2-data/checkpoints/`, production: `s3a://pravbala-data-engineering-projects/Project-2/checkpoints/`).

**References**
- [Spark Structured Streaming — multiple queries on the same source](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [awslabs/spark-sql-kinesis-connector — issues](https://github.com/awslabs/spark-sql-kinesis-connector/issues)

---

### G5 — Same crash on every restart (checkpoint / metadata desync)

**Error**
```
java.lang.IllegalStateException: Unable to fetch committed metadata
from previous batch id 0. Some data may have been missed.
```

**Cause** — `kinesis.metadataPath` was set to `/tmp/`. Glue allocates a fresh worker on each restart with an empty `/tmp/`. Spark's S3 checkpoint shows batch 0 completed; the connector's `/tmp/` metadata has no record of it — they're out of sync.

**Fix** — Move `kinesis.metadataPath` to `s3a://` so both Spark checkpoint and connector metadata persist across restarts:
```python
# Before
"kinesis.metadataPath": f"/tmp/kinesis-metadata-{suffix}",

# After
"kinesis.metadataPath": f"s3a://your-bucket/checkpoints/kinesis-metadata-{suffix}",
```

**In this project** — `read_kinesis()` automatically uses `{checkpoint_path}/kinesis-metadata-{suffix}` when running locally. Passing `--checkpoint-path s3a://etl-project-2-data/checkpoints` (local) or the production equivalent ensures both Spark checkpoints and connector metadata land under the same S3 root and survive restarts.

Resulting S3 layout:
```
checkpoints/
|-- hourly_streams/               <- Spark checkpoint
|-- top_tracks_hourly/            <- Spark checkpoint
|-- country_metrics_hourly/       <- Spark checkpoint
|-- kinesis-metadata-hourly/      <- Connector shard metadata
|-- kinesis-metadata-top_tracks/
`-- kinesis-metadata-country/
```

> To start completely fresh: delete the entire `checkpoints/` prefix in S3. This resets both Spark's checkpoint and connector metadata, and the job replays from `TRIM_HORIZON`.

**References**
- [Spark Structured Streaming — recovery semantics](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovery-semantics-after-changes-in-a-streaming-query)

---

### G6 — `dotenv` import crash (pre-emptive)

**Cause** — `python-dotenv` is in `requirements-dev.txt` for local use but is not available in the Glue 4.0 Python runtime.

**Fix**
```python
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Not available in Glue — env vars come from job parameters
```

---

### Part 2 Summary

| # | Error | Cause | Fix |
|---|-------|-------|-----|
| G1 | `SystemExit: 2` | `parse_args()` rejects Glue-injected flags | Switch to `parse_known_args()` |
| G2 | `kinesis.endpointUrl is not specified` | Connector v1.0.0 has no default endpoint | Set `kinesis.endpointUrl` explicitly, even for real AWS |
| G3 | `No AbstractFileSystem for scheme: s3` | `HDFSMetadataCommitter` auto-derives an `s3://` path; not configured in Glue 4.0 | Set `kinesis.metadataPath` to an `s3a://` path |
| G4 | `Unable to fetch committed metadata` (same run) | Three queries share one `metadataPath`; batch 0 writes overwrite each other | Unique `metadataPath` per query via `metadata_suffix` |
| G5 | `Unable to fetch committed metadata` (on restart) | `metadataPath` in `/tmp/` is wiped on new worker allocation | Move `metadataPath` to `s3a://` |
| G6 | `ModuleNotFoundError: dotenv` | `python-dotenv` absent from Glue 4.0 runtime | Wrap import in `try/except ImportError` |

---

## Key Lessons

- **JVM != Python process.** In Docker, credentials set as env vars are not visible to the Kinesis connector's JVM. Use `-D` JVM system properties via `spark.driver.extraJavaOptions`.
- **`kinesis.region` and `kinesis.kinesisRegion` are different.** Two code paths, two config keys — set both, always.
- **MinIO requires path-style access.** `spark.hadoop.fs.s3a.path.style.access = true` is non-negotiable for S3A -> MinIO.
- **`localhost` means different things inside vs outside Docker.** Keep `.env` (host-side) and `docker-compose.yml` env values (container-side) separate.
- **Use `parse_known_args()` on Glue.** Glue always injects its own `sys.argv` entries; `parse_args()` will always crash.
- **Connector v1.0.0 is not zero-config.** `endpointUrl` and `metadataPath` are both required — no defaults.
- **`s3://` != `s3a://` on Glue.** Any JVM Hadoop path config must use `s3a://`.
- **Never mix ephemeral and durable state.** All checkpoint and metadata paths must live in S3 — never `/tmp/` — for a job that restarts.
- **Concurrent queries need full isolation.** Each `writeStream.start()` needs its own Kinesis read with a unique `metadataPath`.

---

## Final Working Configuration

```python
options = {
    "kinesis.streamName":      kinesis_stream,
    "kinesis.initialPosition": "TRIM_HORIZON",
    "kinesis.region":          region,
    "kinesis.kinesisRegion":   region,
    "kinesis.endpointUrl":     f"https://kinesis.{region}.amazonaws.com",
    "kinesis.metadataPath":    f"s3a://your-bucket/checkpoints/kinesis-metadata-{metadata_suffix}",
}

if use_localstack:
    options.update({
        "kinesis.endpointUrl":          localstack_endpoint,
        "kinesis.verifyCertificate":    "false",
        "kinesis.allowUnauthorizedSsl": "true",
    })
```

Pattern for isolated concurrent queries:
```python
def make_enriched(suffix):
    raw = read_kinesis(spark, kinesis_stream, region, use_localstack, metadata_suffix=suffix)
    return enrich_events(parse_events(raw), songs, users)

hourly     = compute_hourly_streams(make_enriched("hourly"), ...)
top_tracks = compute_top_tracks(make_enriched("top_tracks"), ...)
country    = compute_country_metrics(make_enriched("country"), ...)
```
