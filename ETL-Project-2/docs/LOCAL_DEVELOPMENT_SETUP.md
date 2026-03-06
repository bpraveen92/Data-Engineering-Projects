# Local Development Setup: MinIO & LocalStack

The whole local stack runs in Docker — LocalStack mocks Kinesis, MinIO mocks S3. The same Python code that runs locally connects to real AWS in production; the only thing that changes is the `.env` file.

---

## Local vs Production Infrastructure

In production this pipeline uses AWS Kinesis for streaming and AWS S3 for storage. Locally, LocalStack and MinIO replace both — they expose the same APIs, so no code changes are needed when switching environments.

```
Spark (PySpark job)
    ↓
boto3 / S3A connector
    ↓
    ├── LocalStack  →  mocks AWS Kinesis  (port 4566)
    └── MinIO       →  mocks AWS S3       (port 9000 / 9001 console)
```

---

## LocalStack

LocalStack runs inside the `etl-project-2-localstack` container. It mocks Kinesis (and S3, though I use MinIO for storage).

**Docker Compose config:**
```yaml
localstack:
  image: localstack/localstack:latest
  container_name: etl-project-2-localstack
  environment:
    - SERVICES=kinesis,s3
    - DEBUG=0
    - AWS_ACCESS_KEY_ID=test
    - AWS_SECRET_ACCESS_KEY=test
    - AWS_DEFAULT_REGION=ap-south-2
  ports:
    - "4566:4566"
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
```

`SERVICES=kinesis,s3` activates just those two. Port `4566` is the single endpoint for all LocalStack services. The `test/test` credentials are fake — LocalStack accepts anything.

**How the Spark container connects to it:**
```yaml
# docker-compose.yml — spark service environment
- AWS_ACCESS_KEY_ID=test
- AWS_SECRET_ACCESS_KEY=test
- AWS_DEFAULT_REGION=ap-south-2
- KINESIS_ENDPOINT=http://etl-project-2-localstack:4566
```

Both the producer and aggregator run inside this container, so they both reach LocalStack via the container hostname — no host port mapping required.

```python
# kinesis_stream_producer.py
endpoint = os.getenv('KINESIS_ENDPOINT', 'http://localhost:4566')
```

The `http://localhost:4566` fallback is only relevant if you were running the producer on the Mac host directly. Inside the container the env var is already set to the correct internal hostname.

---

## MinIO

MinIO runs inside the `etl-project-2-minio` container. It's an S3-compatible object store — I point the Spark S3A connector at it and it behaves exactly like S3.

**Docker Compose config:**
```yaml
minio:
  image: minio/minio:latest
  container_name: etl-project-2-minio
  environment:
    MINIO_ROOT_USER: minioadmin
    MINIO_ROOT_PASSWORD: minioadmin
  ports:
    - "9000:9000"   # S3 API
    - "9001:9001"   # Web console
  volumes:
    - minio-storage:/minio_data
  command: server /minio_data --console-address ":9001"
```

Port `9000` is the S3 API endpoint. Port `9001` is the browser console at `http://localhost:9001`. Data persists across restarts via the `minio-storage` named volume.

**How `create_spark_session()` configures it:**
```python
s3_endpoint   = os.getenv('S3_ENDPOINT',   'http://etl-project-2-minio:9000')
s3_access_key = os.getenv('S3_ACCESS_KEY', 'minioadmin')
s3_secret_key = os.getenv('S3_SECRET_KEY', 'minioadmin')

builder.config("spark.hadoop.fs.s3a.endpoint",          s3_endpoint)
builder.config("spark.hadoop.fs.s3a.access.key",        s3_access_key)
builder.config("spark.hadoop.fs.s3a.secret.key",        s3_secret_key)
builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
```

Inside the container the default falls back to the container hostname. Outside, it picks up `S3_ENDPOINT` from `.env`.

---

## Container Networking

`docker-compose.yml` creates a bridge network (`etl-network`) so the containers talk to each other by hostname:

| From | To | Address |
|------|----|---------|
| Spark container | LocalStack | `http://etl-project-2-localstack:4566` |
| Spark container | MinIO | `http://etl-project-2-minio:9000` |
| Mac host | LocalStack | `http://localhost:4566` |
| Mac host | MinIO | `http://localhost:9000` |
| Mac host | Spark UI | `http://localhost:4040` |
| Mac host | MinIO console | `http://localhost:9001` |

**Key rule:** Inside Docker, always use the container hostname. From the Mac host, use `localhost` with the mapped port. This is why `docker-compose.yml` sets `KINESIS_ENDPOINT=http://etl-project-2-localstack:4566` in the Spark service environment — not `localhost`.

---

## Development Workflow

For the full step-by-step run guide with expected output, see [`EXECUTION.md`](EXECUTION.md). The short version:

```bash
make up          # start containers
# create MinIO buckets (see EXECUTION.md Step 2)
make producer    # start event generator
make consumer    # start Spark aggregator (new terminal)
make down        # stop everything
```

---

## Environment Variables

The `.env` file configures endpoints and credentials. Copy `.env.example` to `.env` to get started — no values need changing for local development.

The Spark container has its values set directly in `docker-compose.yml` (using internal container hostnames), so it ignores most `.env` values at runtime. The `localhost`-based values in `.env` are primarily used when running scripts or tests directly on the Mac host.

```properties
# AWS credentials (fake values for LocalStack)
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=ap-south-2

# MinIO (S3A filesystem)
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin

# LocalStack (Kinesis)
KINESIS_ENDPOINT=http://localhost:4566
```

---

## Local vs AWS

| Aspect | LocalStack / MinIO | Real AWS |
|--------|-------------------|----------|
| Cost | Free | Pay per request |
| Credentials | `test/test`, `minioadmin/minioadmin` | Real IAM keys |
| Endpoint | `localhost:PORT` | `service.region.amazonaws.com` |
| Kinesis | `etl-project-2-localstack:4566` | `kinesis.ap-south-2.amazonaws.com` |
| S3 | `etl-project-2-minio:9000` | `s3.ap-south-2.amazonaws.com` |
| Data persistence | Docker named volume | AWS managed |
| Internet required | No | Yes |
| Debugging | Local container logs | CloudWatch |

---

## Troubleshooting

If you run into errors while setting up or running the local stack, see [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md). Part 1 of that document covers all the local Docker issues in detail — JVM credential visibility, MinIO path-style access, `NoSuchBucket` after `make down`, LocalStack hostname resolution inside containers, and region derivation failures from the Kinesis connector.

---

## Quick Reference

### Commands

```bash
make up          # start all containers
make down        # stop containers and wipe volumes
make logs        # tail docker-compose logs
make test        # run unit tests
make clean       # stop containers, remove built images, delete __pycache__
make producer    # run Kinesis producer inside etl-project-2-spark (docker exec)
make consumer    # run Spark aggregator inside etl-project-2-spark (docker exec, writes to s3a://etl-project-2-data/aggregations/)
```

Both `make producer` and `make consumer` run entirely inside the Docker environment — no local Python or Spark required.

### Kinesis Metadata Path

The Kinesis connector requires a `kinesis.metadataPath` — a durable location where it persists shard offsets between micro-batches. In this project `read_kinesis()` derives the path automatically:

| Environment | `kinesis.metadataPath` |
|-------------|------------------------|
| **Local** | `{checkpoint_path}/kinesis-metadata-{suffix}` → e.g. `s3a://etl-project-2-data/checkpoints/kinesis-metadata-hourly` |
| **Production (Glue)** | `s3a://pravbala-data-engineering-projects/Project-2/checkpoints/kinesis-metadata-{suffix}` |

The local path is driven by the `--checkpoint-path` argument passed in `make consumer` (`s3a://etl-project-2-data/checkpoints`). Both Spark checkpoints and connector metadata land under the same root, so a full reset only requires deleting the `checkpoints/` prefix in MinIO.

> **Each of the 3 streaming queries gets its own unique suffix** (`hourly`, `top_tracks`, `country`) to avoid metadata conflicts — see TROUBLESHOOTING.md G4 for details.

### Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | `http://localhost:9001` | minioadmin / minioadmin |
| MinIO API | `http://localhost:9000` | minioadmin / minioadmin |
| LocalStack | `http://localhost:4566` | test / test |
| Spark UI | `http://localhost:4040` | — |

### Key Files

| File | Purpose |
|------|---------|
| `.env` | Local environment config (copy from `.env.example`) |
| `docker-compose.yml` | Container definitions and networking |
| `scripts/kinesis_stream_producer.py` | Puts events onto the Kinesis stream |
| `scripts/spark_aggregator.py` | Consumes the stream and writes aggregations |

### Environment Variables at a Glance

| Variable | Used By | Purpose |
|----------|---------|---------|
| `KINESIS_ENDPOINT` | Producer + aggregator | Kinesis service URL |
| `AWS_ACCESS_KEY_ID` | Both + Docker env | Access key (fake locally) |
| `AWS_SECRET_ACCESS_KEY` | Both + Docker env | Secret key (fake locally) |
| `AWS_DEFAULT_REGION` | Docker container JVM | Default region for SDK |
| `S3_ENDPOINT` | Aggregator | MinIO / S3 endpoint for S3A |
| `S3_ACCESS_KEY` | Aggregator | MinIO access key |
| `S3_SECRET_KEY` | Aggregator | MinIO secret key |
