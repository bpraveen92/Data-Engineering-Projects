# Local Development Setup

The full local stack runs in Docker — LocalStack mocks Kinesis, MinIO mocks S3. The same Python code connects to real AWS in production; only the `.env` values change.

---

## Container Architecture

```
spark_aggregator.py / kinesis_stream_producer.py
    │
    ├── LocalStack  (Kinesis mock)   port 4566
    └── MinIO       (S3 mock)        port 9000 / 9001 console
```

All three containers share a bridge network (`etl-network`) and communicate by hostname.

---

## Container Networking

| From | To | Address |
|------|----|---------|
| Spark container | LocalStack | `http://etl-project-2-localstack:4566` |
| Spark container | MinIO | `http://etl-project-2-minio:9000` |
| Mac host | LocalStack | `http://localhost:4566` |
| Mac host | MinIO API | `http://localhost:9000` |
| Mac host | MinIO console | `http://localhost:9001` |
| Mac host | Spark UI | `http://localhost:4040` |

**Key rule:** Inside Docker always use the container hostname. From the Mac host use `localhost`. This is why `docker-compose.yml` sets `KINESIS_ENDPOINT=http://etl-project-2-localstack:4566` in the Spark service env — not `localhost`.

---

## LocalStack

Mocks Kinesis (and S3) inside `etl-project-2-localstack`. Credentials are fake — LocalStack accepts anything.

```yaml
environment:
  - SERVICES=kinesis,s3
  - AWS_ACCESS_KEY_ID=test
  - AWS_SECRET_ACCESS_KEY=test
  - AWS_DEFAULT_REGION=ap-south-2
```

---

## MinIO

S3-compatible object store inside `etl-project-2-minio`. Data persists across restarts via the `minio-storage` named volume — wiped on `make down`.

```yaml
environment:
  MINIO_ROOT_USER: minioadmin
  MINIO_ROOT_PASSWORD: minioadmin
ports:
  - "9000:9000"   # S3 API
  - "9001:9001"   # web console
```

The Spark S3A connector is configured to hit MinIO with path-style access:

```python
builder.config("spark.hadoop.fs.s3a.endpoint",          "http://etl-project-2-minio:9000")
builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
```

---

## Connector JAR (one-time build)

The awslabs Kinesis connector has no published Maven Central artifacts. Build from source once:

```bash
make build-kinesis-jar   # clones v1.4.2, builds with Maven in Docker, ~3 min
```

The JAR is baked into the Spark image at build time. After building, rebuild the image:

```bash
docker compose build spark
# or just run `make up` — it builds automatically if no image exists
```

---

## Environment Variables

Copy `.env.example` to `.env` — no values need changing for local development.

```properties
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=ap-south-2

S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin

KINESIS_ENDPOINT=http://localhost:4566
```

The Spark container overrides these with internal container hostnames via `docker-compose.yml`. The `.env` values are used when running scripts directly on the Mac host.

---

## Local vs AWS

| Aspect | Local | AWS |
|--------|-------|-----|
| Kinesis | `etl-project-2-localstack:4566` | `kinesis.ap-south-2.amazonaws.com` |
| S3 | `etl-project-2-minio:9000` | `s3.ap-south-2.amazonaws.com` |
| Credentials | `test/test`, `minioadmin/minioadmin` | IAM keys |
| Cost | Free | Pay per use |
| Internet | Not required | Required |
| Debugging | Container logs | CloudWatch |

---

## Quick Reference

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO console | `http://localhost:9001` | minioadmin / minioadmin |
| MinIO API | `http://localhost:9000` | minioadmin / minioadmin |
| LocalStack | `http://localhost:4566` | test / test |
| Spark UI | `http://localhost:4040` | — |

For the full step-by-step run guide see [`EXECUTION.md`](EXECUTION.md).  
For errors encountered during setup see [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md).
