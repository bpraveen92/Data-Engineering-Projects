# Execution Guide: Local Docker vs. Production MWAA

## Local Docker Execution

### Prerequisites
- Docker Desktop installed
- AWS credentials configured (`~/.aws/credentials`)
- `.env` file with all configuration values
- Redshift Serverless workgroup created

### Quick Start

```bash
cd Projects/DE-Project-1

# 1. Initialize S3 bucket structure (first time)
make aws-bootstrap

# 2. Start local Airflow stack
make up
# Access at http://localhost:8080 (admin/admin)

# 3. Generate synthetic test data
make inflate-sample

# 4. Upload to S3
make upload-sample

# 5. Trigger DAG
# In Airflow UI: Click "Trigger DAG" button
# Or via CLI: airflow dags trigger s3_to_redshift_pipeline

# 6. Monitor
make logs

# 7. Shutdown
make down
```

### Task Execution Flow

| Task | Runtime | What It Does |
|------|---------|------------|
| `find_pending_files` | 1-2s | Lists unprocessed files in S3 raw prefix |
| `should_run` | <1s | Branches: skip if no pending files |
| `build_run_context` | <1s | Creates run ID, groups by dataset |
| `run_transform` | 15-60s | CSV → normalize → cast to string → parquet in S3 |
| `run_quality_checks` | 5-15s | Great Expectations validation |
| `load_to_redshift` | 10-30s | COPY parquet → staging → upsert into silver tables |
| `build_gold_layer` | 5-10s | Rebuild BI aggregation tables |
| `mark_files_processed` | 2-5s | Update processed files manifest in S3 |

**Total expected runtime**: 40-120 seconds for ~500K rows

### Validate Results

**Check S3 for curated parquet:**
```bash
aws s3 ls s3://<bucket>/Project-1/curated/music/songs/ --recursive
# Should show: run_id=<timestamp>/{data.parquet, schema_snapshot.json}
```

**Query Redshift:**
```sql
SELECT COUNT(*) FROM silver.songs;
SELECT COUNT(*) FROM silver.users;
SELECT COUNT(*) FROM silver.streams;
SELECT * FROM gold.daily_stream_metrics LIMIT 5;
SELECT * FROM silver.pipeline_audit ORDER BY run_date DESC LIMIT 1;
```

**Check processed manifest:**
```bash
aws s3 cp s3://<bucket>/Project-1/schemas/music/airflow/processed_raw_files.json - | jq .
```

### Test Idempotency

```bash
# Same files shouldn't reprocess
make upload-sample
airflow dags trigger s3_to_redshift_pipeline

# Expected: find_pending_files returns 0 files, should_run skips remaining tasks
```

### Test New Data Arrival

```bash
# Generate new synthetic data (different timestamp)
make inflate-sample
make upload-sample
airflow dags trigger s3_to_redshift_pipeline

# Expected: Only new files processed, data accumulates in Redshift
```

---

## Production MWAA Execution

### One-Time AWS Setup

Create these resources manually via AWS Console:

1. **S3 Bucket** with structure:
   ```
   s3://bucket/Project-1/
   ├── raw/
   ├── curated/music/
   ├── schemas/music/
   └── mwaa/
       ├── dags/
       ├── plugins/
       └── requirements/requirements.txt
   ```

2. **Redshift Serverless Workgroup**:
   - Database name (e.g., `dev`)
   - RPU configuration (default 8 for dev/test)
   - Admin credentials in AWS Secrets Manager

3. **AWS Secrets Manager Secret** (`project1-config`):
   ```json
   {
     "AWS_REGION": "ap-south-2",
     "S3_RAW_BUCKET": "my-bucket",
     "S3_RAW_PREFIX": "Project-1/raw/",
     "REDSHIFT_DATABASE": "dev",
     "REDSHIFT_WORKGROUP": "default-workgroup",
     "REDSHIFT_SECRET_ARN": "arn:aws:secretsmanager:...",
     "REDSHIFT_IAM_ROLE_ARN": "arn:aws:iam::..."
   }
   ```

4. **MWAA Environment**:
   - VPC with private subnets (2+ AZs)
   - Execution role auto-created by AWS
   - DAG S3 path: `s3://bucket/Project-1/mwaa/dags/`
   - Requirements path: `s3://bucket/Project-1/mwaa/requirements/requirements.txt`

### IAM Policy for MWAA Execution Role

Attach this inline policy to MWAA's execution role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:ListBucketVersions"],
      "Resource": ["arn:aws:s3:::bucket", "arn:aws:s3:::bucket/Project-1/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
      "Resource": "arn:aws:secretsmanager:ap-south-2:ACCOUNT:secret:project1-config*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "redshift-data:ExecuteStatement", "redshift-data:DescribeStatement",
        "redshift-data:GetStatementResult", "redshift-data:ListStatements"
      ],
      "Resource": "arn:aws:redshift-serverless:ap-south-2:ACCOUNT:workgroup/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces",
        "ec2:DeleteNetworkInterface", "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups", "ec2:DescribeVpcs"
      ],
      "Resource": "*"
    }
  ]
}
```

**Replace:** `bucket`, `ACCOUNT`, `ap-south-2` with your values.

### Deploy to MWAA

```bash
# 1. Upload DAG files
aws s3 cp dags/s3_to_redshift_pipeline.py s3://bucket/Project-1/mwaa/dags/
aws s3 cp config/*.yaml s3://bucket/Project-1/mwaa/dags/config/
aws s3 cp -r src/ s3://bucket/Project-1/mwaa/dags/
aws s3 cp -r transform_jobs/ s3://bucket/Project-1/mwaa/dags/

# 2. Upload requirements
aws s3 cp mwaa/requirements.txt s3://bucket/Project-1/mwaa/requirements/requirements.txt

# 3. Wait ~30-60 seconds for MWAA to sync from S3
```

### Operate MWAA

1. **Access**: AWS Console → MWAA → Environment → Open Airflow UI
2. **Unpause DAG**: Toggle pause switch to ON
3. **Monitor**: MWAA runs every 15 minutes (or manually trigger)
4. **Logs**: CloudWatch Logs → `/aws/mwaa/<env-name>/`

### Verify Results

```sql
-- Check data loaded
SELECT COUNT(*) FROM silver.songs;
SELECT * FROM gold.daily_stream_metrics ORDER BY stream_date DESC LIMIT 5;

-- Check audit trail
SELECT run_id, task_name, status FROM silver.pipeline_audit 
ORDER BY run_date DESC LIMIT 10;
```

### Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `Missing required environment variable` error | Secrets Manager secret not created or permissions missing | Create `project1-config` secret; grant MWAA role `secretsmanager:GetSecretValue` |
| `load_to_redshift` times out | Network blocked to Redshift | Check MWAA/Redshift security groups; allow outbound HTTPS (443) |
| `NoCredentialsError` | MWAA execution role lacks permissions | Verify IAM policy attached with S3/Redshift/Secrets Manager actions |
| DAG not appearing | Files not synced to MWAA DAG folder | Verify S3 path: `s3://bucket/Project-1/mwaa/dags/` |

### Update Production Code

```bash
# 1. Test locally
make up
# ... verify changes ...
make down

# 2. Deploy to MWAA
aws s3 cp <modified-files> s3://bucket/Project-1/mwaa/dags/

# 3. Verify sync (~1 min) and trigger manual run in MWAA UI
```

---

## Quick Reference

| Task | Local | MWAA |
|------|-------|------|
| Start | `make up` | Already running |
| Config | `.env` file | AWS Secrets Manager |
| Schedule | 15 min + manual trigger | 15 min schedule |
| Logs | `make logs` | CloudWatch Logs |
| Cost | Free | ~$100-300/month |
| Best for | Development | Production |
