# MWAA Secrets Manager Setup Guide

## Overview

This guide explains how I configured AWS Secrets Manager to provide environment variables to my MWAA-deployed S3-to-Redshift pipeline. The pipeline uses a clever hybrid approach:

1. **Local Development**: Reads from `.env` file (no AWS calls, super fast)
2. **MWAA Deployment**: Falls back to AWS Secrets Manager (when `.env` isn't available)

This way, I can work efficiently locally without hitting AWS APIs, and the production environment stays secure with centralized config management.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│          src/common/config.py env() function        │
└─────────────────────────────────────────────────────┘
                         │
                    Checks stages:
                         │
    ┌────────────────────┼────────────────────┐
    │                    │                    │
    ▼                    ▼                    ▼
Stage 1:            Stage 2:             Stage 3:
Local .env      Secrets Manager         Default/Error
(10ms)          (50-100ms, cached)      (immediate)
    │                    │                    │
    └────────────────────┴────────────────────┘
                         │
                    Return value
```

---

## Prerequisites

Before you get started, make sure you have:
- AWS account with permissions to create Secrets Manager secrets
- MWAA environment already created and running
- MWAA execution IAM role identified

---

## Step 1: Create the Secret in AWS Secrets Manager

### Via AWS Console

1. Go to **AWS Secrets Manager** console
2. Click **Store a new secret**
3. Set it up like this:
   - **Secret type**: Other type of secret
   - **Key/value pairs**: Enter the configuration below
4. **Name**: `project1-config`
5. **Description**: "Configuration for Project-1 S3-to-Redshift pipeline"

### Configuration Key-Value Pairs

Here's the JSON I use as the secret value:

```json
{
  "AWS_REGION": "ap-south-2",
  "S3_RAW_BUCKET": "pravbala-data-engineering-projects",
  "S3_RAW_PREFIX": "Project-1/raw/",
  "S3_CURATED_BUCKET": "pravbala-data-engineering-projects",
  "S3_CURATED_PREFIX": "Project-1/curated/music/",
  "S3_SCHEMA_REGISTRY_PREFIX": "Project-1/schemas/music/",
  "REDSHIFT_DATABASE": "dev",
  "REDSHIFT_WORKGROUP": "default-workgroup",
  "REDSHIFT_SECRET_ARN": "arn:aws:secretsmanager:ap-south-2:ACCOUNT:secret:redshift-serverless-PnaOPt",
  "REDSHIFT_IAM_ROLE_ARN": "arn:aws:iam::ACCOUNT:role/service-role/AmazonRedshift-CommandsAccessRole-20260226T125816",
  "TARGET_SCHEMA": "silver",
  "GOLD_SCHEMA": "gold",
  "PIPELINE_NAME": "s3_csv_to_redshift",
  "BATCH_SCHEDULE": "*/15 * * * *",
  "FAILURE_POLICY": "quarantine-and-continue"
}
```

**Important**: Replace the placeholder values with your actual AWS account ID, resource ARNs, and settings.


---

## Step 2: Configure MWAA Execution Role Permissions

### About the MWAA Execution Role

When you create your MWAA environment in AWS, **AWS automatically creates an execution IAM role** for you. This role has basic permissions for MWAA to function, but you need to **add additional permissions** for our pipeline to access Secrets Manager, S3, and Redshift.

### Identify Your MWAA Execution Role

1. Go to **AWS MWAA** console
2. Select your environment (e.g., `project1-mwaa`)
3. Click the **Permissions** tab
4. Note the **Execution role ARN** (format: `arn:aws:iam::ACCOUNT:role/service-role/ROLE_NAME`)

### Add Required Permissions

Instead of manually creating a policy, I recommend using the complete policy document from [EXECUTION.md → IAM Policy Configuration](EXECUTION.md#iam-policy-configuration-for-mwaa). This single policy grants all necessary permissions for Secrets Manager, S3, and Redshift access.

**Steps**:
1. Go to **IAM** → **Roles** → Select your MWAA execution role
2. Click **Add inline policy**
3. Choose **JSON** and paste the policy from [EXECUTION.md](EXECUTION.md#iam-policy-configuration-for-mwaa)
4. Replace placeholders (account ID, region, bucket name)
5. Name it: `Project1-Pipeline-Permissions`
6. Click **Create policy**

That's it! No need to attach any additional managed policies.

---

## Step 3: Verify the Configuration

### Test Locally (Before MWAA)

The code will automatically use your local `.env` file. No changes needed.

```bash
# This will read from .env
python3 -c "from src.common.config import env; print(env('S3_RAW_BUCKET'))"
# Output: pravbala-data-engineering-projects
```

### Test on MWAA

1. Upload updated DAG files and requirements to the MWAA S3 paths (run the `aws s3 cp` commands mentioned in `docs/EXECUTION.md` under "Upload DAG Files to MWAA S3 Path"). Note: the MWAA environment itself was created manually via the AWS Console; there's no automated deployment script in this repo.

2. Trigger a DAG run from MWAA UI

3. Check the logs for the DAG run:
    - If successful: DAG should proceed to step 2 (`find_pending_files`)
    - If failed: Check error logs for Secrets Manager issues

### Understand the Caching Strategy

I implemented a 5-minute cache for Secrets Manager lookups to handle a realistic production scenario: **files arriving continuously and unpredictably in S3**. With the DAG running every 15 minutes and potentially multiple data ingestion windows, I wanted to avoid hammering the Secrets Manager API on every task—especially when the configuration rarely changes.

Here's how it works in practice:
- **First DAG run**: Secrets Manager is queried once, credentials are cached (~50-100ms API call)
- **Subsequent runs within 5 minutes**: Configuration served from cache (~1ms, zero API calls)
- **After cache expires**: Fresh API call fetches latest secrets (handles config updates gracefully)

This balances two concerns:
1. **Performance**: No latency overhead for repeated runs within the cache window
2. **Freshness**: Config changes propagate within 5 minutes without manual pipeline restarts

Monitor CloudWatch Logs to see cache behavior:
```
First run: GetSecretValue API call successful
Run 2-3 (within 5 min): Serving from cache
5+ minutes later: Fresh API call to refresh cache
```


---

## Code Changes Summary

The following files were modified to support Secrets Manager:

### 1. `src/common/config.py`

I added:
- `load_secrets_from_secrets_manager()` function
- Enhanced `env()` function with 3-stage resolution:
  1. Local environment variables (`.env`)
  2. AWS Secrets Manager (with 5-minute caching)
  3. Default value or error
- Caching logic to minimize API calls
- Lazy initialization of boto3 (so imports don't fail when it's not needed)

### 2. `mwaa/requirements.txt`

I added:
- `boto3==1.34.162` and `botocore==1.34.162` (for MWAA)


---

## How It Works in Detail

### Local Development Flow

```
env("S3_RAW_BUCKET")
  ↓
Check os.getenv("S3_RAW_BUCKET") from .env
  ↓
Found! Return "pravbala-data-engineering-projects"
(No SM calls, ~1ms)
```

### MWAA Deployment Flow (First Run)

```
env("S3_RAW_BUCKET")
  ↓
Check os.getenv("S3_RAW_BUCKET")
  ↓
Not found (no .env file on MWAA)
  ↓
Load from Secrets Manager (project1-config)
  ↓
Cache result for 5 minutes
  ↓
Return "pravbala-data-engineering-projects"
(SM API call, ~50-100ms, cached for next 5 min)
```

### MWAA Deployment Flow (Subsequent Runs Within 5 Minutes)

```
env("S3_RAW_BUCKET")
  ↓
Check os.getenv("S3_RAW_BUCKET")
  ↓
Not found
  ↓
Return from cache (already loaded)
(No SM API call, ~1ms)
```### Local Development Flow

```
env("S3_RAW_BUCKET")
  ↓
Check os.getenv("S3_RAW_BUCKET") from .env
  ↓
Found! Return "pravbala-data-engineering-projects"
(No SM calls, ~1ms)
```

### MWAA Deployment Flow (First Run)

```
env("S3_RAW_BUCKET")
  ↓
Check os.getenv("S3_RAW_BUCKET")
  ↓
Not found (no .env file on MWAA)
  ↓
Load from Secrets Manager (project1-config)
  ↓
Cache result for 5 minutes
  ↓
Return "pravbala-data-engineering-projects"
(SM API call, ~50-100ms, cached for next 5 min)
```

### MWAA Deployment Flow (Subsequent Runs Within 5 Minutes)

```
env("S3_RAW_BUCKET")
  ↓
Check os.getenv("S3_RAW_BUCKET")
  ↓
Not found
  ↓
Return from cache (already loaded)
(No SM API call, ~1ms)
```


---

## Reference

- [AWS Secrets Manager Documentation](https://docs.aws.amazon.com/secretsmanager/)
- [MWAA IAM Permissions](https://docs.aws.amazon.com/mwaa/latest/userguide/mwaa-iam-roles.html)
- [boto3 Secrets Manager API](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/secretsmanager.html)
