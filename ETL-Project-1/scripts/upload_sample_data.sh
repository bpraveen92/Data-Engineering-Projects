#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f .env ]]; then
  echo "Missing .env. Create it from .env.example first."
  exit 1
fi

set -a
source .env
set +a

python3 scripts/upload_sample_data_to_s3.py
