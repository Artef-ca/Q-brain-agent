#!/usr/bin/env bash
set -euo pipefail

export $(cat .env | xargs)

# If using SA key locally:
# export GOOGLE_APPLICATION_CREDENTIALS="/path/to/sa.json"

uvicorn app.api.main:app --host 0.0.0.0 --port 8082 --reload
