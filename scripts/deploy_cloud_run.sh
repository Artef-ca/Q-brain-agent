#!/usr/bin/env bash
set -euo pipefail

export $(cat .env | xargs)

SERVICE="qbrain-agent-api"
gcloud config set project "$PROJECT_ID"
gcloud config set run/region "$REGION"

gcloud run deploy "$SERVICE" \
  --source . \
  --region "$REGION" \
  --allow-unauthenticated \
  --set-env-vars "PROJECT_ID=$PROJECT_ID,REGION=$REGION,GEMINI_MODEL=$GEMINI_MODEL,DE_LOCATION=$DE_LOCATION,DE_METADATA_SERVING_CONFIG=$DE_METADATA_SERVING_CONFIG,DE_DOC_SERVING_CONFIG=$DE_DOC_SERVING_CONFIG,OBS_DATASET=$OBS_DATASET,POLICY_TABLE=$POLICY_TABLE,MAX_BQ_BYTES=$MAX_BQ_BYTES,MAX_ROWS=$MAX_ROWS,DEFAULT_TIME_WINDOW_DAYS=$DEFAULT_TIME_WINDOW_DAYS"
