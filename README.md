# Q-Brain Agent API (End-to-End Skeleton)

## 1) Prereqs
- Vertex AI Workbench OR any machine with gcloud auth
- Service Account (temporary) with:
  - BigQuery read on business datasets
  - BigQuery write on agent_observability dataset
  - Discovery Engine (Vertex AI Search) access
  - Vertex AI access for Gemini

## 2) Configure
Copy `.env.example` to `.env` and fill values.

If running locally with an SA key:
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/sa.json"

## 3) Create BigQuery tables
Run the SQL in:
- sql/00_create_datasets.sql
- sql/01_create_policy_registry.sql
- sql/02_create_observability_tables.sql

## 4) Run locally
bash scripts/run_local.sh

## 5) Smoke test
python scripts/smoke_test.py

## 6) Deploy (Cloud Run)
bash scripts/deploy_cloud_run.sh
