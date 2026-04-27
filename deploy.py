# QBrain/qbrain-agent/deploy.py
import os
import requests
import vertexai
from vertexai import agent_engines
from dotenv import load_dotenv
import logging

# Load .env file for local runs
# In CI, variables are injected by GitLab directly so this is a no-op
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# All config read from environment 
AGENT_SERVICE_ACCOUNT = os.environ["AGENT_SERVICE_ACCOUNT"]

PROJECT_ID        = os.environ.get("PROJECT_ID",        "prj-ai-dev-qic")
REGION            = os.environ.get("REGION",            "europe-west1")
STAGING_BUCKET    = os.environ.get("STAGING_BUCKET",    "gs://qbrain-staging")
AGENT_DISPLAY_NAME = os.environ.get("AGENT_DISPLAY_NAME", "qbrain-root-agent-test-ci")

GITLAB_API_TOKEN  = os.environ.get("GITLAB_API_TOKEN",  "")
GITLAB_PROJECT_ID = os.environ.get("CI_PROJECT_ID",     "")
GITLAB_API_URL    = os.environ.get("CI_API_V4_URL",     "")

# Agent runtime env vars 
AGENT_ENV_VARS = {
    "PROJECT_ID":                  os.environ["PROJECT_ID"],
    "REGION":                      os.environ["REGION"],
    "GEMINI_MODEL":                os.environ["GEMINI_MODEL"],
    "DE_LOCATION":                 os.environ["DE_LOCATION"],
    "DE_METADATA_SERVING_CONFIG":  os.environ["DE_METADATA_SERVING_CONFIG"],
    "DE_DOC_SERVING_CONFIG":       os.environ["DE_DOC_SERVING_CONFIG"],
    "OBS_DATASET":                 os.environ["OBS_DATASET"],
    "POLICY_TABLE":                os.environ["POLICY_TABLE"],
    "MAX_BQ_BYTES":                os.environ["MAX_BQ_BYTES"],
    "MAX_ROWS":                    os.environ["MAX_ROWS"],
    "DEFAULT_TIME_WINDOW_DAYS":    os.environ["DEFAULT_TIME_WINDOW_DAYS"],
}


def save_resource_name_to_gitlab(resource_name: str):
    if not GITLAB_API_TOKEN or not GITLAB_PROJECT_ID:
        logger.warning("GITLAB_API_TOKEN or CI_PROJECT_ID not set — skipping GitLab variable save.")
        return

    url = f"{GITLAB_API_URL}/projects/{GITLAB_PROJECT_ID}/variables"
    headers = {"PRIVATE-TOKEN": GITLAB_API_TOKEN}

    # Disable SSL verification for self-signed certificate
    verify_ssl = False

    check = requests.get(
        f"{url}/AGENT_RESOURCE_NAME",
        headers=headers,
        verify=verify_ssl
    )

    if check.status_code == 200:
        response = requests.put(
            f"{url}/AGENT_RESOURCE_NAME",
            headers=headers,
            json={"value": resource_name, "protected": True, "masked": False},
            verify=verify_ssl
        )
    else:
        response = requests.post(
            url,
            headers=headers,
            json={"key": "AGENT_RESOURCE_NAME", "value": resource_name, "protected": True, "masked": False},
            verify=verify_ssl
        )

    if response.status_code in (200, 201):
        logger.info(f"✅ Saved AGENT_RESOURCE_NAME to GitLab: {resource_name}")
    else:
        logger.error(f"❌ Failed to save to GitLab: {response.status_code} {response.text}")

def main():
    if not STAGING_BUCKET:
        raise ValueError("Set STAGING_BUCKET (e.g. gs://qbrain-staging)")

    # Validate all required env vars are present before doing anything
    missing = [k for k, v in AGENT_ENV_VARS.items() if not v]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")

    vertexai.init(project=PROJECT_ID, location=REGION, staging_bucket=STAGING_BUCKET)

    from main import local_app

    logger.info(f"Creating new agent '{AGENT_DISPLAY_NAME}'...")

    engine = agent_engines.create(
        agent_engine=local_app,
        display_name=AGENT_DISPLAY_NAME,
        requirements="requirements.txt",
        extra_packages=[
            "./main.py",
            "./app",
        ],
        service_account=AGENT_SERVICE_ACCOUNT,
        env_vars=AGENT_ENV_VARS,
    )

    logger.info("✅ Deployed Successfully!")
    logger.info(f"Resource Name: {engine.resource_name}")

    save_resource_name_to_gitlab(engine.resource_name)


if __name__ == "__main__":
    main()