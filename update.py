# QBrain/qbrain-agent/update.py
import os
import vertexai
from vertexai.preview.reasoning_engines import ReasoningEngine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID", "prj-ai-dev-qic")
REGION = os.environ.get("REGION", "europe-west1")
STAGING_BUCKET = os.environ.get("STAGING_BUCKET", "gs://qbrain-staging")
AGENT_RESOURCE_NAME = os.environ.get("AGENT_RESOURCE_NAME", "projects/928365833437/locations/europe-west1/reasoningEngines/2381111177204727808")


def main():
    if not STAGING_BUCKET:
        raise ValueError("Set STAGING_BUCKET")

    if not AGENT_RESOURCE_NAME:
        raise ValueError(
            "AGENT_RESOURCE_NAME is not set. "
            "Run deploy.py first to create the agent and register the variable."
        )

    logger.info("Initializing Vertex AI...")
    vertexai.init(project=PROJECT_ID, location=REGION, staging_bucket=STAGING_BUCKET)

    from main import local_app

    logger.info(f"Updating agent: {AGENT_RESOURCE_NAME}")
    engine = ReasoningEngine(AGENT_RESOURCE_NAME)

    engine.update(
        reasoning_engine=local_app,
        requirements="requirements.txt",
        extra_packages=[
            "./main.py",
            "./app",
        ]
    )

    logger.info("✅ Updated Successfully!")
    logger.info(f"Resource Name: {engine.resource_name}")


if __name__ == "__main__":
    main()