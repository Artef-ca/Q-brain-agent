import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


DE_METADATA_SERVING_CONFIG="projects/928365833437/locations/eu/collections/default_collection/engines/qbrain-metadata-search_1771244237073/servingConfigs/default_search:search"
DE_DOC_SERVING_CONFIG="projects/928365833437/locations/eu/collections/default_collection/engines/testqic_1770890010811/servingConfigs/default_search:search" 


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise ValueError(f"Missing env var: {name}")
    return v


@dataclass(frozen=True)
class Settings:
    project_id: str = _env("PROJECT_ID","prj-ai-dev-qic")
    region: str = _env("REGION", "europe-west1")
    gemini_model: str = _env("GEMINI_MODEL", "gemini-2.5-flash")
    model_armor_template_id: str = _env("MODEL_ARMOR_TEMPLATE_ID", "")

    de_location: str = _env("DE_LOCATION", "eu")
    de_metadata_serving_config: str = _env("DE_METADATA_SERVING_CONFIG",DE_METADATA_SERVING_CONFIG)
    de_doc_serving_config: str = _env("DE_DOC_SERVING_CONFIG",DE_DOC_SERVING_CONFIG)

    obs_dataset: str = _env("OBS_DATASET", "agent_observability")
    policy_table: str = _env("POLICY_TABLE", "governance.table_policy_registry")

    max_bq_bytes: int = int(_env("MAX_BQ_BYTES", "2000000000"))
    max_rows: int = int(_env("MAX_ROWS", "5000"))
    default_time_window_days: int = int(_env("DEFAULT_TIME_WINDOW_DAYS", "90"))

settings = Settings()
