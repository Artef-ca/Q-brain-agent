from __future__ import annotations
from typing import Any
import logging
import sys

from app.tools.discoveryengine_search import DiscoveryEngineTools
from app.config import settings

# Configure logging format to include timestamps
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class RAGAgent:
    def __init__(self, trace_id: str, de: DiscoveryEngineTools) -> None:
        self.de = de

    def answer(self, trace_id: str, question: str) -> dict[str, Any]:
        logger.info(f"--- [RAGAgent] [Trace: {trace_id}] Fetching RAG answer for: '{question}' ---")
        return self.de.answer(trace_id, settings.de_doc_serving_config, question)
