from __future__ import annotations
from app.config import settings
import logging
import sys # <--- Import sys
# app/tools/discoveryengine_search.py
from dataclasses import dataclass
from typing import Any
from google.api_core.client_options import ClientOptions
from google.cloud import discoveryengine_v1alpha as de
from google.protobuf.json_format import MessageToDict


# logger = logging.getLogger("uvicorn.error") # Use uvicorn's logger to ensure it shows up
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class SearchHit:
    doc_id: str
    score: float
    struct_data: dict[str, Any]
    derived_struct_data: dict[str, Any]

class DiscoveryEngineTools:
    def __init__(self) -> None:
        client_options = ClientOptions(api_endpoint="eu-discoveryengine.googleapis.com")
        self.search_client = de.SearchServiceClient(client_options=client_options)
        self.conv_client = de.ConversationalSearchServiceClient(client_options=client_options)

    def search(self, trace_id: str, serving_config: str, query: str, page_size: int = 10) -> list[SearchHit]:
        logger.info(f"--- [DiscoveryEngineTools] [Trace: {trace_id}] Executing Metadata Search ---")
        logger.info(f"--- [DiscoveryEngineTools] [Trace: {trace_id}] Query: {query}")
        logger.info(f"--- [DiscoveryEngineTools] [Trace: {trace_id}] Config: {serving_config}")

        req = de.SearchRequest(serving_config=serving_config, query=query, page_size=page_size)
        
        try:
            resp = self.search_client.search(req)
        except Exception as e:
            logger.error(f"--- [DiscoveryEngineTools] [Trace: {trace_id}] Search Failed: {e}")
            raise e

        logger.info(f"--- [DiscoveryEngineTools] [Trace: {trace_id}] Search Results Found: {len(resp.results)} ---")

        hits: list[SearchHit] = []
        for r in resp.results:
            doc = r.document
            
            # Log the ID of hits found to verify correctness
            logger.info(f"--- [DiscoveryEngineTools] [Trace: {trace_id}] Hit Found: {doc.id}")

            sd = dict(doc.struct_data) if doc.struct_data else {}
            dsd = dict(doc.derived_struct_data) if doc.derived_struct_data else {}
            # logger.info(f"SD Found: {sd}")
            # logger.info(f"dsd Found: {dsd}")

            hits.append(
                SearchHit(
                    doc_id=str(doc.id),
                    score=getattr(r, "relevance_score", 0.0),
                    struct_data=sd,
                    derived_struct_data=dsd,
                )
            )
            # logger.info(f"hits Final Appended Found: {hits}")
        return hits

    def answer(self, trace_id: str, serving_config: str, question: str, session: str | None = None) -> dict[str, Any]:
        logger.info(f"--- [DiscoveryEngineTools] [Trace: {trace_id}] Executing RAG Answer for {question} ---")

        # --- THE FIX: Relax the aggressive filtering rules ---
        answer_generation_spec = de.AnswerQueryRequest.AnswerGenerationSpec(
            ignore_non_answer_seeking_query=False,
            ignore_low_relevant_content=False
        )
        
        req = de.AnswerQueryRequest(
            serving_config=serving_config,
            query=de.Query(text=question),
            session=session or "",
            answer_generation_spec=answer_generation_spec
        )
        
        logger.info(f"--- [DiscoveryEngineTools] [Trace: {trace_id}] Prepared req : {req} ---")

        resp = self.conv_client.answer_query(req)
        logger.info(f"--- [DiscoveryEngineTools] [Trace: {trace_id}] RAG Answer Text Length: {len(getattr(resp.answer, 'answer_text', ''))}")
        logger.info(f"--- [DiscoveryEngineTools] [Trace: {trace_id}] RAG Answer Text Length: {resp.answer}")

        # --- FIX: Manually extract fields instead of using dict() on Step object ---
        steps_list = []
        for s in getattr(resp.answer, "steps", []):
            steps_list.append({
                "state": getattr(s, "state", "UNKNOWN"),
                "description": getattr(s, "description", ""),
                "thought": getattr(s, "thought", "")
            })

        return {
            "answer": getattr(resp.answer, "answer_text", ""),
            "citations": [dict(c) for c in getattr(resp.answer, "citations", [])],
            "steps": steps_list, # Use the manually fixed list
        }

  
