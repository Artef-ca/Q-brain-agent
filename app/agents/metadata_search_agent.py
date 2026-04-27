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

def _num(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

class MetadataSearchAgent:
    def __init__(self, de_tools: DiscoveryEngineTools) -> None:
        self.de = de_tools

    # ADDED trace_id parameter
    def find_best_table(self, trace_id: str, question: str) -> dict[str, Any]:
        logger.info(f"--- [MetadataSearchAgent] [Trace: {trace_id}] Finding best table for question: '{question}' ---")
        
        # PASSED trace_id to de.search
        hits = self.de.search(trace_id, settings.de_metadata_serving_config, question, page_size=8)
        
        if not hits:
            logger.warning(f"--- [MetadataSearchAgent] [Trace: {trace_id}] No results found in Discovery Engine ---")
            return {"ok": False, "error": "NO_RESULTS"}

        candidates: list[dict[str, Any]] = []
        for h in hits:
            sd = h.struct_data or {}
            dsd = h.derived_struct_data or {}

            # Prefer struct_data fields (your UI shows these exist)
            src = sd if sd else dsd

            project = src.get("project") or src.get("bq_project")  # support either
            dataset = src.get("dataset")
            table = src.get("table")

            # --- NEW: Explicitly extract columns ---
            # Discovery Engine usually returns a list of dicts: [{'name': '...', 'type': '...'}, ...]
            columns = src.get("columns", [])
            

            # Sometimes you encode only id; fallback parse from id:
            # id = "smartadvisor-483817_qic_test_AssetSales"
            if (not project or not dataset or not table) and h.doc_id:
                parts = h.doc_id.split("_")
                if len(parts) >= 3:
                    project = project or parts[0]
                    dataset = dataset or parts[1]
                    table = table or "_".join(parts[2:])

            if not (project and dataset and table):
                continue

            search_rel = _num(h.score, 0.0)
            bq_rank = _num(src.get("bq_ranking_score"), 0.0)
            recency = _num(src.get("bq_recency_score"), 0.0)
            usage = _num(src.get("bq_usage_score"), 0.0)

            # Weighted score (tune as you like)
            final_score = (
                0.55 * search_rel +
                0.35 * bq_rank +
                0.07 * recency +
                0.03 * usage
            )

            candidates.append({
                "project": project,
                "dataset": dataset,
                "table": table,
                # --- CRITICAL ADDITION ---
                "columns": columns, 
                "description": src.get("description", ""),
                "llm_enrichment_json": src.get("llm_enrichment_json", "{}"),
                # -------------------------
                "search_relevance_score": search_rel,
                "bq_ranking_score": bq_rank,
                "bq_recency_score": recency,
                "bq_usage_score": usage,
                "final_score": final_score,
                "doc_id": h.doc_id,
                "raw": src,  # keep for debug/telemetry
            })

        if not candidates:
            logger.warning(f"--- [MetadataSearchAgent] [Trace: {trace_id}] Hits found, but missing required project/dataset/table fields ---")
            return {"ok": False, "error": "NO_TABLE_FIELDS_FOUND"}

        candidates.sort(key=lambda x: x["final_score"], reverse=True)
        
        logger.info(f"--- [MetadataSearchAgent] [Trace: {trace_id}] Ranked {len(candidates)} valid candidate tables. Top pick: {candidates[0]['dataset']}.{candidates[0]['table']} ---")
        
        return {"ok": True, "best": candidates[0], "top_k": candidates[:5]}