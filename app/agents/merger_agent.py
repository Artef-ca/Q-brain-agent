from __future__ import annotations
from typing import Any

class MergerAgent:
    def merge(self, plan: dict[str, Any], bq_payload: dict[str, Any] | None, rag_payload: dict[str, Any] | None) -> dict[str, Any]:
        steps = []
        evidence = []
        stats = {}

        if plan.get("needs_bq"):
            steps.append("Computed structured metrics from BigQuery (validated by policy).")
            stats["bigquery"] = {k: bq_payload.get(k) for k in ["job_id","bytes_processed","elapsed_ms","dry_run_bytes"]} if bq_payload else {}
            evidence.append({"type":"bq","title":"BigQuery metrics", "details": stats["bigquery"]})

        if plan.get("needs_rag"):
            steps.append("Retrieved grounded information from documents via Vertex AI Search.")
            evidence.append({"type":"rag","title":"RAG citations", "details": {"citations": (rag_payload or {}).get("citations", [])}})

        return {"steps": steps, "evidence": evidence, "stats": stats}
