from __future__ import annotations
from typing import Any
import json
import logging
import uuid
from datetime import datetime, timezone
from google.cloud import bigquery

from app.config import settings
from app.core.trace import Trace

logger = logging.getLogger(__name__)

class TelemetryWriter:
    def __init__(self) -> None:
        self.client = bigquery.Client(project=settings.project_id) 

    def _table(self, name: str) -> str:
        return f"{settings.project_id}.{settings.obs_dataset}.{name}"

    def write_trace(self, trace: Trace, status: str, final_intent: str, answer_len: int) -> None:
        
        try:
            current_time = datetime.now(timezone.utc).isoformat() 
            row = {
                "trace_id": trace.trace_id,
                "session_id": trace.session_id,
                "user_id": trace.user_id, 
                "is_pro": trace.metadata.get("is_pro_mode", False),
                "status": status,
                "final_intent": final_intent,
                "question": trace.metadata.get("question", ""),
                "answer": str(trace.metadata.get("answer", ""))[:10000], # Cap length for safety
                "error_message": trace.metadata.get("error_message"),
                "total_tokens": trace.metadata.get("total_tokens"),
                "answer_len": answer_len,
                "start_datetime": t.get("start_datetime"), 
                "end_datetime": t.get("end_datetime"), 
                "latency_ms": trace.metadata.get("latency_ms", getattr(trace, 'elapsed_ms', lambda: 0)()),
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            
            errors = self.client.insert_rows_json(self._table("traces"), [row])
            if errors:
                logger.error(f"BQ Insert Error (traces): {errors}")
        except Exception as e:
            logger.error(f"Failed to prepare traces telemetry: {e}")

    def write_routing(self, trace: Trace) -> None:
        if not trace.routing:
            return

        try:
            rows = []
            for r in trace.routing:
                rows.append({
                    "trace_id": trace.trace_id,
                    "step": float(r.get("step", 0)),
                    "agent": r.get("agent", "unknown"),
                    "decision_type": r.get("decision_type", "unknown"),
                    "payload": json.dumps(r.get("payload", {}), default=str), # BQ expects Stringified JSON
                    "latency_ms": r.get("latency_ms"),
                    "start_datetime": s.get("start_datetime"),
                    "end_datetime": s.get("end_datetime"),
                    "created_at": datetime.now(timezone.utc).isoformat()
                })

            errors = self.client.insert_rows_json(self._table("routing_decisions"), rows)
            if errors:
                logger.error(f"BQ Insert Error (routing_decisions): {errors}")
        except Exception as e:
            logger.error(f"Failed to prepare routing telemetry: {e}")

    def write_tool_calls(self, trace: Trace) -> None:
        if not trace.tool_calls:
            return
            
        try:
            rows = []
            for t in trace.tool_calls:
                rows.append({
                    "trace_id": trace.trace_id,
                    "tool_name": t.get("tool_name", "unknown"),
                    "input": json.dumps(t.get("input", {}), default=str),
                    "output": json.dumps(t.get("output", {}), default=str),
                    "status": t.get("status", "SUCCESS"),
                    "latency_ms": t.get("latency_ms"),
                    "error_message": t.get("error_message"),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "start_datetime": t.get("start_datetime"),
                    "end_datetime": t.get("end_datetime")
                })
                
            errors = self.client.insert_rows_json(self._table("tool_calls"), rows)
            if errors:
                logger.error(f"BQ Insert Error (tool_calls): {errors}")
        except Exception as e:
            logger.error(f"Failed to prepare tool_calls telemetry: {e}")

    def write_sql_audit(self, trace: Trace) -> None:
        if not trace.sql_audit:
            return
            
        try:
            rows = []
            for s in trace.sql_audit:
                rows.append({
                    "trace_id": trace.trace_id,
                    "sql_text": s.get("sql_text", ""),
                    "tables_referenced": s.get("tables_referenced", []),
                    "status": s.get("status", "SUCCESS"),
                    "latency_ms": s.get("latency_ms"),
                    "bytes_processed": s.get("bytes_processed"),
                    "error_message": s.get("error_message"),
                    "start_datetime": s.get("start_datetime"),
                    "end_datetime": s.get("end_datetime"),
                    "created_at": datetime.now(timezone.utc).isoformat()
                })
                
            errors = self.client.insert_rows_json(self._table("sql_audit"), rows)
            if errors:
                logger.error(f"BQ Insert Error (sql_audit): {errors}")
        except Exception as e:
            logger.error(f"Failed to prepare sql_audit telemetry: {e}")

    def write_llm_calls(self, trace: Trace) -> None:
        # Check if the Trace object has llm_calls initialized (to avoid breaking old code)
        if not hasattr(trace, 'llm_calls') or not trace.llm_calls:
            return

        try:
            rows = []
            for llm in trace.llm_calls:
                rows.append({
                    "llm_call_id": llm.get("llm_call_id", str(uuid.uuid4())),
                    "trace_id": trace.trace_id,
                    "session_id": trace.session_id,
                    "agent_step": llm.get("agent_step", "unknown"),
                    "is_pro_mode": trace.metadata.get("is_pro_mode", False),
                    "model_name": llm.get("model_name", "gemini-2.5-pro"),
                    "temperature": llm.get("temperature"),
                    "max_output_tokens": llm.get("max_output_tokens"),
                    "system_instruction": llm.get("system_instruction"),
                    "prompt_text": llm.get("prompt_text"),
                    "response_text": llm.get("response_text"),
                    "latency_ms": llm.get("latency_ms"),
                    "start_datetime": s.get("start_datetime"),
                    "end_datetime": s.get("end_datetime"),
                    "prompt_tokens": llm.get("prompt_tokens"),
                    "completion_tokens": llm.get("completion_tokens"),
                    "total_tokens": llm.get("total_tokens"),
                    "status": llm.get("status", "SUCCESS"),
                    "safety_ratings": json.dumps(llm.get("safety_ratings", {}), default=str),
                    "error_message": llm.get("error_message"),
                    "created_at": datetime.now(timezone.utc).isoformat()
                })
                
            errors = self.client.insert_rows_json(self._table("llm_calls"), rows)
            if errors:
                logger.error(f"BQ Insert Error (llm_calls): {errors}")
        except Exception as e:
            logger.error(f"Failed to prepare llm_calls telemetry: {e}")

    def write_rag(self, trace: Trace) -> None:
        if not trace.rag_citations:
            return
            
        try:
            rows = []
            for c in trace.rag_citations:
                rows.append({
                    "trace_id": trace.trace_id,
                    "source": json.dumps(c.get("source", {}), default=str),
                    "latency_ms": c.get("latency_ms"), 
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "start_datetime": s.get("start_datetime"),
                    "end_datetime": s.get("end_datetime"),
                })
                
            errors = self.client.insert_rows_json(self._table("rag_citations"), rows)
            if errors:
                logger.error(f"BQ Insert Error (rag_citations): {errors}")
        except Exception as e:
            logger.error(f"Failed to prepare rag_citations telemetry: {e}")

    # ==========================================
    # MASTER FLUSH WRAPPER
    # ==========================================
    def write_all_logs(self, trace: Trace, status: str = "OK", final_intent: str = "unknown", answer_len: int = 0) -> None:
        """
        A unified helper method that safely flushes all tables at once.
        Use this inside your background threads to ensure clean execution.
        """
        try:
            self.write_routing(trace)
            self.write_tool_calls(trace)
            self.write_sql_audit(trace)
            self.write_llm_calls(trace)
            self.write_rag(trace)
            # Write the parent trace last
            self.write_trace(trace, status=status, final_intent=final_intent, answer_len=answer_len)
        except Exception as e:
            logger.error(f"Critical failure while flushing full telemetry block: {e}") 
