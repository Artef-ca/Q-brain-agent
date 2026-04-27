from __future__ import annotations
from typing import Any, Dict, Optional
import threading  # Added for background telemetry
import uuid       # Added for auto session generation
import time       # Added for exact latency tracking
import traceback  # Added for error capturing
from datetime import datetime, timezone # Added for timestamping
from app.core.logger import get_gcp_logger
from app.core.trace import Trace

logger = get_gcp_logger("QBrainEngine")

class QBrainReasoningEngine:
    """
    Must be picklable. Do NOT create google cloud clients in __init__.
    """
    def __init__(self) -> None:
        self._root = None
        self._telemetry = None

    def set_up(self) -> None:
        """
        Vertex AI automatically calls this method when the container starts.
        All unpicklable logic and heavy library imports go here so the agent is warm.
        """
        # Import inside to avoid pickling issues during deployment
        from app.agents.root_agent import RootAgent
        from app.telemetry.bq_telemetry import TelemetryWriter
        
        self._root = RootAgent()
        self._telemetry = TelemetryWriter()

    def _write_telemetry_background(self, trace: Trace, status: str, final_intent: str, answer_len: int) -> None:
        """Helper method to run all BigQuery inserts without blocking the user response."""
        try:
            # Use the master wrapper to flush all tables
            self._telemetry.write_all_logs(
                trace=trace, 
                status=status, 
                final_intent=final_intent, 
                answer_len=answer_len
            )
        except Exception as e:
            # Fallback: try to write at least the main trace if parallel tables fail
            try:
                self._telemetry.write_trace(trace, status="TELEMETRY_ERROR", final_intent=final_intent, answer_len=answer_len)
            except Exception:
                pass

    def query(self, request: Dict[str, Any]) -> Dict[str, Any]:
        start_time = time.time()
        global_start_dt = datetime.now(timezone.utc).isoformat()
        
        question = (request.get("query") or "").strip()
        if not question:
            return {"ok": False, "error": "Missing 'question' in request."}

        # --- AUTO SESSION GENERATION ---
        raw_session = request.get("sessionid")
        session_id = str(raw_session) if raw_session else str(uuid.uuid4())
        user_id = str(request.get("userid") or "default_user")

        trace = Trace(session_id=session_id, user_id=user_id)
        trace.metadata["question"] = question
        trace.metadata["start_datetime"] = global_start_dt # Capture exact API entry
        
        result = {}
        status = "ERROR"

        try:
            # Extract the is_pro flag and safely convert it to a boolean
            is_pro_raw = request.get("is_pro", False)
            is_pro_flag = str(is_pro_raw).lower() == 'true' if isinstance(is_pro_raw, str) else bool(is_pro_raw)
            
            # 1. Get the answer from your agent, passing the flag down
            result = self._root.run(trace, question, is_pro_mode=is_pro_flag)
            status = "OK"

        except Exception as e:
            # Catch crashes so the container doesn't die, and record the exact error stack
            error_stack = traceback.format_exc()
            trace.metadata["error_message"] = error_stack
            
            # Print to standard output so Google Cloud Logging catches the fatal error with the trace ID!
            logger.error(f"FATAL CRASH: {error_stack}",extra={"trace_id": trace.trace_id})
            
            print(f"--- [Trace: {trace.trace_id}] FATAL CRASH: {error_stack}")
            
            result = {"error": str(e)}

        finally:
            # 2. GUARANTEED EXECUTION: This runs even if the agent crashed.
            trace.metadata["latency_ms"] = int((time.time() - start_time) * 1000)
            trace.metadata["end_datetime"] = datetime.now(timezone.utc).isoformat() # Capture exact exit
            
            final_intent = result.get("intent", "unknown") if status == "OK" else "crash"
            
            # Safely calculate answer length
            response_content = result.get("response", "")
            answer_len = len(str(response_content)) if response_content else 0
            
            # Fire off telemetry in a background thread (DOES NOT BLOCK!)
            bg_thread = threading.Thread(
                target=self._write_telemetry_background, 
                args=(trace, status, final_intent, answer_len)
            )
            bg_thread.start()

        # 3. Immediately return the answer to the user
        if status == "ERROR":
            return {"ok": False, "trace_id": getattr(trace, "trace_id", None), "error": result.get("error")}

        return {
            "session_id": session_id,
            "response": result.get("response", ""),
            "suggestions": result.get("suggestions", []),
            "trace_id": trace.trace_id,
            "intent": result.get("intent", ""),
            "steps": result.get("steps", []),
            "stats": result.get("stats", {}),
            "evidence": result.get("evidence", []),
            "is_pro": result.get("is_pro", False) 
        }

# Instantiate for local testing/deployment binding
local_app = QBrainReasoningEngine()
