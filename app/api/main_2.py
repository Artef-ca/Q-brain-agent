from __future__ import annotations
from fastapi import FastAPI, HTTPException, BackgroundTasks
from app.core.schemas import ChatRequest, ChatResponse, EvidenceBlock, OutputData
from app.core.trace import Trace
from app.agents.root_agent import RootAgent
from app.telemetry.bq_telemetry import TelemetryWriter
import logging
import traceback
import warnings
import uuid
import time
from datetime import datetime, timezone # Added for accurate timestamping

# Suppress warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*Python version.*", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", message=".*feature is deprecated.*", category=UserWarning)

logger = logging.getLogger("qbrain")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Q-Brain Agent API", version="1.0")

root = RootAgent()
telemetry = TelemetryWriter()

# Dedicated background function for FastAPI
def write_telemetry_background(trace: Trace, status: str, final_intent: str, answer_len: int):
    try:
        # Use the master flush method!
        telemetry.write_all_logs(
            trace=trace, 
            status=status, 
            final_intent=final_intent, 
            answer_len=answer_len
        )
    except Exception as e:
        logger.error(f"Failed to write background telemetry: {e}")

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest, background_tasks: BackgroundTasks):
    start_time = time.time()
    global_start_dt = datetime.now(timezone.utc).isoformat() # Capture exact API entry
    
    current_session_id = req.session_id if req.session_id else str(uuid.uuid4())
    
    trace = Trace(session_id=current_session_id, user_id=req.user_id)
    trace.metadata["question"] = req.question
    trace.metadata["start_datetime"] = global_start_dt 
    
    result = {}
    status = "ERROR"
    
    try:
        # Extract the is_pro flag from the request
        is_pro_flag = getattr(req, "is_pro", False)
        
        # Pass the flag to the agent
        result = root.run(trace, req.question, is_pro_mode=is_pro_flag)
        status = "OK"
        
        evidence_blocks = [EvidenceBlock(**e) for e in result.get("evidence", [])]

        # Wrap in output logic for FastAPI local testing
        out_data = OutputData(
            session_id=current_session_id,
            is_pro=result.get("is_pro", False),
            response=result.get("response", {}),
            suggestions=result.get("suggestions", []),
            trace_id=trace.trace_id,
            intent=result.get("intent", ""),
            steps=result.get("steps", []),
            stats=result.get("stats", {}),
            evidence=evidence_blocks,
        )

        return ChatResponse(output=out_data)

    except Exception as e:
        trace.metadata["error_message"] = traceback.format_exc()
        # LOG TRACE ID ON FATAL CRASH
        logger.error(f"--- [Trace: {trace.trace_id}] FATAL CRASH OCCURRED ---")
        logger.error(trace.metadata["error_message"]) 
        # We don't raise immediately, we let the finally block run first
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # GUARANTEED EXECUTION: Compute latency, set exit time, and attach background task
        trace.metadata["latency_ms"] = int((time.time() - start_time) * 1000)
        trace.metadata["end_datetime"] = datetime.now(timezone.utc).isoformat() # Capture exact exit
        
        final_intent = result.get("intent", "unknown") if status == "OK" else "crash"
        
        # Safely calculate answer length
        response_content = result.get("response", "")
        answer_len = len(str(response_content)) if response_content else 0
        
        # This tells FastAPI to run the function after HTTP response is sent
        background_tasks.add_task(
            write_telemetry_background, 
            trace, 
            status, 
            final_intent, 
            answer_len
        )

@app.post("/chat_debug")
def chat_debug(req: ChatRequest):
    trace = Trace(session_id=req.session_id, user_id=req.user_id)
    is_pro_flag = getattr(req, "is_pro", False)
    result = root.run(trace, req.question, is_pro_mode=is_pro_flag)

    return {
        "trace_id": trace.trace_id,
        "routing": trace.routing,
        "tool_calls": trace.tool_calls,
        "sql_audit": trace.sql_audit,
        "rag": trace.rag_citations,
        "result": result,
    }
