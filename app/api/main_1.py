from __future__ import annotations
from fastapi import FastAPI, HTTPException
from app.core.schemas import ChatRequest, ChatResponse, EvidenceBlock, OutputData
from app.core.trace import Trace
from app.agents.root_agent import RootAgent
from app.telemetry.bq_telemetry import TelemetryWriter
import logging
import traceback
import warnings
import uuid

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

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    current_session_id = req.session_id if req.session_id else str(uuid.uuid4())
    
    trace = Trace(session_id=current_session_id, user_id=req.user_id)
    try:
        result = root.run(trace, req.question)
        
        # Persist telemetry
        telemetry.write_routing(trace)
        telemetry.write_tool_calls(trace)
        telemetry.write_sql_audit(trace)
        telemetry.write_rag(trace)
        telemetry.write_trace(trace, status="OK", final_intent=result.get("intent",""), answer_len=len(result.get("response","")))

        evidence_blocks = [EvidenceBlock(**e) for e in result.get("evidence", [])]

        import json
        def _safe(obj):
            return json.dumps(obj, indent=2, default=str)
    
        print("\n================ TRACE ================")
        print("TRACE ID:", trace.trace_id)
        print("ROUTING:", json.dumps(trace.routing, indent=2))
        print("TOOL CALLS:", json.dumps(trace.tool_calls, indent=2))
        print("SQL AUDIT:", json.dumps(trace.sql_audit, indent=2))
        print("RAG:", json.dumps(trace.rag_citations, indent=2))
        print("=======================================\n")

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
        logger.error("ERROR OCCURRED")
        logger.error(traceback.format_exc()) 
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/chat_debug")
def chat_debug(req: ChatRequest):
    trace = Trace(session_id=req.session_id, user_id=req.user_id)
    result = root.run(trace, req.question)

    return {
        "trace_id": trace.trace_id,
        "routing": trace.routing,
        "tool_calls": trace.tool_calls,
        "sql_audit": trace.sql_audit,
        "rag": trace.rag_citations,
        "result": result,
    }
