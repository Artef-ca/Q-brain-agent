from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, List, Optional
import time
import uuid
from typing import Any

@dataclass
class Trace:
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    session_id: str = ""
    user_id: str = ""
    start_ts: float = field(default_factory=time.time)
    
    routing: list[dict[str, Any]] = field(default_factory=list)
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    sql_audit: list[dict[str, Any]] = field(default_factory=list)
    rag_citations: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    llm_calls: list[dict[str, Any]] = field(default_factory=list)
    model_armor_audit: list[dict[str, Any]] = field(default_factory=list) 

    def elapsed_ms(self) -> int:
        return int((time.time() - self.start_ts) * 1000)
