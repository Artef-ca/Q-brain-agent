from pydantic import BaseModel, Field
from typing import Any, Literal, List, Dict, Optional

from pydantic import BaseModel
from typing import Any, List, Dict, Optional

class EvidenceBlock(BaseModel):
    type: str
    title: str
    details: Dict[str, Any]

class ChatRequest(BaseModel):
    question: str
    user_id: str = "default_user"
    session_id: Optional[str] = None
    is_pro: bool = False 

class OutputData(BaseModel):
    session_id: str
    is_pro: bool              
    response: Any
    suggestions: List[str]
    trace_id: str
    intent: str = ""
    steps: List[str] = []
    stats: Dict = {}
    evidence: List[EvidenceBlock] = []

class ChatResponse(BaseModel):
    output: OutputData
    
# class ChatResponse(BaseModel):
#     trace_id: str
#     answer: str
#     steps: list[str]
#     stats: dict[str, Any]
#     evidence: list[EvidenceBlock]
#     followups: list[str] = Field(default_factory=list)
