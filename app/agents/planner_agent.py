from __future__ import annotations
from typing import Any
from app.core.llm import LLM
import json
import logging
import sys


logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class PlannerAgent:
    def __init__(self, llm: LLM) -> None:
        self.llm = llm


    def plan(self, question: str, history_context: str = "", max_insights: int = 5) -> dict[str, Any]:
        # logger.info(f"--- [Planner agent] history_context: {history_context} ---")
        schema_hint = """
                Return JSON like:
                {
                  "intent": "kpi_stats|root_cause|doc_qa|mixed|greeting|refinement",
                  "needs_bq": true/false,
                  "needs_rag": true/false,
                  "needs_forecast": true/false,
                  "metrics": ["..."],
                  "time_window_days": 90
                }
                """
        prompt = f"""
        You are an intelligent planning engine for an enterprise data assistant.
        
        CONVERSATION HISTORY:
        {history_context}
        
        CURRENT USER QUESTION: "{question}".


        
        Also, Your goal is to determine the intent and which tools are needed.
        

        ### AVAILABLE INTENTS & TOOLS
        1. **Greeting** ("greeting"):
           - Use for "Hi", "Hello", "Thanks", "Bye", or generic small talk.  
           - Set `needs_bq`, `needs_rag`, `needs_forecast` to **false**.

        2. **KPI & Stats** ("kpi_stats"):
           - Use for "List employees", "Count revenue", "Show sales by region".
           - Set `needs_bq` = **true**.

        3. **Doc QA** ("doc_qa"):
           - Use for "What is the policy on X?", "How to apply for leave?".
           - Set `needs_rag` = **true**.

        4. **Synthesis / Mixed** ("mixed"):
           - Use for "Why did sales drop?" or "Analyze performance context".
           - This requires BOTH data and text context.
           - Set **BOTH** `needs_bq` = **true** AND `needs_rag` = **true**.
           
        5. **Refinement** ("refinement"):
           - Use when the user wants to modify the PREVIOUS answer. 
           - Examples: "Make it simpler", "Summarize that", "Translate to Spanish", "Format as a table".
           - Tools: All FALSE.
           
        
        
        Output valid JSON only:
        {{
            "intent": "category_of_request",
            "needs_bq": boolean,
            "needs_rag": boolean,
            "metrics": ["list of potential metric names"],
            "time_window_days": int (default 90)
        }}
        """
        
        return self.llm.json(prompt, schema_hint=schema_hint)


        # Available Tools:
        # 1. BigQuery (needs_bq): Use for structured data, tables, counts, lists of entities, aggregations, metrics.
        #    - HINT: If the user asks for a "list" of items discussed in the history (e.g., "list those employees"), use BigQuery.
        # 2. RAG/Docs (needs_rag): Use for policy documents, unstructured text reports, "how to" guides.
        # 3. Greeting (is_greeting): Set to true if the user is just saying "hi", "thanks", "bye", or making small talk with no data request.
        # Decide:
        # #             - If it needs structured metrics from BigQuery
        # #             - If it needs document RAG
        # #             - If forecasting is required
        # #             - What metrics to compute




        # prompt = f"""
        #             Question: {question}
                    
        #             Decide:
        #             - If it needs structured metrics from BigQuery
        #             - If it needs document RAG
        #             - If forecasting is required
        #             - What metrics to compute
        #             """