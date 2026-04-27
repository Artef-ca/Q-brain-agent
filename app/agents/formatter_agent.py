from __future__ import annotations
from typing import Any
import logging
import sys
from app.core.llm import LLM

logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class FormatterAgent:
    def __init__(self, llm: LLM) -> None:
        self.llm = llm

    def format_response(self, instruction: str, context_data: str, scope_data: str = "") -> dict[str, Any]:
        """
        Takes raw data/context and formats it into the Batch2 UI JSON structure.
        Also generates contextual follow-up suggestions for KPI stats.
        """
        
        # We define the strict UI schema the LLM must follow, now including 'suggestions'
        schema_hint = """
        Return ONLY valid JSON matching this exact structure. 
        The root MUST contain "type": "batch2", a "blocks" array, and a "suggestions" array.

        Valid block types: "kpi-card", "comparison", "paragraph-divider", "chart", "table" .

        **Chart block rules:**
        - Block "type" MUST be "chart"
        - "chartType" MUST be one of: "bar", "line", "area", "pie", "donut", "radar", "radial"
        - "height" MUST be exactly 300
        - "data" MUST be an array of objects
        - Every object in "data" MUST include "name" (the dimension label)
        - Data keys MUST be UI-friendly (no raw column names)
        
        Comparison block rules:
        - "items" MUST be an array
        - Each item MUST contain "label", "currentValue", and "change"
        - "trend.direction" MUST be one of: "up", "down"
        - "trend.color" MUST be on of : "red", "green" 
        
        Example JSON Structure:
        {
          "type": "batch2",
          "blocks": [
            {
              "type": "kpi-card",
              "title": "Total Revenue",
              "value": "$1.2M",
              "subtitle": "Q1 2024",
              "trend": {"value": "5%", "direction": "up", "color": "green"}
            },
            {
              "type": "chart",
              "chartType": "bar",
              "title": "Revenue by Region",
              "subtitle": "North America leads",
              "height": 300,
              "data": [{"name": "NA", "Revenue": 500000}, {"name": "EMEA", "Revenue": 300000}]
            },
            {
              "type": "comparison",
              "title": "Financial Spend Comparison",
              "subtitle": "Comparing total financial spend between 2024 and 2025",
              "items": [
                {
                  "label": "Total Financial Spend (2025)",
                  "currentValue": "255.49M SAR",
                  "change": {
                    "value": "-13.32M SAR",
                    "percentage": "-4.95%",
                    "direction": "down"
                  }
                }
              ]
            },
            {
              "type": "table",
              "headers": ["Region", "Revenue"],
              "rows": [["NA", "$500k"], ["EMEA", "$300k"]]
            },
            {
              "type": "paragraph-divider",
              "paragraphs": ["This is a text summary of the data."]
            }
          ],
          "suggestions": [
            "What is the breakdown by region?",
            "Show me the top 5 products by revenue.",
            "Compare this to Q1 2023."
          ]
        }
        """

        prompt = f"""
        You are the Formatter Agent for the UI. You are the FINAL step in the system.

        **CHART DATA SHAPING RULES (MANDATORY):**
        - NEVER pass raw BigQuery rows directly into chart.data.
        - You MUST transform data into UI-friendly chart datasets.
        - Limit chart data to a MAXIMUM of 5–7 categories. Group the rest as "Other".
        - Chart `height` must always be exactly 300.
        - Every data item MUST include a "name" field (dimension).

        **DATA NORMALIZATION RULES:**
        - You MUST NEVER expose raw database column names.
        - You MUST normalize chart data to UI-friendly keys (e.g., "Spend (SAR)", "Attendance").
        
        **GENERAL RULES:**
        - Limit chart data to a MAXIMUM of 5–7 categories
        - Prefer aggregated categories over individual records
        - Use short, human-readable labels
        - If user explicitly asks for a comparison, you MUST return a "comparison" block.
        - For "comparison" blocks, ONLY the keys: type, title, subtitle, items are allowed. Any other keys are invalid.

        **SUGGESTIONS RULES:**
        - Generate EXACTLY 3 short, relevant follow-up questions in the "suggestions" array.
        - If "Schema Scope" is provided below, use those metrics and dimensions to inspire highly relevant analytical questions.
        - Questions should be short, single-line, and unnumbered.

        **TASK:**
        User Instruction: "{instruction}"
        
        Schema Scope:
        {scope_data}
        
        Raw Data Context: 
        {context_data}

        Review the raw data context. Select the most appropriate UI blocks to visualize this data based on the user instruction, and populate the suggestions array.
        """
        
        logger.info("--- [FormatterAgent] Formatting response into UI Blocks & generating suggestions... ---")
        try:
            # Using your existing LLM json method
            ui_json = self.llm.json(prompt, schema_hint=schema_hint)
            return ui_json
        except Exception as e:
            logger.error(f"--- [FormatterAgent] Failed to format: {e} ---")
            # Fallback safe UI block if the LLM fails to output valid JSON
            return {
                "type": "batch2",
                "blocks": [{
                    "type": "paragraph-divider",
                    "paragraphs": ["I processed your data, but encountered an error visualizing it."]
                }],
                "suggestions": ["Can you try rephrasing your question?", "Show me a system overview."]
            }