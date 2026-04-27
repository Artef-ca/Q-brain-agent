
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

    def format_response(self, instruction: str, context_data: str) -> dict[str, Any]:
        """
        Takes raw data/context and formats it into the Batch2 UI JSON structure.
        """
        
        # We define the strict UI schema the LLM must follow
        schema_hint = """
        Return ONLY valid JSON matching this exact structure. 
        The root MUST contain "type": "batch2" and a "blocks" array.
        
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
              "type": "table",
              "headers": ["Region", "Revenue"],
              "rows": [["NA", "$500k"], ["EMEA", "$300k"]]
            },
            {
              "type": "paragraph-divider",
              "paragraphs": ["This is a text summary of the data."]
            }
          ]
        }
        Valid block types: "kpi-card", "comparison", "paragraph-divider", "chart", "table".
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

        **TASK:**
        User Instruction: "{instruction}"
        
        Raw Data Context: 
        {context_data}

        Review the raw data context. Select the most appropriate UI blocks to visualize this data based on the user instruction.
        """
        
        logger.info("--- [FormatterAgent] Formatting response into UI Blocks... ---")
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
                }]
            }



# from google.adk.agents import LlmAgent
# from google.genai import types

# # Import your schema and prompt classes from your app structure
# from app.schema.prompt import Prompt
# from app.schema.batch2_schema import Batch2Response

# # Initialize the prompt configuration
# prom = Prompt()

# # Initialize the Formatter Agent
# formatter_agent = LlmAgent(
#     name="formatter_agent",
#     model="gemini-2.5-flash",  # Hardcoded model instead of using config
#     description="Generates final UI batch2 response. Chooses blocks and formats output.",
#     instruction=prom.formatter_agent_prompt,
#     output_schema=Batch2Response,
#     generate_content_config=types.GenerateContentConfig(
#         temperature=0.2
#     ),
# )



# from google.adk.agents import LlmAgent
# from google.genai import types

# from .. import config
# from app.agents.schema import prompt
# from app.agents.schema.batch2_schema import Batch2Response


# conf = config.Config()
# prom = prompt.Prompt()

# formatter_agent = LlmAgent(
#     name="formatter_agent",
#     model=getattr(conf, "FORMATTER_MODEL", "gemini-2.5-flash"),
#     description="Generates final UI batch2 response. Chooses blocks and formats output.",
#     instruction=prom.formatter_agent_prompt,
#     output_schema=Batch2Response,
#     generate_content_config=types.GenerateContentConfig(
#         temperature=0.2
#     ),
# )

