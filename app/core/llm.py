import json
import re
import time
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from google.cloud import modelarmor_v1
from typing import Any, List, Optional, Union
from app.config import settings 
from app.core.trace import Trace 
from app.core.logger import get_gcp_logger
from datetime import datetime, timezone 

logger = get_gcp_logger("llmAgent")

class LLM:
    def __init__(self) -> None:
        # Initialize Vertex AI
        vertexai.init(project=settings.project_id, location=settings.region)
        self.model = GenerativeModel(settings.gemini_model)
        
        # Initialize Model Armor Client
        self.armor_client = modelarmor_v1.ModelArmorClient(
            transport="rest",
            client_options={
                "api_endpoint": f"modelarmor.{settings.region}.rep.googleapis.com"
            }
        )
        self.template_name = f"projects/{settings.project_id}/locations/{settings.region}/templates/{settings.model_armor_template_id}"

    # Helper method to safely extract trace ID whether it's an object or a string
    def _get_trace_id(self, trace: Optional[Union[Trace, str]]) -> str:
        if hasattr(trace, "trace_id"):
            return trace.trace_id
        elif isinstance(trace, str):
            return trace
        return "NO_TRACE"

    # NOTE: This is now intended to be called ONLY ONCE from root_agent.py at the start of the run.
    def _sanitize_prompt(self, prompt: str, trace: Optional[Union[Trace, str]] = None) -> str:
        t_id = self._get_trace_id(trace)
        logger.info(f"--- [llmAgent] [Trace: {t_id}]. Prompt input: {prompt}... ---")

        """Validates input prompt and logs to trace."""
        start_ts = time.time()
        
        request = modelarmor_v1.SanitizeUserPromptRequest(
            name=self.template_name,
            user_prompt_data=modelarmor_v1.DataItem(text=prompt),
        )
        response = self.armor_client.sanitize_user_prompt(request=request)
        logger.info(f"--- [llmAgent] [Trace: {t_id}]. Sanitised response: {response}... ---")

        
        latency_ms = int((time.time() - start_ts) * 1000)
        is_match = response.sanitization_result.filter_match_state == modelarmor_v1.FilterMatchState.MATCH_FOUND
        logger.info(f"--- [llmAgent] [Trace: {t_id}]. Sanitised is_match : {is_match}... ---")

        
        # SAFE APPEND: Only append if trace is an actual Trace object
        if hasattr(trace, "model_armor_audit"):
            trace.model_armor_audit.append({
                "phase": "input_sanitization",
                "latency_ms": latency_ms,
                "action": "BLOCKED" if is_match else "ALLOWED",
                "is_safe": not is_match
            })
            
        if is_match:
            raise ValueError("Security Alert: Input prompt violated Model Armor safety policies.")
            
        return prompt 

    def _sanitize_response(self, response_text: str, original_prompt: str, trace: Optional[Union[Trace, str]] = None) -> str:
        """Validates LLM output, handles redaction, and logs to trace."""
        start_ts = time.time()
        
        request = modelarmor_v1.SanitizeModelResponseRequest(
            name=self.template_name,
            model_response_data=modelarmor_v1.DataItem(text=response_text),
        )
        response = self.armor_client.sanitize_model_response(request=request)
        
        t_id = self._get_trace_id(trace)
        logger.info(f"--- [llmAgent] [Trace: {t_id}]. Prompt response processed. ---")
        
        latency_ms = int((time.time() - start_ts) * 1000)
        is_match = response.sanitization_result.filter_match_state == modelarmor_v1.FilterMatchState.MATCH_FOUND
        
        action = "ALLOWED"
        final_text = response_text
        
        if is_match:
            sanitized_text = response.sanitization_result.sanitized_data.text
            if sanitized_text:
                action = "REDACTED"
                final_text = sanitized_text
            else:
                action = "BLOCKED"

        # SAFE APPEND: Only append if trace is an actual Trace object
        if hasattr(trace, "model_armor_audit"):
            trace.model_armor_audit.append({
                "phase": "output_sanitization",
                "latency_ms": latency_ms,
                "action": action,
                "is_safe": not is_match
            })
            
        if action == "BLOCKED":
            raise ValueError("Security Alert: LLM response violated Model Armor safety policies.")
            
        return final_text

    def text(self, prompt: str, trace: Optional[Union[Trace, str]] = None) -> str:
        # We NO LONGER sanitize the prompt here. It assumes the RootAgent already handled the raw user input.
        
        # Call LLM
        resp = self.model.generate_content(prompt)
        
        raw_text = ""
        if hasattr(resp, "text") and resp.text:
            raw_text = resp.text
        elif getattr(resp, "candidates", None):
            raw_text = resp.candidates[0].content.parts[0].text
            
        if not raw_text:
            return ""
            
        # Sanitize Output
        logger.info(f"--- [llmAgent] before sanitize : {datetime.now(timezone.utc).isoformat()}")
        safe_response = self._sanitize_response(raw_text, prompt, trace)
        logger.info(f"--- [llmAgent] after sanitize : {datetime.now(timezone.utc).isoformat()}")
        # safe_response = raw_text
        return safe_response

    def json(self, prompt: str, schema_hint: str = "", trace: Optional[Union[Trace, str]] = None) -> dict:
        logger.info(f"--- [llmAgent] [JSON response processing Started. ---")
        full_prompt = f"""
            You MUST return ONLY valid JSON.
            No markdown.
            No explanation.
            No code fences.
            
            {schema_hint}
            
            {prompt}
            """
            
        # Call LLM
        resp = self.model.generate_content(full_prompt)
        logger.info(f"--- [llmAgent] [JSON response processing LLM completed. ---")
        
        # Extract text safely
        raw_text = None
        if hasattr(resp, "text") and resp.text:
            raw_text = resp.text
        elif getattr(resp, "candidates", None):
            try:
                raw_text = resp.candidates[0].content.parts[0].text
            except Exception:
                pass
    
        if not raw_text:
            raise ValueError("LLM returned empty response")
            
        # Sanitize Output 
        logger.info(f"--- [llmAgent] [JSON response Sanitizing LLM Started. ---")
        safe_text = self._sanitize_response(raw_text, full_prompt, trace)
        logger.info(f"--- [llmAgent] [JSON response Sanitizing LLM Completed. ---")
        safe_text = safe_text.strip()
    
        # REMOVE MARKDOWN CODE FENCES
        if safe_text.startswith("```"):
            safe_text = re.sub(r"^```json\s*", "", safe_text)
            safe_text = re.sub(r"^```\s*", "", safe_text)
            safe_text = re.sub(r"\s*```$", "", safe_text)
    
        safe_text = safe_text.strip()
    
        # Final attempt to parse
        try:
            return json.loads(safe_text)
        except Exception as e:
            raise ValueError(f"Failed to parse JSON from LLM output: {safe_text[:500]}") from e