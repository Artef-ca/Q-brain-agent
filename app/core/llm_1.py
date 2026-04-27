from typing import Any
# from google.cloud import aiplatform
import vertexai
from vertexai.generative_models import GenerativeModel, Part

from app.config import settings
import json
import re


class LLM:
    def __init__(self) -> None:
        # aiplatform.init(project=settings.project_id, location=settings.region)
        vertexai.init(project=settings.project_id, location=settings.region)
        self.model = GenerativeModel(settings.gemini_model)

    def text(self, prompt: str) -> str:
        resp = self.model.generate_content(prompt)
        if hasattr(resp, "text") and resp.text:
            return resp.text
        if resp.candidates:
            return resp.candidates[0].content.parts[0].text
        return ""
            

    def json(self, prompt: str, schema_hint: str = "") -> dict:
        full = f"""
            You MUST return ONLY valid JSON.
            No markdown.
            No explanation.
            No code fences.
            
            {schema_hint}
            
            {prompt}
            """
    
        resp = self.model.generate_content(full)
    
        # Extract text safely
        text = None
        if hasattr(resp, "text") and resp.text:
            text = resp.text
        elif getattr(resp, "candidates", None):
            try:
                text = resp.candidates[0].content.parts[0].text
            except Exception:
                pass
    
        if not text:
            raise ValueError("LLM returned empty response")
    
        text = text.strip()
    
        # 🔥 REMOVE MARKDOWN CODE FENCES
        if text.startswith("```"):
            text = re.sub(r"^```json\s*", "", text)
            text = re.sub(r"^```\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
    
        text = text.strip()
    
        # Final attempt
        try:
            return json.loads(text)
        except Exception as e:
            raise ValueError(f"Failed to parse JSON from LLM output: {text[:500]}") from e



