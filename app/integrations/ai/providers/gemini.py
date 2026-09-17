import json
import re
from typing import Any

from app.integrations.ai.providers.base import StructuredExtractionProvider

class GeminiStructuredExtractor(StructuredExtractionProvider):
    def __init__(self, api_key: str, model: str = "gemini-3.6-flash"):
        try:
            from google import genai
        except ImportError as exc:
            raise RuntimeError("The google-genai package is required for the structured extraction provider.") from exc

        self.client = genai.Client(api_key=api_key)
        self.model = model

    async def extract(self, document_text: str, existing_fields: dict | None = None) -> dict[str, Any]:
        existing_str = json.dumps(existing_fields) if existing_fields else "{}"
        
        prompt = f"""
You are a generic document extraction assistant.
You will be given the raw text of a document, as well as an existing JSON object of fields that have already been provided.
Your task is to extract relevant structured information from the document text and return it as a JSON object.

RULES:
1. Retain all keys and values from the `Existing Fields`. Do NOT overwrite or delete them.
2. Extract new traits, properties, or attributes from the document and add them to the JSON object.
3. Keep values concise. Use strings, numbers, lists of strings, or booleans.
4. Output valid JSON only, without any conversational text or markdown formatting.

Existing Fields:
{existing_str}

Document Text:
{document_text}
"""

        response = await self.client.aio.models.generate_content(
            model=self.model,
            contents=prompt,
            config={
                "temperature": 0.0,
                "response_mime_type": "application/json",
            },
        )

        text = getattr(response, "text", None)
        if not text:
            raw_text = str(response)
            text = raw_text

        json_text = text.strip()
        if json_text.startswith("```"):
            json_text = re.sub(r"^```json\s*", "", json_text, flags=re.IGNORECASE)
            json_text = re.sub(r"```$", "", json_text)

        try:
            payload = json.loads(json_text)
        except json.JSONDecodeError:
            raise RuntimeError("Structured extractor returned invalid JSON.")

        if not isinstance(payload, dict):
            raise RuntimeError("Structured extractor returned a non-object payload.")

        return payload

    async def screen_candidate(
        self, 
        candidate_text: str, 
        candidate_fields: dict, 
        campaign_text: str, 
        campaign_fields: dict
    ) -> dict[str, Any]:
        
        prompt = f"""
You are an expert screening assistant. You are evaluating a Candidate against a Campaign (job, role, or generic requirement).

Campaign Raw Text:
{campaign_text}

Campaign Required Fields:
{json.dumps(campaign_fields)}

Candidate Raw Text:
{candidate_text}

Candidate Extracted Fields:
{json.dumps(candidate_fields)}

Your task is to compare the Candidate against the Campaign and output a JSON object with exactly these keys:
- "match_score": A float between 0 and 100 representing how well the candidate matches the campaign requirements.
- "one_line_summary": A concise one-sentence summary of the candidate's fit.
- "matched_fields": A JSON object containing the properties/skills/requirements that the candidate successfully meets.
- "unmatched_fields": A JSON object containing the properties/skills/requirements that the candidate is missing or fails to meet.

Output valid JSON only.
"""
        response = await self.client.aio.models.generate_content(
            model=self.model,
            contents=prompt,
            config={
                "temperature": 0.0,
                "response_mime_type": "application/json",
            },
        )

        text = getattr(response, "text", None)
        if not text:
            raw_text = str(response)
            text = raw_text

        json_text = text.strip()
        if json_text.startswith("```"):
            json_text = re.sub(r"^```json\s*", "", json_text, flags=re.IGNORECASE)
            json_text = re.sub(r"```$", "", json_text)

        try:
            payload = json.loads(json_text)
        except json.JSONDecodeError:
            raise RuntimeError("Screening extractor returned invalid JSON.")

        if not isinstance(payload, dict):
            raise RuntimeError("Screening extractor returned a non-object payload.")

        return {
            "match_score": float(payload.get("match_score", 0.0)),
            "one_line_summary": str(payload.get("one_line_summary", "")),
            "matched_fields": payload.get("matched_fields", {}),
            "unmatched_fields": payload.get("unmatched_fields", {}),
            "summary": str(payload.get("one_line_summary", ""))
        }
