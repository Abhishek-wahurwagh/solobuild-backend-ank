import json
import re
from typing import Any

from app.integrations.ai.providers.base import (
    EmbeddingProvider,
    StructuredExtractionProvider,
)


class OpenAIEmbeddingProvider(EmbeddingProvider):
    def __init__(self, api_key: str, model: str = "text-embedding-3-small"):
        try:
            from openai import AsyncOpenAI
        except ImportError as exc:
            raise RuntimeError("The openai package is required for the OpenAI embedding provider.") from exc

        self.client = AsyncOpenAI(api_key=api_key)
        self.model = model

    async def embed(self, text: str) -> list[float]:
        response = await self.client.embeddings.create(
            model=self.model,
            input=text,
        )
        return response.data[0].embedding


class GeminiEmbeddingProvider(EmbeddingProvider):
    def __init__(self, api_key: str, model: str = "models/text-embedding-004"):
        try:
            from google import genai
        except ImportError as exc:
            raise RuntimeError("The google-genai package is required for the Gemini embedding provider.") from exc

        self.client = genai.Client(api_key=api_key)
        self.model = model

    async def embed(self, text: str) -> list[float]:
        response = await self.client.aio.models.embed_content(
            model=self.model,
            contents=text,
            config={"output_dimensionality": 384,
                    "task_type": "RETRIEVAL_DOCUMENT"},
        )

        embedding = None
        if hasattr(response, "embeddings"):
            embeddings = response.embeddings
            if embeddings:
                embedding = embeddings[0].values
        elif isinstance(response, dict):
            embedding = response.get("embedding")

        if embedding is None:
            raise RuntimeError("Gemini embedding response did not include an embedding vector.")

        return list(embedding)


class GeminiStructuredExtractor(StructuredExtractionProvider):
    def __init__(self, api_key: str, model: str = "gemini-3.6-flash"):
        try:
            from google import genai
        except ImportError as exc:
            raise RuntimeError("The google-genai package is required for the structured extraction provider.") from exc

        self.client = genai.Client(api_key=api_key)
        self.model = model

    async def extract(self, jd_text: str) -> dict[str, Any]:
        prompt = """
You are a deterministic JD extraction assistant. Extract only JSON.
Return a JSON object with exactly these keys:
{
  "experience_required": str | null,
  "skills_required": list[str]
}

Rules:
- Do not invent fields.
- Use the same text as appears in the JD for normalized fields.
- Keep `skills_required` limited to the strict minimum skills required by the JD.
- Output valid JSON only.

Job description:
""" + jd_text

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

        payload = json.loads(json_text)
        if not isinstance(payload, dict):
            raise RuntimeError("Structured JD extractor returned a non-object payload.")

        return {
            "experience_required": payload.get("experience_required"),
            "skills_required": payload.get("skills_required") or [],
        }

    async def screen_candidate(self, prompt: str) -> dict[str, Any]:
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

        payload = json.loads(json_text)
        if not isinstance(payload, dict):
            raise RuntimeError("Structured JD extractor returned a non-object payload for candidate screening.")

        return {
            "match_score": payload.get("match_score"),
            "one_line_summary": payload.get("one_line_summary", ""),
            "matched_skills": payload.get("matched_skills") or [],
            "missing_skills": payload.get("missing_skills") or [],
        }