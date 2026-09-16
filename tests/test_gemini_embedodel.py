import os
import pytest

@pytest.mark.skipif(not os.getenv("GEMINI_API_KEY"), reason="Requires GEMINI_API_KEY")
def test_gemini_embed_content():
    from google import genai
    client = genai.Client()
    response = client.models.embed_content(
        model="gemini-embedding-001",
        contents="Software Development Engineer (SDE) Department: Engineering & Technology | Employment Type: Full-Time",
    )
    assert len(response.embeddings[0].values) > 0