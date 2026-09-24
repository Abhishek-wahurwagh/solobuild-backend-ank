from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, ConfigDict


class CallInitiationRequest(BaseModel):
    candidate_id: UUID
    campaign_id: UUID
    candidate_phone: str
    system_prompt: str | None = None
    required_fields: dict | None = None
    raw_text: str | None = None


class CallInitiationResponse(BaseModel):
    call_id: str
    status: str = "SUCCESS"
    carrier: str | None = None

    model_config = ConfigDict(from_attributes=True)


class CallCompletionWebhook(BaseModel):
    call_id: str
    candidate_id: UUID
    campaign_id: UUID
    status: str
    transcript: str | None = None
    recording_url: str | None = None


class CallStatusResponse(BaseModel):
    call_id: str
    status: str
    candidate_id: UUID | None = None
    campaign_id: UUID | None = None
    transcript: str | None = None
    recording_url: str | None = None
