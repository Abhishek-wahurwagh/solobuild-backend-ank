from typing import Any
from pydantic import BaseModel, ConfigDict, Field
from app.domains.campaigns.models import WorkflowStepStatus
from uuid import UUID
from datetime import datetime


class WorkflowTemplateResponse(BaseModel):
    id: UUID
    template: dict
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class CampaignCreate(BaseModel):
    title: str
    workflow_template_id: UUID | None = None
    agent_id: UUID | None = None
    raw_text: str | None = None
    required_fields: dict | None = None


class CampaignResponse(BaseModel):
    id: UUID
    title: str
    workflow_template_id: UUID | None
    agent_id: UUID | None
    raw_text: str | None
    required_fields: dict | None
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)


class CandidateResponse(BaseModel):
    id: UUID
    campaign_id: UUID
    name: str | None = None
    email: str | None = None
    phone: str | None = None
    file_url: str | None = None
    ingestion_item_id: UUID | None = None
    extracted_fields: dict | None = None
    
    workflow_step: str | None = None
    step_status: WorkflowStepStatus
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)



class DocumentScreeningResponse(BaseModel):
    id: UUID
    match_score: float | None
    matched_fields: dict | None
    unmatched_fields: dict | None
    summary: str | None
    model_config = ConfigDict(from_attributes=True)

class CallScreeningResponse(BaseModel):
    id: UUID
    transcript: str | None
    recording_url: str | None
    match_score: float | None
    matched_fields: dict | None
    unmatched_fields: dict | None
    summary: str | None
    model_config = ConfigDict(from_attributes=True)

class WorkflowEventLogResponse(BaseModel):
    id: UUID
    campaign_id: UUID
    candidate_id: UUID
    service_name: str
    status: str
    payload: dict | None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)

class CallWebhookPayload(BaseModel):
    call_id: str
    candidate_id: UUID
    campaign_id: UUID
    status: str
    transcript: str | None = None
    recording_url: str | None = None


class BatchUploadResponse(BaseModel):
    batch_id: str
    status: str
    accepted_files: int
    rejected_files: int


class CampaignFieldsUpdate(BaseModel):
    """JSON body for directly overwriting campaign required_fields."""
    required_fields: dict


class BatchStatusResponse(BaseModel):
    batch_id: str
    status: str
    total_files: int
    processed: int
    failed: int
    created_at: str | None = None
    updated_at: str | None = None
    finished_at: str | None = None
    failed_files: list[str] = Field(default_factory=list)
    candidates: list[CandidateResponse] | None = None


class ScreeningRequest(BaseModel):
    candidate_ids: list[UUID] | None = None

class ScreeningBatchResponse(BaseModel):
    batch_id: str
    status: str

