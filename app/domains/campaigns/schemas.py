from typing import Any
from pydantic import BaseModel, ConfigDict
from app.domains.campaigns.models import EmploymentType, CandidateStatus
from uuid import UUID
from datetime import datetime


class CandidateResponse(BaseModel):
    """Returned when fetching candidate list."""
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    campaign_id: UUID
    name: str | None = None
    email: str | None = None
    phone: str | None = None
    location: str | None = None
    experience_years: int | None = None
    skills: list[str] = []
    status: CandidateStatus
    created_at: datetime


class BatchUploadResponse(BaseModel):
    """Returned by POST /campaigns/{campaign_id}/candidates/upload"""
    batch_id: str
    status: str = "QUEUED"
    accepted_files: int


class BatchStatusResponse(BaseModel):
    """Returned by GET /campaigns/{campaign_id}/candidates/upload/{batch_id}/status"""
    batch_id: str
    status: str
    total_files: int
    processed: int
    failed: int
    created_at: str | None = None
    updated_at: str | None = None
    finished_at: str | None = None
    candidates: list[CandidateResponse] | None = None


class ScreeningRequest(BaseModel):
    """Payload for POST /campaigns/{campaign_id}/candidates/screen"""
    candidate_ids: list[UUID] | None = None
