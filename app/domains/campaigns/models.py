import enum
import uuid
from datetime import datetime
from uuid6 import uuid7
from sqlalchemy import String, Integer, Float, Text, Enum, ForeignKey, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB

from app.core.database import Base


class CampaignStatus(str, enum.Enum):
    DRAFT = "DRAFT"
    SCREENING = "SCREENING"
    PAUSED = "PAUSED"
    SCREENED = "SCREENED"
    COMPLETED = "COMPLETED"


class EmploymentType(str, enum.Enum):
    FULL_TIME = "FULL_TIME"
    PART_TIME = "PART_TIME"
    INTERNSHIP = "INTERNSHIP"
    CONTRACT = "CONTRACT"


class HiringCampaign(Base):
    __tablename__ = "hiring_campaigns"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid7)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)

    job_title: Mapped[str] = mapped_column(String(255), nullable=False)
    location: Mapped[str] = mapped_column(String(255), nullable=True)
    employment_type: Mapped[EmploymentType] = mapped_column(
        Enum(EmploymentType), default=EmploymentType.FULL_TIME, nullable=False
    )
    role_summary: Mapped[str] = mapped_column(Text, nullable=True)
    
    status: Mapped[CampaignStatus] = mapped_column(
        Enum(CampaignStatus), default=CampaignStatus.DRAFT, nullable=False
    )

    job_description_raw: Mapped[str] = mapped_column(Text, nullable=False)
    
    jd_extracted_data: Mapped[dict] = mapped_column(JSONB, nullable=True, default=dict) 

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), onupdate=func.now(), nullable=True)


class CandidateStatus(str, enum.Enum):
    PENDING = "PENDING"
    RESUME_PARSED = "RESUME_PARSED"
    RESUME_SCREENED = "RESUME_SCREENED"
    CALL_SCHEDULED = "CALL_SCHEDULED"
    CALL_COMPLETED = "CALL_COMPLETED"
    REJECTED = "REJECTED"
    PASSED = "PASSED"


class Candidate(Base):
    __tablename__ = "candidates"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid7)
    campaign_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("hiring_campaigns.id"), nullable=False, index=True)

    name: Mapped[str] = mapped_column(String(255), nullable=True)
    email: Mapped[str] = mapped_column(String(255), nullable=True)
    phone: Mapped[str] = mapped_column(String(50), nullable=True)
    location: Mapped[str] = mapped_column(String(255), nullable=True)
    
    experience_years: Mapped[int] = mapped_column(Integer, nullable=True)
    skills: Mapped[list] = mapped_column(JSONB, nullable=True, default=list)

    raw_resume_url: Mapped[str] = mapped_column(Text, nullable=True)
    resume_text: Mapped[str] = mapped_column(Text, nullable=True)

    status: Mapped[CandidateStatus] = mapped_column(
        Enum(CandidateStatus), default=CandidateStatus.PENDING, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ResumeScreening(Base):
    __tablename__ = "resume_screenings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid7)
    candidate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("candidates.id"), nullable=False, index=True)

    match_score: Mapped[float] = mapped_column(Float, nullable=True)  # e.g., 82.5
    one_line_summary: Mapped[str] = mapped_column(String(512), nullable=True)
    
    matched_skills: Mapped[list] = mapped_column(JSONB, nullable=True, default=list)
    missing_skills: Mapped[list] = mapped_column(JSONB, nullable=True, default=list)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)