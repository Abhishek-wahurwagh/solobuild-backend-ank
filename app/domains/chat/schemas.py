import enum
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# UI Action Types — single source of truth for frontend component dispatch.
# When you add a new renderable component on the frontend, add its key here.
# ---------------------------------------------------------------------------

class UIActionType(str, enum.Enum):
    # Campaign views
    SHOW_CAMPAIGN_LIST   = "SHOW_CAMPAIGN_LIST"     # renders CampaignListPanel
    SHOW_CAMPAIGN_DETAIL = "SHOW_CAMPAIGN_DETAIL"   # renders CampaignDetailPanel
    SHOW_CAMPAIGN_CREATE_FORM = "SHOW_CAMPAIGN_CREATE_FORM"  # renders CampaignCreateModal/Form

    # Candidate views
    SHOW_CANDIDATE_LIST  = "SHOW_CANDIDATE_LIST"    # renders CandidateTablePanel
    SHOW_CANDIDATE_UPLOAD = "SHOW_CANDIDATE_UPLOAD" # renders CandidateUploadModal/Dropzone

    # Batch / progress views
    SHOW_SCREENING_STATUS = "SHOW_SCREENING_STATUS" # renders BatchStatusPanel (screening)
    SHOW_BATCH_STATUS     = "SHOW_BATCH_STATUS"     # renders BatchStatusPanel (generic)

    # Disambiguation
    SHOW_CAMPAIGN_PICKER  = "SHOW_CAMPAIGN_PICKER"  # renders CampaignSelectorCard


class UIAction(BaseModel):
    """
    Tells the frontend which component to render alongside the bot's text.
    The frontend maps `type` → React component and spreads `payload` as props.
    """
    type: UIActionType
    payload: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Request / Response shapes
# ---------------------------------------------------------------------------

class SendMessageRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=4000)


class ChatMessageResponse(BaseModel):
    """A single persisted turn returned to the client."""
    id: UUID
    session_id: UUID
    role: str
    # We only surface text content to the client, not raw Gemini parts.
    text: str | None = None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class BotResponse(BaseModel):
    """The structured reply the chat endpoint returns after processing a message."""
    # Plain-language reply from the model.
    text: str
    # Optional instruction to the frontend to render a specific UI component.
    ui_action: UIAction | None = None


class CreateSessionRequest(BaseModel):
    title: str | None = Field(None, max_length=255)


class ChatSessionResponse(BaseModel):
    id: UUID
    title: str | None
    summary: str | None
    turn_count: int
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)


class ChatSessionListItem(BaseModel):
    id: UUID
    title: str | None
    turn_count: int
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)
