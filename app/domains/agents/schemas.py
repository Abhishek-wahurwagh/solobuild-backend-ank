from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.domains.agents.models import (
    ConversationStyle,
    LanguagePreference,
    VoicePreference,
)


class AgentCreate(BaseModel):
    name: Annotated[str, Field(min_length=3, max_length=255)]
    conversation_style: ConversationStyle
    languages: LanguagePreference
    voice: VoicePreference
    interview_instruction: Annotated[str, Field(min_length=3, max_length=2000)]

    model_config = ConfigDict(use_enum_values=True)


class AgentResponse(BaseModel):
    id: UUID
    name: str
    conversation_style: ConversationStyle
    languages: LanguagePreference
    voice: VoicePreference
    interview_instruction: str

    model_config = ConfigDict(from_attributes=True, use_enum_values=True)


class AgentUpdate(BaseModel):
    name: str | None = None
    conversation_style: ConversationStyle | None = None
    languages: LanguagePreference | None = None
    voice: VoicePreference | None = None
    interview_instruction: str | None = None

    model_config = ConfigDict(use_enum_values=True)

    @model_validator(mode="after")
    def validate_has_at_least_one_field(self):
        if all(
            getattr(self, field) is None
            for field in (
                "name",
                "conversation_style",
                "languages",
                "voice",
                "interview_instruction",
            )
        ):
            raise ValueError("At least one field must be provided for update.")
        return self