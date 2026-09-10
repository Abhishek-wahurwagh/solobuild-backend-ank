import uuid
from datetime import datetime
from enum import Enum

from uuid6 import uuid7
from sqlalchemy import DateTime, ForeignKey, String, func, Enum as SAEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class ConversationStyle(str, Enum):
    FRIENDLY_CONVERSATIONAL = "Friendly Conversational"
    CONVERSATIONAL = "Conversational"
    FORMAL = "Formal"
    PROFESSIONAL = "Professional"

class LanguagePreference(str, Enum):
    ENGLISH = "English"
    ENGLISH_HINDI = "English + Hindi"
    ENGLISH_HINDI_MARATHI = "English + Hindi + Marathi"


class VoicePreference(str, Enum):
    WARM_CLEAR = "Warm & Clear"
    CLEAR_CONFIDENT = "Clear & Confident"
    NATURAL_CLEAR = "Natural & Clear"
    SOFT_PROFESSIONAL = "Soft & Professional"


class Agent(Base):
    __tablename__ = "agents"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid7,
    )

    name: Mapped[str] = mapped_column(String(255), nullable=False)

    conversation_style: Mapped[ConversationStyle] = mapped_column(
        SAEnum(
            ConversationStyle,
            native_enum=False,
            validate_strings=True,
        ),
        nullable=False,
    )

    languages: Mapped[LanguagePreference] = mapped_column(
        SAEnum(
            LanguagePreference,
            native_enum=False,
            validate_strings=True,
        ),
        nullable=False,
    )

    voice: Mapped[VoicePreference] = mapped_column(
        SAEnum(
            VoicePreference,
            native_enum=False,
            validate_strings=True,
        ),
        nullable=False,
    )

    interview_instruction: Mapped[str] = mapped_column(
        String(2000),
        nullable=False,
    )

    created_by_user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id"),
        nullable=False,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class AgentPreset(Base):
    __tablename__ = "agent_presets"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid7,
    )

    conversation_style: Mapped[ConversationStyle] = mapped_column(
        SAEnum(
            ConversationStyle,
            native_enum=False,
            validate_strings=True,
        ),
        nullable=False,
    )

    languages: Mapped[LanguagePreference] = mapped_column(
        SAEnum(
            LanguagePreference,
            native_enum=False,
            validate_strings=True,
        ),
        nullable=False,
    )

    voice: Mapped[VoicePreference] = mapped_column(
        SAEnum(
            VoicePreference,
            native_enum=False,
            validate_strings=True,
        ),
        nullable=False,
    )

    interview_instruction: Mapped[str] = mapped_column(
        String(2000),
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )