import enum
import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, String, Text, Enum, Integer, func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from uuid6 import uuid7

from app.core.database import Base


class MessageRole(str, enum.Enum):
    """The role of a message turn in the conversation."""
    USER = "user"
    MODEL = "model"
    TOOL = "tool"


class ChatSession(Base):
    """
    One conversation thread per user. A user may have many sessions (tabs /
    topics). Older turns are compressed into `summary` to keep the sliding
    window small and token-efficient.
    """
    __tablename__ = "chat_sessions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid7
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )

    # Auto-generated from the first user message.
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Gemini-generated rolling summary of turns that have been evicted from
    # the sliding window. Prepended to every new request as system context.
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)

    # How many raw turns are currently stored for this session.
    # Used to decide when to trigger summarisation without a COUNT(*) query.
    turn_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class ChatMessage(Base):
    """
    A single turn in a conversation. The `content` column stores the raw
    Gemini `Content` object (serialised as JSONB) so we can reconstruct
    the exact `contents[]` array on every request without custom mapping.

    Tool call parts and tool response parts are stored as separate rows with
    role='model' and role='tool' respectively, mirroring the Gemini wire format.
    """
    __tablename__ = "chat_messages"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid7
    )
    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("chat_sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    role: Mapped[MessageRole] = mapped_column(
        Enum(MessageRole, native_enum=False, validate_strings=True),
        nullable=False,
    )

    # Raw Gemini Content object: { "role": "...", "parts": [...] }
    content: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # When True, this turn has been folded into ChatSession.summary and is
    # excluded from the live sliding window sent to Gemini. The row is kept
    # permanently for audit and full history replay.
    is_summarised: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, index=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
